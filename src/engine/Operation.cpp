// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach  (johannes.kalmbach@gmail.com)

#include <engine/Operation.h>
#include <engine/QueryExecutionTree.h>

template <typename F>
void Operation::forAllDescendants(F f) {
  static_assert(
      std::is_same_v<void, std::invoke_result_t<F, QueryExecutionTree*>>);
  for (auto ptr : getChildren()) {
    if (ptr) {
      f(ptr);
      ptr->forAllDescendants(f);
    }
  }
}

template <typename F>
void Operation::forAllDescendants(F f) const {
  static_assert(
      std::is_same_v<void, std::invoke_result_t<F, const QueryExecutionTree*>>);
  for (auto ptr : getChildren()) {
    if (ptr) {
      f(ptr);
      ptr->forAllDescendants(f);
    }
  }
}

// __________________________________________________________________________________________________________
vector<string> Operation::collectWarnings() const {
  vector<string> res = getWarnings();
  for (auto child : getChildren()) {
    if (!child) {
      continue;
    }
    auto recursive = child->collectWarnings();
    res.insert(res.end(), std::make_move_iterator(recursive.begin()),
               std::make_move_iterator(recursive.end()));
  }

  return res;
}

// ________________________________________________________________________
void Operation::recursivelySetTimeoutTimer(
    const ad_utility::SharedConcurrentTimeoutTimer& timer) {
  _timeoutTimer = timer;
  for (auto child : getChildren()) {
    if (child) {
      child->recursivelySetTimeoutTimer(timer);
    }
  }
}

// Get the result for the subtree rooted at this element. Use existing results
// if they are already available, otherwise trigger computation.
shared_ptr<const ResultTable> Operation::getResult(bool isRoot) {
  ad_utility::Timer timer;
  timer.start();

  if (isRoot) {
    // Start with an estimated runtime info which will be updated as we go.
    createRuntimeInfoFromEstimates();
  }
  auto& cache = _executionContext->getQueryTreeCache();
  const string cacheKey = asString();
  const bool pinFinalResultButNotSubtrees =
      _executionContext->_pinResult && isRoot;
  const bool pinResult =
      _executionContext->_pinSubtrees || pinFinalResultButNotSubtrees;

  // When we pin the final result but no subtrees, we need to remember the sizes
  // of all involved index scans that have only one free variable. Note that
  // these index scans are executed already during query planning because they
  // have to be executed anyway, for any query plan. If we don't remember these
  // sizes here, future queries that take the result from the cache would redo
  // these index scans. Note that we do not need to remember the multiplicity
  // (and distinctness) because the multiplicity for an index scan with a single
  // free variable is always 1.
  if (pinFinalResultButNotSubtrees) {
    auto lock =
        getExecutionContext()->getQueryTreeCache().pinnedSizes().wlock();
    forAllDescendants([&lock](QueryExecutionTree* child) {
      if (child->getType() == QueryExecutionTree::OperationType::SCAN &&
          child->getResultWidth() == 1) {
        (*lock)[child->getRootOperation()->asString()] =
            child->getSizeEstimate();
      }
    });
  }

  try {
    // In case of an exception, create the correct runtime info, no matter which
    // exception handler is called.
    ad_utility::OnDestruction onDestruction{[&]() {
      if (std::uncaught_exceptions()) {
        timer.stop();
        updateRuntimeInformationOnFailure(true, timer.msecs());
      }
    }};
    auto computeLambda = [this, &timer] {
      CacheValue val(getExecutionContext()->getAllocator());
      if (_timeoutTimer->wlock()->hasTimedOut()) {
        throw ad_utility::TimeoutException(
            "Timeout in operation with no or insufficient timeout "
            "functionality, before " +
            getDescriptor());
      }
      _runtimeInfo.children().clear();
      computeResult(val._resultTable.get());
      if (_timeoutTimer->wlock()->hasTimedOut()) {
        throw ad_utility::TimeoutException(
            "Timeout in " + getDescriptor() +
            ". This timeout was not caught inside the actual computation, "
            "which indicates insufficient timeout functionality.");
      }
      _runtimeInfo.setRows(val._resultTable->_idTable.size());
      // Make sure that the results that are written to the cache have the
      // correct runtimeInfo. The children of the runtime info are already set
      // correctly because the result was computed, so we can pass `nullopt` as
      // the last argument.
      timer.stop();
      updateRuntimeInformationOnSuccess(*val._resultTable, false, timer.msecs(),
                                        std::nullopt);
      timer.cont();
      val._runtimeInfo = getRuntimeInfo();
      return val;
    };

    auto result = (pinResult) ? cache.computeOncePinned(cacheKey, computeLambda)
                              : cache.computeOnce(cacheKey, computeLambda);

    timer.stop();
    updateRuntimeInformationOnSuccess(result, timer.msecs());
    auto resultNumRows = result._resultPointer->_resultTable->size();
    auto resultNumCols = result._resultPointer->_resultTable->width();
    LOG(DEBUG) << "Computed result of size " << resultNumRows << " x "
               << resultNumCols << std::endl;
    return result._resultPointer->_resultTable;
  } catch (const ad_semsearch::AbortException& e) {
    // A child Operation was aborted, do not print the information again.
    _runtimeInfo.addDetail("status", "failed because child failed");
    throw;
  } catch (const ad_utility::WaitedForResultWhichThenFailedException& e) {
    // Here and in the following, show the detailed information (it's the
    // runtime info) only in the DEBUG log. Note that the exception will be
    // caught by the `processQuery` method, where the error message will be
    // printed *and* included in an error response sent to the client.
    LOG(ERROR) << "Waited for a result from another thread which then failed"
               << endl;
    LOG(DEBUG) << asString();
    throw ad_semsearch::AbortException(e);
  } catch (const std::exception& e) {
    // We are in the innermost level of the exception, so print
    LOG(ERROR) << "Aborted Operation" << endl;
    LOG(DEBUG) << asString() << endl;
    // Rethrow as QUERY_ABORTED allowing us to print the Operation
    // only at innermost failure of a recursive call
    throw ad_semsearch::AbortException(e);
  } catch (...) {
    // We are in the innermost level of the exception, so print
    LOG(ERROR) << "Aborted Operation" << endl;
    LOG(DEBUG) << asString() << endl;
    // Rethrow as QUERY_ABORTED allowing us to print the Operation
    // only at innermost failure of a recursive call
    throw ad_semsearch::AbortException(
        "Unexpected expection that is not a subclass of std::exception");
  }
}

// ______________________________________________________________________
void Operation::checkTimeout() const {
  if (_timeoutTimer->wlock()->hasTimedOut()) {
    throw ad_utility::TimeoutException("Timeout in " + getDescriptor());
  }
}

// _______________________________________________________________________
void Operation::updateRuntimeInformationOnSuccess(
    const ResultTable& resultTable, bool wasCached, size_t timeInMilliseconds,
    std::optional<RuntimeInformation> runtimeInfo) {
  // The column names might differ between a cached result and this operation,
  // so we have to take the local ones.
  _runtimeInfo.setColumnNames(getVariableColumns());

  _runtimeInfo.setCols(getResultWidth());
  _runtimeInfo.setDescriptor(getDescriptor());

  _runtimeInfo.setTime(timeInMilliseconds);
  _runtimeInfo.setRows(resultTable.size());
  _runtimeInfo.setWasCached(wasCached);

  _runtimeInfo.addDetail("status", "completed");

  // If the result was read from the cache, then we need the additional
  // runtime info for the correct child information etc.
  AD_CHECK(!wasCached || runtimeInfo.has_value());

  if (runtimeInfo.has_value()) {
    _runtimeInfo.addDetail("original_total_time", runtimeInfo->getTime());
    _runtimeInfo.addDetail("original_operation_time",
                           runtimeInfo->getOperationTime());
    // Only the result that was actually computed (or read from cache) knows
    // the correct information about the children computations.
    _runtimeInfo.children() = std::move(runtimeInfo->children());
  }
}

// ____________________________________________________________________________________________________________________
void Operation::updateRuntimeInformationOnSuccess(
    const ConcurrentLruCache ::ResultAndCacheStatus& resultAndCacheStatus,
    size_t timeInMilliseconds) {
  updateRuntimeInformationOnSuccess(
      *resultAndCacheStatus._resultPointer->_resultTable,
      resultAndCacheStatus._wasCached, timeInMilliseconds,
      resultAndCacheStatus._resultPointer->_runtimeInfo);
}

// _______________________________________________________________________
void Operation::updateRuntimeInformationOnFailure(
    bool failureCausedByThisOperation, size_t timeInMilliseconds) {
  // The column names might differ between a cached result and this operation,
  // so we have to take the local ones.
  _runtimeInfo.setColumnNames(getVariableColumns());

  _runtimeInfo.setCols(getResultWidth());
  _runtimeInfo.setDescriptor(getDescriptor());

  _runtimeInfo.children().clear();
  for (auto child : getChildren()) {
    _runtimeInfo.children().push_back(child->getRootOperation()->_runtimeInfo);
  }

  _runtimeInfo.setTime(timeInMilliseconds);
  _runtimeInfo.setRows(0);
  _runtimeInfo.setWasCached(false);
  if (failureCausedByThisOperation) {
    _runtimeInfo.addDetail("status", "failed");
  } else {
    _runtimeInfo.addDetail("status", "failed because child failed");
  }
}

// __________________________________________________________________
void Operation::createRuntimeInfoFromEstimates() {
  // reset
  _runtimeInfo = RuntimeInformation{};
  _runtimeInfo.setColumnNames(getVariableColumns());
  const auto numCols = getResultWidth();
  _runtimeInfo.setCols(numCols);
  _runtimeInfo.setDescriptor(getDescriptor());

  for (const auto& child : getChildren()) {
    AD_CHECK(child);
    child->getRootOperation()->createRuntimeInfoFromEstimates();
    _runtimeInfo.children().push_back(
        child->getRootOperation()->getRuntimeInfo());
  }

  _runtimeInfo.setCostEstimate(getCostEstimate());
  _runtimeInfo.setSizeEstimate(getSizeEstimate());
  _runtimeInfo.setTime(0);
  _runtimeInfo.setRows(0);

  std::vector<float> multiplicityEstimates;
  multiplicityEstimates.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    multiplicityEstimates.push_back(getMultiplicity(i));
  }
  _runtimeInfo.addDetail("multiplicity-estimates", multiplicityEstimates);

  auto cachedResult =
      _executionContext->getQueryTreeCache().resultAt(asString());
  if (cachedResult) {
    _runtimeInfo.setWasCached(true);
    _runtimeInfo.setRows(cachedResult->_resultTable->size());
    _runtimeInfo.addDetail("original_total_time",
                           cachedResult->_runtimeInfo.getTime());
    _runtimeInfo.addDetail("original_operation_time",
                           cachedResult->_runtimeInfo.getOperationTime());
  } else {
    _runtimeInfo.setWasCached(false);
  }
  _runtimeInfo.addDetail("status", "not started");
}
