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
        updateRuntimeInformationOnFailure(timer.msecs());
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
      computeResult(val._resultTable.get());
      if (_timeoutTimer->wlock()->hasTimedOut()) {
        throw ad_utility::TimeoutException(
            "Timeout in " + getDescriptor() +
            ". This timeout was not caught inside the actual computation, "
            "which indicates insufficient timeout functionality.");
      }
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
    _runtimeInfo.status_ = RuntimeInformation::Status::failedBecauseChildFailed;
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
  _runtimeInfo.totalTime_ = timeInMilliseconds;
  _runtimeInfo.numRows_ = resultTable.size();
  _runtimeInfo.wasCached_ = wasCached;

  _runtimeInfo.status_ = RuntimeInformation::Status::completed;

  // If the result was read from the cache, then we need the additional
  // runtime info for the correct child information etc.
  AD_CHECK(!wasCached || runtimeInfo.has_value());

  if (runtimeInfo.has_value()) {
    if (wasCached) {
      _runtimeInfo.originalTotalTime_ = runtimeInfo->totalTime_;
      _runtimeInfo.originalOperationTime_ = runtimeInfo->getOperationTime();
      _runtimeInfo.details_ = std::move(runtimeInfo->details_);
    }
    // Only the result that was actually computed (or read from cache) knows
    // the correct information about the children computations.
    _runtimeInfo.children_ = std::move(runtimeInfo->children_);
  } else {
    // The result was computed by this operation (not read from the cache).
    // Therefore, for each child of this operation the correct runtime is
    // available.
    _runtimeInfo.children_.clear();
    for (auto* child : getChildren()) {
      AD_CHECK(child);
      _runtimeInfo.children_.push_back(
          child->getRootOperation()->getRuntimeInfo());
    }
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
void Operation::updateRuntimeInformationOnFailure(size_t timeInMilliseconds) {
  _runtimeInfo.children_.clear();
  for (auto child : getChildren()) {
    _runtimeInfo.children_.push_back(child->getRootOperation()->_runtimeInfo);
  }

  _runtimeInfo.totalTime_ = timeInMilliseconds;
  _runtimeInfo.status_ = RuntimeInformation::Status::failed;
}

// __________________________________________________________________
void Operation::createRuntimeInfoFromEstimates() {
  // reset
  _runtimeInfo = RuntimeInformation{};
  _runtimeInfo.setColumnNames(getVariableColumns());
  const auto numCols = getResultWidth();
  _runtimeInfo.numCols_ = numCols;
  _runtimeInfo.descriptor_ = getDescriptor();

  for (const auto& child : getChildren()) {
    AD_CHECK(child);
    child->getRootOperation()->createRuntimeInfoFromEstimates();
    _runtimeInfo.children_.push_back(
        child->getRootOperation()->getRuntimeInfo());
  }

  _runtimeInfo.costEstimate_ = getCostEstimate();
  _runtimeInfo.sizeEstimate_ = getSizeEstimate();

  std::vector<float> multiplicityEstimates;
  multiplicityEstimates.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    multiplicityEstimates.push_back(getMultiplicity(i));
  }
  _runtimeInfo.multiplicityEstimates_ = multiplicityEstimates;

  auto cachedResult =
      _executionContext->getQueryTreeCache().resultAt(asString());
  if (cachedResult) {
    _runtimeInfo.wasCached_ = true;

    _runtimeInfo.numRows_ = cachedResult->_resultTable->size();
    _runtimeInfo.originalTotalTime_ = cachedResult->_runtimeInfo.totalTime_;
    _runtimeInfo.originalOperationTime_ =
        cachedResult->_runtimeInfo.getOperationTime();
  }
}

// ___________________________________________________________________________
const Operation::VariableToColumnMap& Operation::getVariableColumns() const {
  // TODO<joka921> Once the operation class is based on a variant rather than
  // on inheritance, we can get rid of the locking here, as we can enforce,
  // that `computeVariableToColumnMap` is always called in the constructor of
  // each `Operation`.
  std::lock_guard l{variableToColumnMapMutex_};
  if (!variableToColumnMap_.has_value()) {
    variableToColumnMap_ = computeVariableToColumnMap();
  }
  return variableToColumnMap_.value();
}

// ___________________________________________________________________________
Operation::VariableToColumnMap& Operation::getVariableColumnsNotConst() {
  // This is a safe const-cast because the actual access is to the non-const
  // `*this` object.
  return const_cast<VariableToColumnMap&>(getVariableColumns());
}
