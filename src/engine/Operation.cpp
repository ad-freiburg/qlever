// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach  (johannes.kalmbach@gmail.com)

#include "./Operation.h"

#include "engine/QueryExecutionTree.h"
#include "util/TransparentFunctors.h"

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
  ad_utility::Timer timer{ad_utility::Timer::Started};

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
      updateRuntimeInformationOnSuccess(*val._resultTable,
                                        ad_utility::CacheStatus::computed,
                                        timer.msecs(), std::nullopt);
      val._runtimeInfo = getRuntimeInfo();
      return val;
    };

    // If the result was already computed during the query planning, then
    // we can simply return that result, but only if we don't have to pin
    // it to the cache.
    if (!pinResult) {
      auto precomputedResult = getPrecomputedResultFromQueryPlanning();
      if (precomputedResult.has_value()) {
        return std::move(precomputedResult.value());
      }
    }

    auto result = (pinResult) ? cache.computeOncePinned(cacheKey, computeLambda)
                              : cache.computeOnce(cacheKey, computeLambda);

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
    const ResultTable& resultTable, ad_utility::CacheStatus cacheStatus,
    size_t timeInMilliseconds, std::optional<RuntimeInformation> runtimeInfo) {
  _runtimeInfo.totalTime_ = timeInMilliseconds;
  _runtimeInfo.numRows_ = resultTable.size();
  _runtimeInfo.cacheStatus_ = cacheStatus;

  _runtimeInfo.status_ = RuntimeInformation::Status::completed;

  bool wasCached = cacheStatus != ad_utility::CacheStatus::computed;
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
      resultAndCacheStatus._cacheStatus, timeInMilliseconds,
      resultAndCacheStatus._resultPointer->_runtimeInfo);
}

// _____________________________________________________________________________
void Operation::updateRuntimeInformationWhenOptimizedOut(
    std::vector<RuntimeInformation> children) {
  _runtimeInfo.status_ = RuntimeInformation::Status::optimizedOut;
  _runtimeInfo.children_ = std::move(children);
  // This operation was optimized out, so its operation time is zero.
  // The operation time is computed as
  // `totalTime_ - #sum of childrens' total time#` in `getOperationTime()`.
  // To set it to zero we thus have to set the `totalTime_` to that sum.
  _runtimeInfo.totalTime_ = 0;
  std::ranges::for_each(
      _runtimeInfo.children_, [this](const RuntimeInformation& child) {
        if (child.status_ !=
            RuntimeInformation::Status::completedDuringQueryPlanning) {
          _runtimeInfo.totalTime_ += child.totalTime_;
        }
      });
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
  // Handle the case that the result was already computed during the query
  // planning. In this case, `getResult()` was already called on this object and
  // the runtime information is already correct.
  if (getPrecomputedResultFromQueryPlanning().has_value()) {
    return;
  }
  // TODO<joka921> If the above stuff works, this can be removed.
  std::optional<RuntimeInformation::Status> statusFromPrecomputedResult =
      getPrecomputedResultFromQueryPlanning().has_value()
          ? std::optional{_runtimeInfo.status_}
          : std::nullopt;
  _runtimeInfo.setColumnNames(getInternallyVisibleVariableColumns());
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
      _executionContext->getQueryTreeCache().getIfContained(asString());
  if (cachedResult.has_value()) {
    const auto& [resultPointer, cacheStatus] = cachedResult.value();
    _runtimeInfo.cacheStatus_ = cacheStatus;
    const auto& rtiFromCache = resultPointer->_runtimeInfo;

    _runtimeInfo.numRows_ = rtiFromCache.numRows_;
    _runtimeInfo.originalTotalTime_ = rtiFromCache.totalTime_;
    _runtimeInfo.originalOperationTime_ = rtiFromCache.getOperationTime();
  }
  if (statusFromPrecomputedResult ==
      RuntimeInformation::Status::completedDuringQueryPlanning) {
    _runtimeInfo.status_ =
        RuntimeInformation::Status::completedDuringQueryPlanning;
    _runtimeInfo.cacheStatus_ = ad_utility::CacheStatus::computed;
  }
}

// ___________________________________________________________________________
const VariableToColumnMap& Operation::getInternallyVisibleVariableColumns()
    const {
  // TODO<joka921> Once the operation class is based on a variant rather than
  // on inheritance, we can get rid of the locking here because we can enforce
  // that `computeVariableToColumnMap` is always called in the constructor of
  // each `Operation`.
  std::lock_guard l{variableToColumnMapMutex_};
  if (!variableToColumnMap_.has_value()) {
    variableToColumnMap_ = computeVariableToColumnMap();
  }
  return variableToColumnMap_.value();
}

// ___________________________________________________________________________
const VariableToColumnMap& Operation::getExternallyVisibleVariableColumns()
    const {
  // TODO<joka921> Once the operation class is based on a variant rather than
  // on inheritance, we can get rid of the locking here because we can enforce
  // that `computeVariableToColumnMap` is always called in the constructor of
  // each `Operation`.
  std::lock_guard l{variableToColumnMapMutex_};
  if (!externallyVisibleVariableToColumnMap_.has_value()) {
    externallyVisibleVariableToColumnMap_ = computeVariableToColumnMap();
  }
  return externallyVisibleVariableToColumnMap_.value();
}

// ___________________________________________________________________________
void Operation::setSelectedVariablesForSubquery(
    const std::vector<Variable>& selectedVariables) {
  const auto& internalVariables = getInternallyVisibleVariableColumns();
  externallyVisibleVariableToColumnMap_.emplace();
  auto& externalVariables = externallyVisibleVariableToColumnMap_.value();
  for (const Variable& variable : selectedVariables) {
    if (internalVariables.contains(variable)) {
      externalVariables[variable] = internalVariables.at(variable);
    }
  }
}

// ___________________________________________________________________________
std::optional<Variable> Operation::getPrimarySortKeyVariable() const {
  const auto& varToColMap = getExternallyVisibleVariableColumns();
  const auto& sortedIndices = getResultSortedOn();

  if (sortedIndices.empty()) {
    return std::nullopt;
  }

  // TODO<joka921> Can be simplified using views once they are properly
  // supported inside clang.
  auto it =
      std::ranges::find(varToColMap, sortedIndices.front(), ad_utility::second);
  if (it == varToColMap.end()) {
    return std::nullopt;
  }
  return Variable{it->first};
}

// ___________________________________________________________________________
const vector<size_t>& Operation::getResultSortedOn() const {
  // TODO<joka921> refactor this without a mutex (for details see the
  // `getVariableColumns` method for details.
  std::lock_guard l{_resultSortedColumnsMutex};
  if (!_resultSortedColumns.has_value()) {
    _resultSortedColumns = resultSortedOn();
  }
  return _resultSortedColumns.value();
}

// ___________________________________________________________________________
size_t Operation::getTotalExecutionTimeDuringQueryPlanning() const {
  size_t totalTime = 0;
  forAllDescendants([&totalTime](const QueryExecutionTree* tree) {
    const auto& rti = tree->getRootOperation()->_runtimeInfo;
    if (rti.status_ ==
        RuntimeInformation::Status::completedDuringQueryPlanning) {
      totalTime += rti.getOperationTime();
    }
  });
  return totalTime;
}
