// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach  (johannes.kalmbach@gmail.com)

#include "engine/Operation.h"

#include "engine/QueryExecutionTree.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/TransparentFunctors.h"

using namespace std::chrono_literals;

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
void Operation::recursivelySetCancellationHandle(
    SharedCancellationHandle cancellationHandle) {
  AD_CORRECTNESS_CHECK(cancellationHandle);
  forAllDescendants([&cancellationHandle](auto child) {
    child->getRootOperation()->cancellationHandle_ = cancellationHandle;
  });
  cancellationHandle_ = std::move(cancellationHandle);
}

// ________________________________________________________________________

void Operation::recursivelySetTimeConstraint(
    std::chrono::steady_clock::time_point deadline) {
  deadline_ = deadline;
  forAllDescendants([deadline](auto child) {
    child->getRootOperation()->deadline_ = deadline;
  });
}

// ________________________________________________________________________
shared_ptr<const ResultTable> Operation::getResult(bool isRoot,
                                                   bool onlyReadFromCache) {
  ad_utility::Timer timer{ad_utility::Timer::Started};

  if (isRoot) {
    // Reset runtime info, tests may re-use Operation objects.
    _runtimeInfo = std::make_shared<RuntimeInformation>();
    // Start with an estimated runtime info which will be updated as we go.
    createRuntimeInfoFromEstimates(getRuntimeInfoPointer());
    signalQueryUpdate();
  }
  auto& cache = _executionContext->getQueryTreeCache();
  const string cacheKey = getCacheKey();
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
        (*lock)[child->getRootOperation()->getCacheKey()] =
            child->getSizeEstimate();
      }
    });
  }

  try {
    // In case of an exception, create the correct runtime info, no matter which
    // exception handler is called.
    // We cannot simply use `absl::Cleanup` because
    // `updateRuntimeInformationOnFailure` might (at least theoretically) throw
    // an exception.
    auto onDestruction =
        ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
            [this, &timer]() {
              if (std::uncaught_exceptions()) {
                updateRuntimeInformationOnFailure(timer.msecs());
              }
            });
    auto computeLambda = [this, &timer] {
      checkCancellation([this]() { return "Before " + getDescriptor(); });
      runtimeInfo().status_ = RuntimeInformation::Status::inProgress;
      signalQueryUpdate();
      ResultTable result = computeResult();

      checkCancellation([this]() { return "After " + getDescriptor(); });
      // Compute the datatypes that occur in each column of the result.
      // Also assert, that if a column contains UNDEF values, then the
      // `mightContainUndef` flag for that columns is set.
      // TODO<joka921> It is cheaper to move this calculation into the
      // individual results, but that requires changes in each individual
      // operation, therefore we currently only perform this expensive
      // change in the DEBUG builds.
      AD_EXPENSIVE_CHECK(
          result.checkDefinedness(getExternallyVisibleVariableColumns()));
      // Make sure that the results that are written to the cache have the
      // correct runtimeInfo. The children of the runtime info are already set
      // correctly because the result was computed, so we can pass `nullopt` as
      // the last argument.
      updateRuntimeInformationOnSuccess(result,
                                        ad_utility::CacheStatus::computed,
                                        timer.msecs(), std::nullopt);
      // Apply LIMIT and OFFSET, but only if the call to `computeResult` did not
      // already perform it. An example for an operation that directly computes
      // the Limit is a full index scan with three variables.
      if (!supportsLimit()) {
        ad_utility::timer::Timer limitTimer{ad_utility::timer::Timer::Started};
        // Note: both of the following calls have no effect and negligible
        // runtime if neither a LIMIT nor an OFFSET were specified.
        result.applyLimitOffset(_limit);
        runtimeInfo().addLimitOffsetRow(_limit, limitTimer.msecs(), true);
      } else {
        AD_CONTRACT_CHECK(result.idTable().numRows() ==
                          _limit.actualSize(result.idTable().numRows()));
      }
      return CacheValue{std::move(result), runtimeInfo()};
    };

    auto result = (pinResult) ? cache.computeOncePinned(cacheKey, computeLambda,
                                                        onlyReadFromCache)
                              : cache.computeOnce(cacheKey, computeLambda,
                                                  onlyReadFromCache);

    if (result._resultPointer == nullptr) {
      AD_CORRECTNESS_CHECK(onlyReadFromCache);
      return nullptr;
    }

    updateRuntimeInformationOnSuccess(result, timer.msecs());
    auto resultNumRows = result._resultPointer->resultTable()->size();
    auto resultNumCols = result._resultPointer->resultTable()->width();
    LOG(DEBUG) << "Computed result of size " << resultNumRows << " x "
               << resultNumCols << std::endl;
    return result._resultPointer->resultTable();
  } catch (const ad_utility::AbortException& e) {
    // A child Operation was aborted, do not print the information again.
    runtimeInfo().status_ =
        RuntimeInformation::Status::failedBecauseChildFailed;
    throw;
  } catch (const ad_utility::WaitedForResultWhichThenFailedException& e) {
    // Here and in the following, show the detailed information (it's the
    // runtime info) only in the DEBUG log. Note that the exception will be
    // caught by the `processQuery` method, where the error message will be
    // printed *and* included in an error response sent to the client.
    LOG(ERROR) << "Waited for a result from another thread which then failed"
               << std::endl;
    LOG(DEBUG) << getCacheKey();
    throw ad_utility::AbortException(e);
  } catch (const std::exception& e) {
    // We are in the innermost level of the exception, so print
    LOG(ERROR) << "Aborted Operation" << std::endl;
    LOG(DEBUG) << getCacheKey() << std::endl;
    // Rethrow as QUERY_ABORTED allowing us to print the Operation
    // only at innermost failure of a recursive call
    throw ad_utility::AbortException(e);
  } catch (...) {
    // We are in the innermost level of the exception, so print
    LOG(ERROR) << "Aborted Operation" << std::endl;
    LOG(DEBUG) << getCacheKey() << std::endl;
    // Rethrow as QUERY_ABORTED allowing us to print the Operation
    // only at innermost failure of a recursive call
    throw ad_utility::AbortException(
        "Unexpected expection that is not a subclass of std::exception");
  }
}

// ______________________________________________________________________

std::chrono::milliseconds Operation::remainingTime() const {
  auto interval = deadline_ - std::chrono::steady_clock::now();
  return std::max(
      0ms, std::chrono::duration_cast<std::chrono::milliseconds>(interval));
}

// _______________________________________________________________________
void Operation::updateRuntimeInformationOnSuccess(
    const ResultTable& resultTable, ad_utility::CacheStatus cacheStatus,
    Milliseconds duration, std::optional<RuntimeInformation> runtimeInfo) {
  _runtimeInfo->totalTime_ = duration;
  _runtimeInfo->numRows_ = resultTable.size();
  _runtimeInfo->cacheStatus_ = cacheStatus;

  _runtimeInfo->status_ = RuntimeInformation::Status::fullyMaterialized;

  bool wasCached = cacheStatus != ad_utility::CacheStatus::computed;
  // If the result was read from the cache, then we need the additional
  // runtime info for the correct child information etc.
  AD_CONTRACT_CHECK(!wasCached || runtimeInfo.has_value());

  if (runtimeInfo.has_value()) {
    if (wasCached) {
      _runtimeInfo->originalTotalTime_ = runtimeInfo->totalTime_;
      _runtimeInfo->originalOperationTime_ = runtimeInfo->getOperationTime();
      _runtimeInfo->details_ = std::move(runtimeInfo->details_);
    }
    // Only the result that was actually computed (or read from cache) knows
    // the correct information about the children computations.
    _runtimeInfo->children_ = std::move(runtimeInfo->children_);
  } else {
    // The result was computed by this operation (not read from the cache).
    // Therefore, for each child of this operation the correct runtime is
    // available.
    _runtimeInfo->children_.clear();
    for (auto* child : getChildren()) {
      AD_CONTRACT_CHECK(child);
      _runtimeInfo->children_.push_back(
          child->getRootOperation()->getRuntimeInfoPointer());
    }
  }
  signalQueryUpdate();
}

// ____________________________________________________________________________________________________________________
void Operation::updateRuntimeInformationOnSuccess(
    const ConcurrentLruCache ::ResultAndCacheStatus& resultAndCacheStatus,
    Milliseconds duration) {
  updateRuntimeInformationOnSuccess(
      *resultAndCacheStatus._resultPointer->resultTable(),
      resultAndCacheStatus._cacheStatus, duration,
      resultAndCacheStatus._resultPointer->runtimeInfo());
}

// _____________________________________________________________________________
void Operation::updateRuntimeInformationWhenOptimizedOut(
    std::vector<std::shared_ptr<RuntimeInformation>> children,
    RuntimeInformation::Status status) {
  _runtimeInfo->status_ = status;
  _runtimeInfo->children_ = std::move(children);
  // This operation was optimized out, so its operation time is zero.
  // The operation time is computed as
  // `totalTime_ - #sum of childrens' total time#` in `getOperationTime()`.
  // To set it to zero we thus have to set the `totalTime_` to that sum.
  auto timesOfChildren = _runtimeInfo->children_ |
                         std::views::transform(&RuntimeInformation::totalTime_);
  _runtimeInfo->totalTime_ =
      std::reduce(timesOfChildren.begin(), timesOfChildren.end(), 0ms);

  signalQueryUpdate();
}

// _____________________________________________________________________________
void Operation::updateRuntimeInformationWhenOptimizedOut(
    RuntimeInformation::Status status) {
  auto setStatus = [&status](RuntimeInformation& rti,
                             const auto& self) -> void {
    rti.status_ = status;
    rti.totalTime_ = 0ms;
    for (auto& child : rti.children_) {
      self(*child, self);
    }
  };
  setStatus(*_runtimeInfo, setStatus);

  signalQueryUpdate();
}

// _______________________________________________________________________
void Operation::updateRuntimeInformationOnFailure(Milliseconds duration) {
  _runtimeInfo->children_.clear();
  for (auto child : getChildren()) {
    _runtimeInfo->children_.push_back(child->getRootOperation()->_runtimeInfo);
  }

  _runtimeInfo->totalTime_ = duration;
  _runtimeInfo->status_ = RuntimeInformation::Status::failed;

  signalQueryUpdate();
}

// __________________________________________________________________
void Operation::createRuntimeInfoFromEstimates(
    std::shared_ptr<const RuntimeInformation> root) {
  _rootRuntimeInfo = root;
  _runtimeInfo->setColumnNames(getInternallyVisibleVariableColumns());
  const auto numCols = getResultWidth();
  _runtimeInfo->numCols_ = numCols;
  _runtimeInfo->descriptor_ = getDescriptor();

  for (const auto& child : getChildren()) {
    AD_CONTRACT_CHECK(child);
    child->getRootOperation()->createRuntimeInfoFromEstimates(root);
    _runtimeInfo->children_.push_back(
        child->getRootOperation()->getRuntimeInfoPointer());
  }

  _runtimeInfo->costEstimate_ = getCostEstimate();
  _runtimeInfo->sizeEstimate_ = getSizeEstimateBeforeLimit();

  std::vector<float> multiplicityEstimates;
  multiplicityEstimates.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    multiplicityEstimates.push_back(getMultiplicity(i));
  }
  _runtimeInfo->multiplicityEstimates_ = multiplicityEstimates;

  auto cachedResult =
      _executionContext->getQueryTreeCache().getIfContained(getCacheKey());
  if (cachedResult.has_value()) {
    const auto& [resultPointer, cacheStatus] = cachedResult.value();
    _runtimeInfo->cacheStatus_ = cacheStatus;
    const auto& rtiFromCache = resultPointer->runtimeInfo();

    _runtimeInfo->numRows_ = rtiFromCache.numRows_;
    _runtimeInfo->originalTotalTime_ = rtiFromCache.totalTime_;
    _runtimeInfo->originalOperationTime_ = rtiFromCache.getOperationTime();
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

  auto it = std::ranges::find(
      varToColMap, sortedIndices.front(),
      [](const auto& keyValue) { return keyValue.second.columnIndex_; });
  if (it == varToColMap.end()) {
    return std::nullopt;
  }
  return it->first;
}

// ___________________________________________________________________________
const vector<ColumnIndex>& Operation::getResultSortedOn() const {
  // TODO<joka921> refactor this without a mutex (for details see the
  // `getVariableColumns` method for details.
  std::lock_guard l{_resultSortedColumnsMutex};
  if (!_resultSortedColumns.has_value()) {
    _resultSortedColumns = resultSortedOn();
  }
  return _resultSortedColumns.value();
}

// ___________________________________________________________________________
void Operation::setTextLimit(size_t limit) {
  std::ranges::for_each(getChildren(), [limit](auto* child) {
    child->getRootOperation()->setTextLimit(limit);
  });
}

// _____________________________________________________________________________

void Operation::signalQueryUpdate() const {
  if (_executionContext) {
    _executionContext->signalQueryUpdate(*_rootRuntimeInfo);
  }
}
