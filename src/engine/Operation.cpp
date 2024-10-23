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

// _____________________________________________________________________________
void Operation::updateRuntimeStats(bool applyToLimit, uint64_t numRows,
                                   uint64_t numCols,
                                   std::chrono::microseconds duration) const {
  bool isRtiWrappedInLimit = !applyToLimit && externalLimitApplied_;
  auto& rti =
      isRtiWrappedInLimit ? *runtimeInfo().children_.at(0) : runtimeInfo();
  rti.totalTime_ += duration;
  rti.originalTotalTime_ = rti.totalTime_;
  rti.originalOperationTime_ = rti.getOperationTime();
  // Don't update the number of rows/cols twice if the rti for the limit and the
  // rti for the actual operation are the same.
  if (!applyToLimit || externalLimitApplied_) {
    rti.numRows_ += numRows;
    rti.numCols_ = numCols;
  }
  if (isRtiWrappedInLimit) {
    runtimeInfo().totalTime_ += duration;
    runtimeInfo().originalTotalTime_ = runtimeInfo().totalTime_;
    runtimeInfo().originalOperationTime_ = runtimeInfo().getOperationTime();
  }
}

// _____________________________________________________________________________
ProtoResult Operation::runComputation(const ad_utility::Timer& timer,
                                      ComputationMode computationMode) {
  AD_CONTRACT_CHECK(computationMode != ComputationMode::ONLY_IF_CACHED);
  checkCancellation();
  runtimeInfo().status_ = RuntimeInformation::Status::inProgress;
  signalQueryUpdate();
  ProtoResult result =
      computeResult(computationMode == ComputationMode::LAZY_IF_SUPPORTED);
  AD_CONTRACT_CHECK(computationMode == ComputationMode::LAZY_IF_SUPPORTED ||
                    result.isFullyMaterialized());

  checkCancellation();
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    // Compute the datatypes that occur in each column of the result.
    // Also assert, that if a column contains UNDEF values, then the
    // `mightContainUndef` flag for that columns is set.
    // TODO<joka921> It is cheaper to move this calculation into the
    // individual results, but that requires changes in each individual
    // operation, therefore we currently only perform this expensive
    // change in the DEBUG builds.
    result.checkDefinedness(getExternallyVisibleVariableColumns());
  }
  // Make sure that the results that are written to the cache have the
  // correct runtimeInfo. The children of the runtime info are already set
  // correctly because the result was computed, so we can pass `nullopt` as
  // the last argument.
  if (result.isFullyMaterialized()) {
    updateRuntimeInformationOnSuccess(result.idTable().size(),
                                      ad_utility::CacheStatus::computed,
                                      timer.msecs(), std::nullopt);
  } else {
    runtimeInfo().status_ = RuntimeInformation::lazilyMaterialized;
    result.runOnNewChunkComputed(
        [this, timeSizeUpdate = 0us](
            const IdTable& idTable,
            std::chrono::microseconds duration) mutable {
          updateRuntimeStats(false, idTable.numRows(), idTable.numColumns(),
                             duration);
          LOG(DEBUG) << "Computed partial chunk of size " << idTable.numRows()
                     << " x " << idTable.numColumns() << std::endl;
          timeSizeUpdate += duration;
          if (timeSizeUpdate > 50ms) {
            timeSizeUpdate = 0us;
            signalQueryUpdate();
          }
        },
        [this](bool failed) {
          if (failed) {
            runtimeInfo().status_ = RuntimeInformation::failed;
          }
          signalQueryUpdate();
        });
  }
  // Apply LIMIT and OFFSET, but only if the call to `computeResult` did not
  // already perform it. An example for an operation that directly computes
  // the Limit is a full index scan with three variables. Note that the
  // `QueryPlanner` does currently only set the limit for operations that
  // support it natively, except for operations in subqueries. This means
  // that a lot of the time the limit is only artificially applied during
  // export, allowing the cache to reuse the same operation for different
  // limits and offsets.
  if (!supportsLimit()) {
    runtimeInfo().addLimitOffsetRow(_limit, true);
    AD_CONTRACT_CHECK(!externalLimitApplied_);
    externalLimitApplied_ = !_limit.isUnconstrained();
    result.applyLimitOffset(_limit, [this](std::chrono::microseconds limitTime,
                                           const IdTable& idTable) {
      updateRuntimeStats(true, idTable.numRows(), idTable.numColumns(),
                         limitTime);
    });
  } else {
    result.assertThatLimitWasRespected(_limit);
  }
  return result;
}

// _____________________________________________________________________________
CacheValue Operation::runComputationAndPrepareForCache(
    const ad_utility::Timer& timer, ComputationMode computationMode,
    const std::string& cacheKey, bool pinned) {
  auto& cache = _executionContext->getQueryTreeCache();
  auto result = runComputation(timer, computationMode);
  if (!result.isFullyMaterialized() &&
      !unlikelyToFitInCache(cache.getMaxSizeSingleEntry())) {
    AD_CONTRACT_CHECK(!pinned);
    result.cacheDuringConsumption(
        [maxSize = cache.getMaxSizeSingleEntry()](
            const std::optional<Result::IdTableVocabPair>& currentIdTablePair,
            const Result::IdTableVocabPair& newIdTable) {
          auto currentSize =
              currentIdTablePair.has_value()
                  ? CacheValue::getSize(currentIdTablePair.value().idTable_)
                  : 0_B;
          return maxSize >=
                 currentSize + CacheValue::getSize(newIdTable.idTable_);
        },
        [runtimeInfo = getRuntimeInfoPointer(), &cache,
         cacheKey](Result aggregatedResult) {
          auto copy = *runtimeInfo;
          copy.status_ = RuntimeInformation::Status::fullyMaterialized;
          cache.tryInsertIfNotPresent(
              false, cacheKey,
              std::make_shared<CacheValue>(std::move(aggregatedResult),
                                           std::move(copy)));
        });
  }
  if (result.isFullyMaterialized()) {
    auto resultNumRows = result.idTable().size();
    auto resultNumCols = result.idTable().numColumns();
    LOG(DEBUG) << "Computed result of size " << resultNumRows << " x "
               << resultNumCols << std::endl;
  }

  return CacheValue{std::move(result), runtimeInfo()};
}

// ________________________________________________________________________
std::shared_ptr<const Result> Operation::getResult(
    bool isRoot, ComputationMode computationMode) {
  // Use the precomputed Result if it exists.
  if (precomputedResultBecauseSiblingOfService_.has_value()) {
    auto result = std::move(precomputedResultBecauseSiblingOfService_).value();
    precomputedResultBecauseSiblingOfService_.reset();
    return result;
  }

  ad_utility::Timer timer{ad_utility::Timer::Started};

  if (isRoot) {
    // Reset runtime info, tests may reuse Operation objects.
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
    auto cacheSetup = [this, &timer, computationMode, &cacheKey, pinResult]() {
      return runComputationAndPrepareForCache(timer, computationMode, cacheKey,
                                              pinResult);
    };

    auto suitedForCache = [](const CacheValue& cacheValue) {
      return cacheValue.resultTable().isFullyMaterialized();
    };

    bool onlyReadFromCache = computationMode == ComputationMode::ONLY_IF_CACHED;

    auto result =
        pinResult ? cache.computeOncePinned(cacheKey, cacheSetup,
                                            onlyReadFromCache, suitedForCache)
                  : cache.computeOnce(cacheKey, cacheSetup, onlyReadFromCache,
                                      suitedForCache);

    if (result._resultPointer == nullptr) {
      AD_CORRECTNESS_CHECK(onlyReadFromCache);
      return nullptr;
    }

    if (result._resultPointer->resultTable().isFullyMaterialized()) {
      updateRuntimeInformationOnSuccess(result, timer.msecs());
    }

    return result._resultPointer->resultTablePtr();
  } catch (ad_utility::CancellationException& e) {
    e.setOperation(getDescriptor());
    runtimeInfo().status_ = RuntimeInformation::Status::cancelled;
    throw;
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
        "Unexpected exception that is not a subclass of std::exception");
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
    size_t numRows, ad_utility::CacheStatus cacheStatus, Milliseconds duration,
    std::optional<RuntimeInformation> runtimeInfo) {
  _runtimeInfo->totalTime_ = duration;
  _runtimeInfo->numRows_ = numRows;
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
    const QueryResultCache::ResultAndCacheStatus& resultAndCacheStatus,
    Milliseconds duration) {
  const auto& result = resultAndCacheStatus._resultPointer->resultTable();
  AD_CONTRACT_CHECK(result.isFullyMaterialized());
  updateRuntimeInformationOnSuccess(
      result.idTable().size(), resultAndCacheStatus._cacheStatus, duration,
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
      std::reduce(timesOfChildren.begin(), timesOfChildren.end(), 0us);

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
  const auto& [limit, offset, _] = getLimit();
  if (limit.has_value()) {
    _runtimeInfo->addDetail("limit", limit.value());
  }
  if (offset > 0) {
    _runtimeInfo->addDetail("offset", offset);
  }

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

// _____________________________________________________________________________

void Operation::signalQueryUpdate() const {
  if (_executionContext) {
    _executionContext->signalQueryUpdate(*_rootRuntimeInfo);
  }
}
