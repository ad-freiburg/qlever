// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach  (johannes.kalmbach@gmail.com)

#include "engine/Operation.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/inlined_vector.h>

#include "engine/QueryExecutionTree.h"
#include "global/RuntimeParameters.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/TransparentFunctors.h"

using namespace std::chrono_literals;

namespace {
// Keep track of some statistics about the local vocabs of the results.
struct LocalVocabTracking {
  size_t maxSize_ = 0;
  size_t sizeSum_ = 0;
  size_t totalVocabs_ = 0;
  size_t nonEmptyVocabs_ = 0;

  float avgSize() const {
    return nonEmptyVocabs_ == 0 ? 0
                                : static_cast<float>(sizeSum_) /
                                      static_cast<float>(nonEmptyVocabs_);
  }
};

// Merge the stats of a single local vocab into the overall stats.
void mergeStats(LocalVocabTracking& stats, const LocalVocab& vocab) {
  stats.maxSize_ = std::max(stats.maxSize_, vocab.size());
  stats.sizeSum_ += vocab.size();
  ++stats.totalVocabs_;
  if (!vocab.empty()) {
    ++stats.nonEmptyVocabs_;
  }
}
}  // namespace

//______________________________________________________________________________
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

//______________________________________________________________________________
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

// _____________________________________________________________________________
vector<std::string> Operation::collectWarnings() const {
  vector<std::string> res{*getWarnings().rlock()};
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

// _____________________________________________________________________________
void Operation::addWarningOrThrow(std::string warning) const {
  if (RuntimeParameters().get<"throw-on-unbound-variables">()) {
    throw InvalidSparqlQueryException(std::move(warning));
  } else {
    addWarning(std::move(warning));
  }
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
Result Operation::runComputation(const ad_utility::Timer& timer,
                                 ComputationMode computationMode) {
  AD_CONTRACT_CHECK(computationMode != ComputationMode::ONLY_IF_CACHED);
  checkCancellation();
  runtimeInfo().status_ = RuntimeInformation::Status::inProgress;
  signalQueryUpdate();
  Result result =
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
    size_t vocabSize = result.localVocab().size();
    if (vocabSize > 1) {
      runtimeInfo().addDetail("local-vocab-size", vocabSize);
    }
    AD_CORRECTNESS_CHECK(result.idTable().numColumns() == getResultWidth());
    updateRuntimeInformationOnSuccess(result.idTable().size(),
                                      ad_utility::CacheStatus::computed,
                                      timer.msecs(), std::nullopt);
    AD_CORRECTNESS_CHECK(
        result.idTable().empty() || !knownEmptyResult(), [&]() {
          return absl::StrCat("Operation ", getDescriptor(),
                              "returned non-empty result, but "
                              "knownEmptyResult() returned true");
        });
  } else {
    auto& rti = runtimeInfo();
    rti.status_ = RuntimeInformation::lazilyMaterialized;
    rti.totalTime_ = timer.msecs();
    rti.originalTotalTime_ = rti.totalTime_;
    rti.originalOperationTime_ = rti.getOperationTime();
    result.runOnNewChunkComputed(
        [this, timeSizeUpdate = 0us, vocabStats = LocalVocabTracking{},
         ker = knownEmptyResult()](const Result::IdTableVocabPair& pair,
                                   std::chrono::microseconds duration) mutable {
          const IdTable& idTable = pair.idTable_;
          AD_CORRECTNESS_CHECK(idTable.empty() || !ker,
                               "Operation returned non-empty result, but "
                               "knownEmptyResult() returned true");
          updateRuntimeStats(false, idTable.numRows(), idTable.numColumns(),
                             duration);
          AD_CORRECTNESS_CHECK(idTable.numColumns() == getResultWidth());
          LOG(DEBUG) << "Computed partial chunk of size " << idTable.numRows()
                     << " x " << idTable.numColumns() << std::endl;
          mergeStats(vocabStats, pair.localVocab_);
          if (vocabStats.sizeSum_ > 0) {
            runtimeInfo().addDetail(
                "non-empty-local-vocabs",
                absl::StrCat(vocabStats.nonEmptyVocabs_, " / ",
                             vocabStats.totalVocabs_,
                             ", Ø = ", vocabStats.avgSize(),
                             ", max = ", vocabStats.maxSize_));
          }
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
  if (!supportsLimitOffset()) {
    runtimeInfo().addLimitOffsetRow(limitOffset_, true);
    AD_CONTRACT_CHECK(!externalLimitApplied_);
    externalLimitApplied_ = !limitOffset_.isUnconstrained();
    result.applyLimitOffset(
        limitOffset_,
        [this](std::chrono::microseconds limitTime, const IdTable& idTable) {
          updateRuntimeStats(true, idTable.numRows(), idTable.numColumns(),
                             limitTime);
        });
  } else {
    result.assertThatLimitWasRespected(limitOffset_);
  }
  return result;
}

// _____________________________________________________________________________
CacheValue Operation::runComputationAndPrepareForCache(
    const ad_utility::Timer& timer, ComputationMode computationMode,
    const QueryCacheKey& cacheKey, bool pinned, bool isRoot) {
  auto& cache = _executionContext->getQueryTreeCache();
  auto result = runComputation(timer, computationMode);
  auto maxSize =
      isRoot ? cache.getMaxSizeSingleEntry()
             : std::min(RuntimeParameters().get<"cache-max-size-lazy-result">(),
                        cache.getMaxSizeSingleEntry());
  if (canResultBeCached() && !result.isFullyMaterialized() &&
      !unlikelyToFitInCache(maxSize)) {
    AD_CONTRACT_CHECK(!pinned);
    result.cacheDuringConsumption(
        [maxSize](
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
  const QueryCacheKey cacheKey = {
      getCacheKey(), _executionContext->locatedTriplesSnapshot().index_};
  const bool pinFinalResultButNotSubtrees =
      _executionContext->_pinResult && isRoot;
  const bool pinResult =
      _executionContext->_pinSubtrees || pinFinalResultButNotSubtrees;

  // If pinned there's no point in computing the result lazily.
  if (pinResult && computationMode == ComputationMode::LAZY_IF_SUPPORTED) {
    computationMode = ComputationMode::FULLY_MATERIALIZED;
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
    auto cacheSetup = [this, &timer, computationMode, &cacheKey, pinResult,
                       isRoot]() {
      return runComputationAndPrepareForCache(timer, computationMode, cacheKey,
                                              pinResult, isRoot);
    };

    auto suitedForCache = [](const CacheValue& cacheValue) {
      return cacheValue.resultTable().isFullyMaterialized();
    };

    bool onlyReadFromCache = computationMode == ComputationMode::ONLY_IF_CACHED;

    auto result = [&]() {
      auto compute = [&](auto&&... args) {
        if (!canResultBeCached()) {
          return cache.computeButDontStore(AD_FWD(args)...);
        }
        return pinResult ? cache.computeOncePinned(AD_FWD(args)...)
                         : cache.computeOnce(AD_FWD(args)...);
      };
      return compute(cacheKey, cacheSetup, onlyReadFromCache, suitedForCache);
    }();

    if (result._resultPointer == nullptr) {
      AD_CORRECTNESS_CHECK(onlyReadFromCache);
      return nullptr;
    }

    if (result._resultPointer->resultTable().isFullyMaterialized()) {
      AD_CORRECTNESS_CHECK(
          result._resultPointer->resultTable().idTable().numColumns() ==
              getResultWidth(),
          result._cacheStatus == ad_utility::CacheStatus::computed
              ? "This should never happen, non-matching result widths should "
                "have been caught earlier"
              : "Retrieved result from cache with a different number of "
                "columns than expected. There's something wrong with the cache "
                "key.");
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
                         ql::views::transform(&RuntimeInformation::totalTime_);
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
void Operation::applyLimitOffset(const LimitOffsetClause& limitOffsetClause) {
  limitOffset_.mergeLimitAndOffset(limitOffsetClause);
  // We can safely ignore members that are not `_offset` and `_limit` since
  // they are unused by subclasses of `Operation`.
  onLimitOffsetChanged(limitOffsetClause);
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
  // We are interested only in the first two elements of the limit tuple.
  const auto& [limit, offset, _1, _2] = getLimitOffset();
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

  auto cachedResult = _executionContext->getQueryTreeCache().getIfContained(
      {getCacheKey(), locatedTriplesSnapshot().index_});
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

  auto it = ql::ranges::find(
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
  if (_executionContext && _executionContext->areWebsocketUpdatesEnabled()) {
    _executionContext->signalQueryUpdate(*_rootRuntimeInfo);
  }
}

// _____________________________________________________________________________
std::string Operation::getCacheKey() const {
  auto result = getCacheKeyImpl();
  if (limitOffset_._limit.has_value()) {
    absl::StrAppend(&result, " LIMIT ", limitOffset_._limit.value());
  }
  if (limitOffset_._offset != 0) {
    absl::StrAppend(&result, " OFFSET ", limitOffset_._offset);
  }
  return result;
}

// _____________________________________________________________________________
uint64_t Operation::getSizeEstimate() {
  if (limitOffset_._limit.has_value()) {
    return std::min(limitOffset_._limit.value(), getSizeEstimateBeforeLimit());
  } else {
    return getSizeEstimateBeforeLimit();
  }
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Operation::clone() const {
  auto result = cloneImpl();

  if (variableToColumnMap_ && externallyVisibleVariableToColumnMap_) {
    // Make sure previously hidden variables remain hidden.
    std::vector<Variable> visibleVariables;
    ql::ranges::copy(getExternallyVisibleVariableColumns() | ql::views::keys,
                     std::back_inserter(visibleVariables));
    result->setSelectedVariablesForSubquery(visibleVariables);
  }
  result->limitOffset_ = limitOffset_;

  auto compareTypes = [this, &result]() {
    const auto& reference = *result;
    return typeid(*this) == typeid(reference);
  };
  AD_CORRECTNESS_CHECK(compareTypes());
  AD_CORRECTNESS_CHECK(result->_executionContext == _executionContext);
  auto areChildrenDifferent = [this, &result]() {
    auto ownChildren = getChildren();
    auto otherChildren = result->getChildren();
    if (ownChildren.size() != otherChildren.size()) {
      return false;
    }
    for (size_t i = 0; i < ownChildren.size(); i++) {
      if (ownChildren.at(i) == otherChildren.at(i)) {
        return false;
      }
    }
    return true;
  };
  AD_CORRECTNESS_CHECK(areChildrenDifferent());
  AD_CORRECTNESS_CHECK(variableToColumnMap_ == result->variableToColumnMap_);
  // If the result can be cached, then the cache key must be the same for
  // the cloned operation.
  AD_EXPENSIVE_CHECK(!canResultBeCached() ||
                     getCacheKey() == result->getCacheKey());
  return result;
}

// _____________________________________________________________________________
bool Operation::isSortedBy(const vector<ColumnIndex>& sortColumns) const {
  auto inputSortedOn = resultSortedOn();
  if (sortColumns.size() > inputSortedOn.size()) {
    return false;
  }
  for (size_t i = 0; i < sortColumns.size(); ++i) {
    if (sortColumns[i] != inputSortedOn[i]) {
      return false;
    }
  }
  return true;
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>> Operation::makeSortedTree(
    const vector<ColumnIndex>& sortColumns) const {
  AD_CONTRACT_CHECK(!isSortedBy(sortColumns));
  return std::nullopt;
}

// _____________________________________________________________________________
bool Operation::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // Returning false does never lead to a wrong result, but it might be
  // inefficient.
  if (ql::ranges::none_of(getChildren(), [&variable](const auto* child) {
        return child->getVariableColumnOrNullopt(variable).has_value();
      })) {
    return false;
  }
  return ql::ranges::all_of(getChildren(), [&variable](const auto* child) {
    return !child->getVariableColumnOrNullopt(variable).has_value() ||
           child->getRootOperation()->columnOriginatesFromGraphOrUndef(
               variable);
  });
}
