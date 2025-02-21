// Copyright 2015 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de>    [2015 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> [2018 - 2024]

#pragma once

#include <absl/container/inlined_vector.h>
#include <gtest/gtest_prod.h>

#include <memory>

#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"
#include "engine/RuntimeInformation.h"
#include "engine/VariableToColumnMap.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/data/LimitOffsetClause.h"
#include "parser/data/Variable.h"
#include "util/CancellationHandle.h"
#include "util/CompilerExtensions.h"
#include "util/CopyableSynchronization.h"

// forward declaration needed to break dependencies
class QueryExecutionTree;

enum class ComputationMode {
  FULLY_MATERIALIZED,
  ONLY_IF_CACHED,
  LAZY_IF_SUPPORTED
};

class Operation {
 private:
  using SharedCancellationHandle = ad_utility::SharedCancellationHandle;
  using Milliseconds = std::chrono::milliseconds;

  // Holds a precomputed Result of this operation if it is the sibling of a
  // Service operation.
  std::optional<std::shared_ptr<const Result>>
      precomputedResultBecauseSiblingOfService_;

  std::shared_ptr<RuntimeInformation> _runtimeInfo =
      std::make_shared<RuntimeInformation>();
  /// Pointer to the head of the `RuntimeInformation`.
  /// Used in `signalQueryUpdate()`, reset in `createRuntimeInfoFromEstimates()`
  std::shared_ptr<const RuntimeInformation> _rootRuntimeInfo = _runtimeInfo;
  RuntimeInformationWholeQuery _runtimeInfoWholeQuery;

  // Collect all the warnings that were created during the creation or
  // execution of this operation. This attribute is declared mutable in order to
  // allow const-functions in subclasses of Operation to add warnings.
  using ThreadsafeWarnings = ad_utility::Synchronized<std::vector<std::string>>;
  mutable ThreadsafeWarnings warnings_;

  // The limit from a SPARQL `LIMIT` clause.

  // Note: This limit will only be set in the following cases:
  // 1. This operation is the last operation of a subquery
  // 2. This operation is the last operation of a query AND it supports an
  //    efficient calculation of the limit (see also the `supportsLimit()`
  //    function).
  // We have chosen this design (in contrast to a dedicated subclass
  // of `Operation`) to favor such efficient implementations of a limit in the
  // future.
  LimitOffsetClause _limit;

  // Mutex that protects the `variableToColumnMap_` below.
  mutable ad_utility::CopyableMutex variableToColumnMapMutex_;
  // Store the mapping from variables to column indices. `nullopt` means that
  // this map has not yet been computed. This computation is typically performed
  // in the const member function `getInternallyVisibleVariableColumns`, so we
  // have to make it threadsafe.
  mutable std::optional<VariableToColumnMap> variableToColumnMap_;

  // Store the mapping from variables to column indices that is externally
  // visible. This might be different from the `variableToColumnMap_` in case of
  // a subquery that doesn't select all variables.
  mutable std::optional<VariableToColumnMap>
      externallyVisibleVariableToColumnMap_;

  // Mutex that protects the `_resultSortedColumns` below.
  mutable ad_utility::CopyableMutex _resultSortedColumnsMutex;

  // Store the list of columns by which the result is sorted.
  mutable std::optional<vector<ColumnIndex>> _resultSortedColumns =
      std::nullopt;

  // True if this operation does not support limits/offsets natively and a
  // limit/offset is applied post computation.
  bool externalLimitApplied_ = false;

  // See the documentation of the getter function below.
  bool canResultBeCached_ = true;

 public:
  // Holds a `PrefilterExpression` with its corresponding `Variable`.
  using PrefilterVariablePair = sparqlExpression::PrefilterExprVariablePair;

  // Default Constructor.
  Operation() : _executionContext(nullptr) {}

  // Typical Constructor.
  explicit Operation(QueryExecutionContext* executionContext)
      : _executionContext(executionContext) {}

  // Destructor.
  virtual ~Operation() {
    // Do NOT delete _executionContext, since
    // there is no ownership.
  }

  /// get non-owning pointers to all the held subtrees to actually use the
  /// Execution Trees as trees
  virtual std::vector<QueryExecutionTree*> getChildren() = 0;

  /// get non-owning constant pointers to all the held subtrees to actually use
  /// the Execution Trees as trees
  virtual std::vector<const QueryExecutionTree*> getChildren() const final {
    vector<QueryExecutionTree*> interm{
        const_cast<Operation*>(this)->getChildren()};
    return {interm.begin(), interm.end()};
  }

  // Get access to the children's RuntimeInfo. Overridden by the `Service`, as
  // it's children can't be accessed using `getChildren()` above.
  virtual absl::InlinedVector<std::shared_ptr<RuntimeInformation>, 2>
  getRuntimeInfoChildren();

  // recursively collect all Warnings generated by all descendants
  vector<string> collectWarnings() const;

  // Add a warning to the `Operation`. The warning will be returned by
  // `collectWarnings()` above.
  void addWarning(std::string warning) const {
    warnings_.wlock()->push_back(std::move(warning));
  }

  // If unbound variables that are used in a query are supposed to throw because
  // the corresponding `RuntimeParameter` is set, then throw. Else add a
  // warning.
  void addWarningOrThrow(std::string warning) const;

  /**
   * @return A list of columns on which the result of this operation is sorted.
   */
  const vector<ColumnIndex>& getResultSortedOn() const;

  const Index& getIndex() const { return _executionContext->getIndex(); }

  const auto& locatedTriplesSnapshot() const {
    return _executionContext->locatedTriplesSnapshot();
  }

  // Get an updated `QueryExecutionTree` that applies as many of the given
  // `PrefilterExpression`s over `IndexScan` as possible. Returns `nullopt`
  // if no `PrefilterExpression` is applicable and thus the `QueryExecutionTree`
  // is not changed.
  // Note: The default implementation always returns `nullopt` while this
  // function is currently only overridden for `IndexScan`. In the future also
  // other operations could pass on the `PrefilterExpressions` to the
  // `IndexScan` in their subtree.
  virtual std::optional<std::shared_ptr<QueryExecutionTree>>
  setPrefilterGetUpdatedQueryExecutionTree(
      [[maybe_unused]] const std::vector<PrefilterVariablePair>& prefilterPairs)
      const {
    return std::nullopt;
  };

  // Get a unique, not ambiguous string representation for a subtree.
  // This should act like an ID for each subtree.
  // Calls  `getCacheKeyImpl` and adds the information about the `LIMIT` clause.
  virtual std::string getCacheKey() const final;

  // If this function returns `false`, then the result of this `Operation` will
  // never be stored in the cache. It might however be read from the cache.
  // This can be used, if the operation actually only returns a subset of the
  // actual result because it has been constrained by a parent operation (e.g.
  // an IndexScan that has been prefiltered by another operation which it is
  // joined with).
  virtual bool canResultBeCached() const { return canResultBeCached_; }

  // After calling this function, `canResultBeCached()` will return `false` (see
  // above for details).
  virtual void disableStoringInCache() final { canResultBeCached_ = false; }

 private:
  // The individual implementation of `getCacheKey` (see above) that has to
  // be customized by every child class.
  virtual string getCacheKeyImpl() const = 0;

 public:
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  virtual string getDescriptor() const = 0;
  virtual size_t getResultWidth() const = 0;

  virtual size_t getCostEstimate() = 0;

  virtual uint64_t getSizeEstimate() final;

 private:
  virtual uint64_t getSizeEstimateBeforeLimit() = 0;

 public:
  virtual float getMultiplicity(size_t col) = 0;
  virtual bool knownEmptyResult() = 0;

  // Get the mapping from variables to columns but without the variables that
  // are not visible to the outside because they were not selected by a
  // subquery.
  virtual const VariableToColumnMap& getExternallyVisibleVariableColumns()
      const final;
  virtual void setSelectedVariablesForSubquery(
      const std::vector<Variable>& selectedVariables) final;

  /// Return true if this object is an instance of `IndexScan` and has the
  /// specified number of variables. For this to work this function needs to
  /// be overridden by `IndexScan` to do the right thing.
  virtual bool isIndexScanWithNumVariables(
      [[maybe_unused]] size_t target) const {
    return false;
  }

  // See the member variable with the same name below for documentation.
  std::optional<std::shared_ptr<const Result>>&
  precomputedResultBecauseSiblingOfService() {
    return precomputedResultBecauseSiblingOfService_;
  }

  RuntimeInformation& runtimeInfo() const { return *_runtimeInfo; }

  std::shared_ptr<RuntimeInformation> getRuntimeInfoPointer() {
    return _runtimeInfo;
  }

  RuntimeInformationWholeQuery& getRuntimeInfoWholeQuery() {
    return _runtimeInfoWholeQuery;
  }

  /// Notify the `QueryExecutionContext` of the latest `RuntimeInformation`.
  void signalQueryUpdate() const;

  /**
   * @brief Get the result for the subtree rooted at this element. Use existing
   * results if they are already available, otherwise trigger computation.
   * Always returns a non-null pointer, except for when `onlyReadFromCache` is
   * true (see below).
   * @param isRoot Has be set to `true` iff this is the root operation of a
   * complete query to obtain the expected behavior wrt cache pinning and
   * runtime information in error cases.
   * @param computationMode If set to `CACHE_ONLY` the result is only returned
   * if it can be read from the cache without any computation. If the result is
   * not in the cache, `nullptr` will be returned. If set to `LAZY` this will
   * request the result to be computable at request in chunks. If the operation
   * does not support this, it will do nothing.
   * @return A shared pointer to the result. May only be `nullptr` if
   * `onlyReadFromCache` is true.
   */
  std::shared_ptr<const Result> getResult(
      bool isRoot = false,
      ComputationMode computationMode = ComputationMode::FULLY_MATERIALIZED);

  // Use the same cancellation handle for all children of an operation (= query
  // plan rooted at that operation). As soon as one child is aborted, the whole
  // operation is aborted out.
  void recursivelySetCancellationHandle(
      SharedCancellationHandle cancellationHandle);

  template <typename Rep, typename Period>
  void recursivelySetTimeConstraint(
      std::chrono::duration<Rep, Period> duration) {
    recursivelySetTimeConstraint(std::chrono::steady_clock::now() + duration);
  }

  void recursivelySetTimeConstraint(
      std::chrono::steady_clock::time_point deadline);

  // Optimization for lazy operations where the very nature of the operation
  // makes it unlikely to ever fit in cache when completely materialized.
  virtual bool unlikelyToFitInCache(
      [[maybe_unused]] ad_utility::MemorySize maxCacheableSize) const {
    return false;
  }

  // True iff this operation directly implement a `OFFSET` and `LIMIT` clause on
  // its result.
  [[nodiscard]] virtual bool supportsLimit() const { return false; }

  // Set the value of the `LIMIT` clause that will be applied to the result of
  // this operation.
  void setLimit(const LimitOffsetClause& limitOffsetClause) {
    _limit = limitOffsetClause;
  }

  // Create and return the runtime information wrt the size and cost estimates
  // without actually executing the query.
  virtual void createRuntimeInfoFromEstimates(
      std::shared_ptr<const RuntimeInformation> root) final;

  QueryExecutionContext* getExecutionContext() const {
    return _executionContext;
  }

  const ad_utility::AllocatorWithLimit<Id>& allocator() const {
    return getExecutionContext()->getAllocator();
  }

  // If the result of this `Operation` is sorted (either because this
  // `Operation` enforces this sorting, or because it preserves the sorting of
  // its children), return the variable that is the primary sort key. Else
  // return nullopt.
  virtual std::optional<Variable> getPrimarySortKeyVariable() const final;

  // Direct access to the `computeResult()` method. This should be only used for
  // testing, otherwise the `getResult()` function should be used which also
  // sets the runtime info and uses the cache.
  virtual ProtoResult computeResultOnlyForTesting(
      bool requestLaziness = false) final {
    return computeResult(requestLaziness);
  }

  const auto& getLimit() const { return _limit; }

 private:
  // Actual implementation of `clone()` without extra checks.
  virtual std::unique_ptr<Operation> cloneImpl() const = 0;

 public:
  // Create a deep copy of this operation.
  std::unique_ptr<Operation> clone() const;

 protected:
  // The QueryExecutionContext for this particular element.
  // No ownership.
  QueryExecutionContext* _executionContext;

  /**
   * @brief Compute and return the columns on which the result will be sorted
   * @return The columns on which the result will be sorted.
   */
  [[nodiscard]] virtual vector<ColumnIndex> resultSortedOn() const = 0;

  // get access to the generated warnings of this operation.
  const ThreadsafeWarnings& getWarnings() const { return warnings_; }

  // Check if the cancellation flag has been set and throw an exception if
  // that's the case. This will be called at strategic places on code that
  // potentially can take a (too) long time. This function is designed to be
  // as lightweight as possible because of that.
  AD_ALWAYS_INLINE void checkCancellation(
      ad_utility::source_location location =
          ad_utility::source_location::current()) const {
    cancellationHandle_->throwIfCancelled(location,
                                          [this]() { return getDescriptor(); });
  }

  std::chrono::milliseconds remainingTime() const;

  /// Pointer to the cancellation handle of this operation.
  SharedCancellationHandle cancellationHandle_ =
      std::make_shared<SharedCancellationHandle::element_type>();

  std::chrono::steady_clock::time_point deadline_ =
      std::chrono::steady_clock::time_point::max();

  // Get the mapping from variables to column indices. This mapping may only be
  // used internally, because the actually visible variables might be different
  // in case of a subquery.
  virtual const VariableToColumnMap& getInternallyVisibleVariableColumns()
      const final;

 private:
  //! Compute the result of the query-subtree rooted at this element..
  virtual ProtoResult computeResult(bool requestLaziness) = 0;

  // Update the runtime information of this operation according to the given
  // arguments, considering the possibility that the initial runtime information
  // was replaced by calling `RuntimeInformation::addLimitOffsetRow`.
  // `applyToLimit` indicates if the stats should be applied to the runtime
  // information of the limit, or the runtime information of the actual
  // operation. If `supportsLimit() == true`, then the operation does already
  // track the limit stats correctly and there's no need to keep track of both.
  // Otherwise `externalLimitApplied_` decides how stat tracking should be
  // handled.
  void updateRuntimeStats(bool applyToLimit, uint64_t numRows, uint64_t numCols,
                          std::chrono::microseconds duration) const;

  // Perform the expensive computation modeled by the subclass of this
  // `Operation`. The value provided by `computationMode` decides if lazy
  // results are preferred. It must not be `ONLY_IF_CACHED`, this will lead to
  // an `ad_utility::Exception`.
  ProtoResult runComputation(const ad_utility::Timer& timer,
                             ComputationMode computationMode);

  // Call `runComputation` and transform it into a value that could be inserted
  // into the cache.
  CacheValue runComputationAndPrepareForCache(const ad_utility::Timer& timer,
                                              ComputationMode computationMode,
                                              const QueryCacheKey& cacheKey,
                                              bool pinned, bool isRoot);

  // Create and store the complete runtime information for this operation after
  // it has either been successfully computed or read from the cache.
  virtual void updateRuntimeInformationOnSuccess(
      const QueryResultCache::ResultAndCacheStatus& resultAndCacheStatus,
      Milliseconds duration) final;

  // Similar to the function above, but the components are specified manually.
  // If nullopt is specified for the last argument, then the `_runtimeInfo` is
  // expected to already have the correct children information. This is only
  // allowed when `cacheStatus` is `cachedPinned` or `cachedNotPinned`,
  // otherwise a runtime check will fail.
  virtual void updateRuntimeInformationOnSuccess(
      size_t numRows, ad_utility::CacheStatus cacheStatus,
      Milliseconds duration,
      std::optional<RuntimeInformation> runtimeInfo) final;

 public:
  // This function has to be called when this `Operation` was not executed,
  // because one of its ancestors used a special implementation that did not
  // evaluate the full query execution tree. Calling this function sets the
  // actual operation time to zero and the status to `optimizedOut`. The
  // runtime information of the children is set to the argument `children`.
  // This is useful in case this operation was optimized out, but some of the
  // children were evaluated nevertheless. For an example usage of this feature
  // see `GroupBy.cpp`
  virtual void updateRuntimeInformationWhenOptimizedOut(
      std::vector<std::shared_ptr<RuntimeInformation>> children,
      RuntimeInformation::Status status =
          RuntimeInformation::Status::optimizedOut);

  // Use the already stored runtime info for the children,
  // but set all of them to `optimizedOut`. This can be used, when a complete
  // tree was optimized out. For example when one child of a JOIN operation is
  // empty, the result will be empty, and it is not necessary to evaluate the
  // other child.
  virtual void updateRuntimeInformationWhenOptimizedOut(
      RuntimeInformation::Status status =
          RuntimeInformation::Status::optimizedOut);

 private:
  // Create the runtime information in case the evaluation of this operation has
  // failed.
  void updateRuntimeInformationOnFailure(Milliseconds duration);

  // Compute the variable to column index mapping. Is used internally by
  // `getInternallyVisibleVariableColumns`.
  virtual VariableToColumnMap computeVariableToColumnMap() const = 0;

  // Recursively call a function on all children.
  template <typename F>
  void forAllDescendants(F f);

  // Recursively call a function on all children.
  template <typename F>
  void forAllDescendants(F f) const;

  FRIEND_TEST(Operation, updateRuntimeStatsWorksCorrectly);
  FRIEND_TEST(Operation, verifyRuntimeInformationIsUpdatedForLazyOperations);
  FRIEND_TEST(Operation, ensureFailedStatusIsSetWhenGeneratorThrowsException);
  FRIEND_TEST(Operation, testSubMillisecondsIncrementsAreStillTracked);
  FRIEND_TEST(Operation, ensureSignalUpdateIsOnlyCalledEvery50msAndAtTheEnd);
  FRIEND_TEST(Operation,
              ensureSignalUpdateIsCalledAtTheEndOfPartialConsumption);
  FRIEND_TEST(Operation,
              verifyLimitIsProperlyAppliedAndUpdatesRuntimeInfoCorrectly);
  FRIEND_TEST(Operation, ensureLazyOperationIsCachedIfSmallEnough);
  FRIEND_TEST(Operation, checkLazyOperationIsNotCachedIfTooLarge);
  FRIEND_TEST(Operation, checkLazyOperationIsNotCachedIfUnlikelyToFitInCache);
  FRIEND_TEST(Operation, checkMaxCacheSizeIsComputedCorrectly);
};
