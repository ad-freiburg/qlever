// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <iomanip>
#include <memory>

#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"
#include "engine/RuntimeInformation.h"
#include "engine/VariableToColumnMap.h"
#include "parser/data/LimitOffsetClause.h"
#include "parser/data/Variable.h"
#include "util/CancellationHandle.h"
#include "util/CompilerExtensions.h"

// forward declaration needed to break dependencies
class QueryExecutionTree;

class Operation {
  using SharedCancellationHandle = ad_utility::SharedCancellationHandle;
  using Milliseconds = std::chrono::milliseconds;

 public:
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

  // recursively collect all Warnings generated by all descendants
  vector<string> collectWarnings() const;

  /**
   * @return A list of columns on which the result of this operation is sorted.
   */
  const vector<ColumnIndex>& getResultSortedOn() const;

  const Index& getIndex() const { return _executionContext->getIndex(); }

  // Get a unique, not ambiguous string representation for a subtree.
  // This should act like an ID for each subtree.
  // Calls  `getCacheKeyImpl` and adds the information about the `LIMIT` clause.
  virtual string getCacheKey() const final {
    auto result = getCacheKeyImpl();
    if (_limit._limit.has_value()) {
      absl::StrAppend(&result, " LIMIT ", _limit._limit.value());
    }
    if (_limit._offset != 0) {
      absl::StrAppend(&result, " OFFSET ", _limit._offset);
    }
    return result;
  }

 private:
  // The individual implementation of `getCacheKey` (see above) that has to be
  // customized by every child class.
  virtual string getCacheKeyImpl() const = 0;

 public:
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  virtual string getDescriptor() const = 0;
  virtual size_t getResultWidth() const = 0;

  virtual size_t getCostEstimate() = 0;

  virtual uint64_t getSizeEstimate() final {
    if (_limit._limit.has_value()) {
      return std::min(_limit._limit.value(), getSizeEstimateBeforeLimit());
    } else {
      return getSizeEstimateBeforeLimit();
    }
  }

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
   * @param onlyReadFromCache If set to true the result is only returned if it
   * can be read from the cache without any computation. If the result is not in
   * the cache, `nullptr` will be returned.
   * @return A shared pointer to the result. May only be `nullptr` if
   * `onlyReadFromCache` is true.
   */
  shared_ptr<const Result> getResult(bool isRoot = false,
                                     bool onlyReadFromCache = false);

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

  // True iff this operation directly implement a `LIMIT` clause on its result.
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
  virtual Result computeResultOnlyForTesting() final { return computeResult(); }

 protected:
  // The QueryExecutionContext for this particular element.
  // No ownership.
  QueryExecutionContext* _executionContext;

  /**
   * @brief Compute and return the columns on which the result will be sorted
   * @return The columns on which the result will be sorted.
   */
  [[nodiscard]] virtual vector<ColumnIndex> resultSortedOn() const = 0;

  const auto& getLimit() const { return _limit; }

  /// interface to the generated warnings of this operation
  std::vector<std::string>& getWarnings() { return _warnings; }
  [[nodiscard]] const std::vector<std::string>& getWarnings() const {
    return _warnings;
  }

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
  virtual Result computeResult() = 0;

  // Create and store the complete runtime information for this operation after
  // it has either been succesfully computed or read from the cache.
  virtual void updateRuntimeInformationOnSuccess(
      const ConcurrentLruCache::ResultAndCacheStatus& resultAndCacheStatus,
      Milliseconds duration) final;

  // Similar to the function above, but the components are specified manually.
  // If nullopt is specified for the last argument, then the `_runtimeInfo` is
  // expected to already have the correct children information. This is only
  // allowed when `cacheStatus` is `cachedPinned` or `cachedNotPinned`,
  // otherwise a runtime check will fail.
  virtual void updateRuntimeInformationOnSuccess(
      const Result& resultTable, ad_utility::CacheStatus cacheStatus,
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

  std::shared_ptr<RuntimeInformation> _runtimeInfo =
      std::make_shared<RuntimeInformation>();
  /// Pointer to the head of the `RuntimeInformation`.
  /// Used in `signalQueryUpdate()`, reset in `createRuntimeInfoFromEstimates()`
  std::shared_ptr<const RuntimeInformation> _rootRuntimeInfo = _runtimeInfo;
  RuntimeInformationWholeQuery _runtimeInfoWholeQuery;

  // Collect all the warnings that were created during the creation or
  // execution of this operation.
  std::vector<std::string> _warnings;

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

  // A mutex that can be "copied". The semantics are, that copying will create
  // a new mutex. This is sufficient for applications like in
  // `getInternallyVisibleVariableColumns()` where we just want to make a
  // `const` member function that modifies a `mutable` member threadsafe.
  struct CopyableMutex : std::mutex {
    using std::mutex::mutex;
    CopyableMutex(const CopyableMutex&) {}
  };

  // Mutex that protects the `variableToColumnMap_` below.
  mutable CopyableMutex variableToColumnMapMutex_;
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
  mutable CopyableMutex _resultSortedColumnsMutex;

  // Store the list of columns by which the result is sorted.
  mutable std::optional<vector<ColumnIndex>> _resultSortedColumns =
      std::nullopt;
};
