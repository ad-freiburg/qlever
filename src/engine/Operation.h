// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <iomanip>
#include <iostream>
#include <memory>
#include <utility>

#include "engine/QueryExecutionContext.h"
#include "engine/ResultTable.h"
#include "engine/RuntimeInformation.h"
#include "engine/VariableToColumnMap.h"
#include "parser/data/LimitOffsetClause.h"
#include "parser/data/Variable.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"

// forward declaration needed to break dependencies
class QueryExecutionTree;

class Operation {
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
  std::vector<const QueryExecutionTree*> getChildren() const {
    vector<QueryExecutionTree*> interm{
        const_cast<Operation*>(this)->getChildren()};
    return {interm.begin(), interm.end()};
  }

  // recursively collect all Warnings generated by all descendants
  vector<string> collectWarnings() const;

  /**
   * @return A list of columns on which the result of this operation is sorted.
   */
  const vector<size_t>& getResultSortedOn() const;

  const Index& getIndex() const { return _executionContext->getIndex(); }

  const Engine& getEngine() const { return _executionContext->getEngine(); }

  // Get a unique, not ambiguous string representation for a subtree.
  // This should act like an ID for each subtree.
  // Calls  `asStringImpl` and adds the information about the `LIMIT` clause.
  virtual string asString(size_t indent = 0) const final {
    auto result = asStringImpl(indent);
    if (_limit._limit.has_value()) {
      absl::StrAppend(&result, " LIMIT ", _limit._limit.value());
    }
    if (_limit._offset != 0) {
      absl::StrAppend(&result, " OFFSET ", _limit._offset);
    }
    return result;
  }

 private:
  // The individual implementation of `asString` (see above) that has to be
  // customized by every child class.
  virtual string asStringImpl(size_t indent = 0) const = 0;

 public:
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  virtual string getDescriptor() const = 0;
  virtual size_t getResultWidth() const = 0;
  virtual void setTextLimit(size_t limit) = 0;
  virtual size_t getCostEstimate() = 0;

  virtual size_t getSizeEstimate() final {
    if (_limit._limit.has_value()) {
      return std::min(_limit._limit.value(), getSizeEstimateBeforeLimit());
    } else {
      return getSizeEstimateBeforeLimit();
    }
  }

 private:
  virtual size_t getSizeEstimateBeforeLimit() = 0;

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

  RuntimeInformation& getRuntimeInfo() { return _runtimeInfo; }
  RuntimeInformationWholeQuery& getRuntimeInfoWholeQuery() {
    return _runtimeInfoWholeQuery;
  }

  // Get the result for the subtree rooted at this element.
  // Use existing results if they are already available, otherwise
  // trigger computation.
  shared_ptr<const ResultTable> getResult(bool isRoot = false);

  // Use the same timeout timer for all children of an operation (= query plan
  // rooted at that operation). As soon as one child times out, the whole
  // operation times out.
  void recursivelySetTimeoutTimer(
      const ad_utility::SharedConcurrentTimeoutTimer& timer);

  // True iff this operation directly implement a `LIMIT` clause on its result.
  [[nodiscard]] virtual bool supportsLimit() const { return false; }

  // Set the value of the `LIMIT` clause that will be applied to the result of
  // this operation.
  void setLimit(const LimitOffsetClause& limitOffsetClause) {
    _limit = limitOffsetClause;
  }

  // Create and return the runtime information wrt the size and cost estimates
  // without actually executing the query.
  virtual void createRuntimeInfoFromEstimates() final;

  QueryExecutionContext* getExecutionContext() const {
    return _executionContext;
  }

  // If the result of this `Operation` is sorted (either because this
  // `Operation` enforces this sorting, or because it preserves the sorting of
  // its children), return the variable that is the primary sort key. Else
  // return nullopt.
  virtual std::optional<Variable> getPrimarySortKeyVariable() const final;

  // `IndexScan`s with only one variable are often executed already during
  // query planning. The result is stored in the cache as well as in the
  // `IndexScan` object itself. This interface allows to ask an Operation
  // explicitly whether it stores such a precomputed result. In this case we can
  // call `computeResult` in O(1) to obtain the precomputed result. This has two
  // advantages over implicitly reading the result from the cache:
  // 1. The result might be not in the cache anymore, but the IndexScan still
  //    stores it.
  // 2. We can preserve the information, whether the computation was read from
  //    the cache or actually computed during the query planning or processing.
  virtual std::optional<std::shared_ptr<const ResultTable>>
  getPrecomputedResultFromQueryPlanning() {
    return std::nullopt;
  }

  // Direct access to the `computeResult()` method. This should be only used for
  // testing, otherwise the `getResult()` function should be used which also
  // sets the runtime info and uses the cache.
  virtual ResultTable computeResultOnlyForTesting() final {
    return computeResult();
  }

 protected:
  // The QueryExecutionContext for this particular element.
  // No ownership.
  QueryExecutionContext* _executionContext;

  /**
   * @brief Compute and return the columns on which the result will be sorted
   * @return The columns on which the result will be sorted.
   */
  [[nodiscard]] virtual vector<size_t> resultSortedOn() const = 0;

  const auto& getLimit() const { return _limit; }

  /// interface to the generated warnings of this operation
  std::vector<std::string>& getWarnings() { return _warnings; }
  [[nodiscard]] const std::vector<std::string>& getWarnings() const {
    return _warnings;
  }

  // Check if there is still time left and throw a TimeoutException otherwise.
  // This will be called at strategic places on code that potentially can take a
  // (too) long time.
  void checkTimeout() const;

  // Handles the timeout of this operation.
  ad_utility::SharedConcurrentTimeoutTimer _timeoutTimer =
      std::make_shared<ad_utility::ConcurrentTimeoutTimer>(
          ad_utility::TimeoutTimer::unlimited());

  // Returns a lambda with the following behavior: For every call, increase the
  // internal counter i by countIncrease. If the counter exceeds countMax, check
  // for timeout and reset the counter to zero. That way, the expensive timeout
  // check is called only rarely. Note that we sometimes need to "simulate"
  // several operations at a time, hence the countIncrease.
  auto checkTimeoutAfterNCallsFactory(
      size_t numOperationsBetweenTimeoutChecks =
          NUM_OPERATIONS_BETWEEN_TIMEOUT_CHECKS) const {
    return [numOperationsBetweenTimeoutChecks, i = 0ull,
            this](size_t countIncrease = 1) mutable {
      i += countIncrease;
      if (i >= numOperationsBetweenTimeoutChecks) {
        _timeoutTimer->wlock()->checkTimeoutAndThrow(
            [this]() { return "Timeout in " + getDescriptor(); });
        i = 0;
      }
    };
  }

  // Get the mapping from variables to column indices. This mapping may only be
  // used internally, because the actually visible variables might be different
  // in case of a subquery.
  virtual const VariableToColumnMap& getInternallyVisibleVariableColumns()
      const final;

 private:
  //! Compute the result of the query-subtree rooted at this element..
  virtual ResultTable computeResult() = 0;

  // Create and store the complete runtime information for this operation after
  // it has either been succesfully computed or read from the cache.
  virtual void updateRuntimeInformationOnSuccess(
      const ConcurrentLruCache::ResultAndCacheStatus& resultAndCacheStatus,
      size_t timeInMilliseconds) final;

  // Similar to the function above, but the components are specified manually.
  // If nullopt is specified for the last argument, then the `_runtimeInfo` is
  // expected to already have the correct children information. This is only
  // allowed when `cacheStatus` is `cachedPinned` or `cachedNotPinned`,
  // otherwise a runtime check will fail.
  virtual void updateRuntimeInformationOnSuccess(
      const ResultTable& resultTable, ad_utility::CacheStatus cacheStatus,
      size_t timeInMilliseconds,
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
      std::vector<RuntimeInformation> children);

  // Some operations (currently `IndexScans` with only one variable) are
  // computed during query planning. Get the total time spent in such
  // computations for this operation and all its descendants. This can be used
  // to correct the time statistics for the query planning and execution.
  size_t getTotalExecutionTimeDuringQueryPlanning() const;

 private:
  // Create the runtime information in case the evaluation of this operation has
  // failed.
  void updateRuntimeInformationOnFailure(size_t timeInMilliseconds);

  // Compute the variable to column index mapping. Is used internally by
  // `getInternallyVisibleVariableColumns`.
  virtual VariableToColumnMap computeVariableToColumnMap() const = 0;

  // Recursively call a function on all children.
  template <typename F>
  void forAllDescendants(F f);

  // Recursively call a function on all children.
  template <typename F>
  void forAllDescendants(F f) const;

  RuntimeInformation _runtimeInfo;
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
  mutable std::optional<vector<size_t>> _resultSortedColumns = std::nullopt;
};
