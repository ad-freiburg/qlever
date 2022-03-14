// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <iomanip>
#include <iostream>
#include <memory>
#include <utility>

#include "../util/Exception.h"
#include "../util/Log.h"
#include "../util/Timer.h"
#include "QueryExecutionContext.h"
#include "ResultTable.h"
#include "RuntimeInformation.h"

using std::endl;
using std::pair;
using std::shared_ptr;

// forward declaration needed to break dependencies
class QueryExecutionTree;

class Operation {
 public:
  // Default Constructor.
  Operation() : _executionContext(nullptr), _hasComputedSortColumns(false) {}

  // Typical Constructor.
  explicit Operation(QueryExecutionContext* executionContext)
      : _executionContext(executionContext), _hasComputedSortColumns(false) {}

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
  const vector<size_t>& getResultSortedOn() {
    if (!_hasComputedSortColumns) {
      _hasComputedSortColumns = true;
      _resultSortedColumns = resultSortedOn();
    }
    return _resultSortedColumns;
  }

  const Index& getIndex() const { return _executionContext->getIndex(); }

  const Engine& getEngine() const { return _executionContext->getEngine(); }

  // Get a unique, not ambiguous string representation for a subtree.
  // This should possible act like an ID for each subtree.
  virtual string asString(size_t indent = 0) const final {
    auto result = asStringImpl(indent);
    if (supportsLimit() && _limit.has_value()) {
      result +=
          " LIMIT (directly from operation) " + std::to_string(_limit.value());
    }
    return result;
  }

 protected:
  virtual string asStringImpl(size_t indent = 0) const = 0;

 public:
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  virtual string getDescriptor() const = 0;
  virtual size_t getResultWidth() const = 0;
  virtual void setTextLimit(size_t limit) = 0;
  virtual size_t getCostEstimate() = 0;
  virtual size_t getSizeEstimate() = 0;
  virtual float getMultiplicity(size_t col) = 0;
  virtual bool knownEmptyResult() = 0;
  virtual ad_utility::HashMap<string, size_t> getVariableColumns() const = 0;

  RuntimeInformation& getRuntimeInfo() { return _runtimeInfo; }

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
  // this operation. May only be called if `supportsLimit` returns true.
  void setLimit(uint64_t limit) {
    AD_CHECK(supportsLimit());
    _limit = limit;
  }

 protected:
  QueryExecutionContext* getExecutionContext() const {
    return _executionContext;
  }

  // The QueryExecutionContext for this particular element.
  // No ownership.
  QueryExecutionContext* _executionContext;

  /**
   * @brief Allows for updating of the sorted columns of an operation. This
   *        has to be used by an operation if it's sort columns change during
   *        the operations lifetime.
   */
  void setResultSortedOn(const vector<size_t>& sortedColumns) {
    _resultSortedColumns = sortedColumns;
  }

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
        _timeoutTimer->wlock()->checkTimeoutAndThrow("Timeout in "s +
                                                     getDescriptor());
        i = 0;
      }
    };
  }

 private:
  //! Compute the result of the query-subtree rooted at this element..
  //! Computes both, an EntityList and a HitList.
  virtual void computeResult(ResultTable* result) = 0;

  // Create and store the complete runtime Information for this operation.
  // All data that was previously stored in the runtime information will be
  // deleted.
  virtual void createRuntimeInformation(
      const ConcurrentLruCache ::ResultAndCacheStatus& resultAndCacheStatus,
      size_t timeInMilliseconds) final {
    // reset
    _runtimeInfo = RuntimeInformation();
    // the column names might differ between a cached result and this operation,
    // so we have to take the local ones.
    _runtimeInfo.setColumnNames(getVariableColumns());

    _runtimeInfo.setCols(getResultWidth());
    _runtimeInfo.setDescriptor(getDescriptor());

    // Only the result that was actually computed (or read from cache) knows
    // the correct information about the children computations.
    _runtimeInfo.children() =
        resultAndCacheStatus._resultPointer->_runtimeInfo.children();

    _runtimeInfo.setTime(timeInMilliseconds);
    _runtimeInfo.setRows(
        resultAndCacheStatus._resultPointer->_resultTable->size());
    _runtimeInfo.setWasCached(resultAndCacheStatus._wasCached);
    _runtimeInfo.addDetail(
        "original_total_time",
        resultAndCacheStatus._resultPointer->_runtimeInfo.getTime());
    _runtimeInfo.addDetail(
        "original_operation_time",
        resultAndCacheStatus._resultPointer->_runtimeInfo.getOperationTime());
  }

  // recursively call a function on all children
  template <typename F>
  void forAllDescendants(F f);

  // recursively call a function on all children
  template <typename F>
  void forAllDescendants(F f) const;

  vector<size_t> _resultSortedColumns;
  RuntimeInformation _runtimeInfo;

  bool _hasComputedSortColumns;

  /// collect all the warnings that were created during the creation or
  /// execution of this operation
  std::vector<std::string> _warnings;

  /// The
  std::optional<uint64_t> _limit;
};
