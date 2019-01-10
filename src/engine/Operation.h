// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <memory>
#include <utility>

#include "../util/Exception.h"
#include "../util/Log.h"
#include "./QueryExecutionContext.h"
#include "./ResultTable.h"

using std::endl;
using std::pair;
using std::shared_ptr;

class Operation {
 public:
  // Default Constructor.
  Operation() : _executionContext(NULL), _hasComputedSortColumns(false) {}

  // Typical Constructor.
  explicit Operation(QueryExecutionContext* executionContext)
      : _executionContext(executionContext), _hasComputedSortColumns(false) {}

  // Destructor.
  virtual ~Operation() {
    // Do NOT delete _executionContext, since
    // there is no ownership.
  }

  // Get the result for the subtree rooted at this element.
  // Use existing results if they are already available, otherwise
  // trigger computation.
  shared_ptr<const ResultTable> getResult() const {
    LOG(DEBUG) << "Check cache for Operation result" << endl;
    LOG(DEBUG) << "Using key: \n" << asString() << endl;
    auto [newResult, existingResult] =
        _executionContext->getQueryTreeCache().tryEmplace(asString());
    if (newResult) {
      LOG(DEBUG) << "Not in the cache, need to compute result" << endl;
      // Passing the raw pointer here is ok as the result shared_ptr remains
      // in scope
      try {
        computeResult(newResult.get());
      } catch (const ad_semsearch::Exception& e) {
        if (e.getErrorCode() != ad_semsearch::Exception::QUERY_ABORTED) {
          // Only print the Operation at the innermost failure
          LOG(ERROR) << "Failed to compute Operation result for:" << endl;
          LOG(ERROR) << asString() << endl;
          LOG(ERROR) << e.getFullErrorMessage() << endl;
        }
        newResult->abort();
        // Rethrow as QUERY_ABORTED allowing us to print the Operation
        // only at innermost failure of a recursive call
        throw ad_semsearch::Exception(ad_semsearch::Exception::QUERY_ABORTED,
                                      e.getErrorDetails());
      }
      return newResult;
    }
    existingResult->awaitFinished();
    if (existingResult->status() == ResultTable::ABORTED) {
      LOG(ERROR) << "Result in the cache was aborted" << endl;
      AD_THROW(ad_semsearch::Exception::QUERY_ABORTED,
               "Operation was found aborted in the cache");
    }
    return existingResult;
  }

  //! Set the QueryExecutionContext for this particular element.
  void setQueryExecutionContext(QueryExecutionContext* executionContext) {
    _executionContext = executionContext;
  }

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
  virtual string asString(size_t indent = 0) const = 0;
  virtual size_t getResultWidth() const = 0;
  virtual void setTextLimit(size_t limit) = 0;
  virtual size_t getCostEstimate() = 0;
  virtual size_t getSizeEstimate() = 0;
  virtual float getMultiplicity(size_t col) = 0;
  virtual bool knownEmptyResult() = 0;

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
  virtual vector<size_t> resultSortedOn() const = 0;

 private:
  //! Compute the result of the query-subtree rooted at this element..
  //! Computes both, an EntityList and a HitList.
  virtual void computeResult(ResultTable* result) const = 0;

  vector<size_t> _resultSortedColumns;
  bool _hasComputedSortColumns;
};
