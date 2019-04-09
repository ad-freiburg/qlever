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
  shared_ptr<const ResultTable> getResult() {
    ad_utility::Timer timer;
    timer.start();
    LOG(TRACE) << "Check cache for Operation result" << endl;
    LOG(TRACE) << "Using key: \n" << asString() << endl;
    auto [newResult, existingResult] =
        _executionContext->getQueryTreeCache().tryEmplace(asString());

    if (newResult) {
      LOG(TRACE) << "Not in the cache, need to compute result" << endl;
      // Passing the raw pointer here is ok as the result shared_ptr remains
      // in scope
      try {
        computeResult(newResult->_resTable.get());
      } catch (const ad_semsearch::AbortException& e) {
        newResult->_resTable->abort();
        // AbortExceptions have already been printed simply rethrow to
        // unwind the callstack until the whole query is aborted
        throw;
      } catch (const std::exception& e) {
        // Only print the Operation at the innermost (original) failure
        // then "rethrow" as special ad_semsearch::AbortException
        LOG(ERROR) << "Failed to compute Operation result for:" << endl;
        LOG(ERROR) << asString() << endl;
        LOG(ERROR) << e.what() << endl;
        newResult->_resTable->abort();
        // Rethrow as QUERY_ABORTED allowing us to print the Operation
        // only at innermost failure of a recursive call
        throw ad_semsearch::AbortException(e);
      } catch (...) {
        // Only print the Operation at the innermost (original) failure
        // then create not so weird AbortException
        LOG(ERROR) << "Failed to compute Operation result for:" << endl;
        LOG(ERROR) << asString() << endl;
        LOG(ERROR) << "WEIRD_EXCEPTION not inheriting from std::exception"
                   << endl;
        newResult->_resTable->abort();
        // Rethrow as QUERY_ABORTED allowing us to print the Operation
        // only at innermost failure of a recursive call
        throw ad_semsearch::AbortException("WEIRD_EXCEPTION");
      }
      timer.stop();
      _runtimeInformation.setRows(newResult->_resTable->size());
      _runtimeInformation.setCols(getResultWidth());
      _runtimeInformation.setColumnNames(getVariableColumns());
      _runtimeInformation.setTime(timer.msecs());
      _runtimeInformation.setWasCached(false);
      // cache the runtime information for the execution as well
      newResult->_runtimeInfo = _runtimeInformation;
      // Only now we can let other threads access the result
      // and runtime information
      newResult->_resTable->finish();
      return newResult->_resTable;
    }
    existingResult->_resTable->awaitFinished();
    if (existingResult->_resTable->status() == ResultTable::ABORTED) {
      LOG(ERROR) << "Result in the cache was aborted" << endl;
      AD_THROW(ad_semsearch::Exception::BAD_QUERY,
               "Operation was found aborted in the cache");
    }
    timer.stop();
    // If the result for this Operation came from the cache we
    // need to update get column names as caching is invariant to renames
    _runtimeInformation = existingResult->_runtimeInfo;
    _runtimeInformation.setColumnNames(getVariableColumns());
    _runtimeInformation.setTime(timer.msecs());
    _runtimeInformation.addDetail("OriginalTime",
                                  existingResult->_runtimeInfo.getTime());
    _runtimeInformation.setTime(timer.msecs());
    _runtimeInformation.setWasCached(true);
    return existingResult->_resTable;
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
  virtual ad_utility::HashMap<string, size_t> getVariableColumns() const = 0;

  RuntimeInformation& getRuntimeInfo() { return _runtimeInformation; }

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
  virtual void computeResult(ResultTable* result) = 0;

  vector<size_t> _resultSortedColumns;
  bool _hasComputedSortColumns;
  RuntimeInformation _runtimeInformation;
};
