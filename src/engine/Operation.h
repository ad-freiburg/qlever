// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <dirent.h>
#include <ev.h>
#include "../util/Log.h"
#include "../util/Exception.h"
#include "./ResultTable.h"
#include "./QueryExecutionContext.h"

using std::endl;

class Operation {
public:

  // Default Constructor.
  Operation() : _executionContext(NULL) {
  }

  // Typical Constructor.
  explicit Operation(QueryExecutionContext* executionContext) :
      _executionContext(executionContext) {
  }

  // Destructor.
  virtual ~Operation() {
    // Do NOT delete _executionContext, since
    // there is no ownership.
  }

  // Get the result for the subtree rooted at this element.
  // Use existing results if they are already available, otherwise
  // trigger computation.
  const ResultTable& getResult() const {
    LOG(TRACE) << "Get result from cache (possibly empty)" << endl;
    LOG(TRACE) << "Using key: \"" << asString() << "\"" << endl;
    ResultTable* result =
        _executionContext->getCachedResultForQueryTree(asString());
    if (result->_status != ResultTable::FINISHED) {
      LOG(TRACE) << "Result from cache is empty. Compute it." << endl;
      computeResult(result);
    }
    LOG(TRACE) << "Result should be filled." << endl;
    AD_CHECK_EQ(ResultTable::FINISHED, result->_status);
    return *result;
  }

  //! Set the QueryExecutionContext for this particular element.
  void setQueryExecutionContext(QueryExecutionContext* executionContext) {
    _executionContext = executionContext;
  }

  const Index& getIndex() const {
    return _executionContext->getIndex();
  }

  const Engine& getEngine() const {
    return _executionContext->getEngine();
  }

  // Get a unique, not ambiguous string representation for a subtree.
  // This should possible act like an ID for each subtree.
  virtual string asString() const = 0;

  virtual size_t getResultWidth() const = 0;

  virtual size_t resultSortedOn() const = 0;

protected:

  QueryExecutionContext* getExecutionContext() const {
    return _executionContext;
  }

  // The QueryExecutionContext for this particular element.
  // No ownership.
  QueryExecutionContext* _executionContext;

private:
  //! Compute the result of the query-subtree rooted at this element..
  //! Computes both, an EntityList and a HitList.
  virtual void computeResult(ResultTable* result) const = 0;
};
