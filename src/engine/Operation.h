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
  Operation() : _executionContext(NULL) {}

  // Typical Constructor.
  explicit Operation(QueryExecutionContext* executionContext)
      : _executionContext(executionContext) {}

  // Destructor.
  virtual ~Operation() {
    // Do NOT delete _executionContext, since
    // there is no ownership.
  }

  // Get the result for the subtree rooted at this element.
  // Use existing results if they are already available, otherwise
  // trigger computation.
  shared_ptr<const ResultTable> getResult() const {
    LOG(TRACE) << "Try to atomically emplace a new empty ResultTable" << endl;
    LOG(TRACE) << "Using key: \n" << asString() << endl;
    // TODO(schnelle) with C++17 we can use nice decomposition here
    pair<shared_ptr<ResultTable>, shared_ptr<const ResultTable>> emplacePair =
        _executionContext->getQueryTreeCache().tryEmplace(asString());
    if (emplacePair.first) {
      LOG(DEBUG) << "We were the first to emplace, need to compute result"
                 << endl;
      // Passing the raw pointer here is ok as the result shared_ptr remains
      // in scope
      computeResult(emplacePair.first.get());
      return emplacePair.first;
    }
    LOG(INFO) << "Result already (being) computed" << endl;
    emplacePair.second->awaitFinished();
    return emplacePair.second;
  }

  //! Set the QueryExecutionContext for this particular element.
  void setQueryExecutionContext(QueryExecutionContext* executionContext) {
    _executionContext = executionContext;
  }

  const Index& getIndex() const { return _executionContext->getIndex(); }

  const Engine& getEngine() const { return _executionContext->getEngine(); }

  // Get a unique, not ambiguous string representation for a subtree.
  // This should possible act like an ID for each subtree.
  virtual string asString(size_t indent = 0) const = 0;
  virtual size_t getResultWidth() const = 0;
  virtual size_t resultSortedOn() const = 0;
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

 private:
  //! Compute the result of the query-subtree rooted at this element..
  //! Computes both, an EntityList and a HitList.
  virtual void computeResult(ResultTable* result) const = 0;
};
