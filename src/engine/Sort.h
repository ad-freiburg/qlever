// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <unordered_map>
#include "./Operation.h"
#include "./QueryExecutionTree.h"


using std::list;
using std::unordered_map;


class Sort : public Operation {
  public:
    virtual size_t getResultWidth() const;

  public:

    Sort(QueryExecutionContext *qec, const QueryExecutionTree& subtree,
         size_t sortCol);

    Sort(const Sort& other);

    Sort& operator=(const Sort& other);

    virtual ~Sort();

    virtual string asString() const;

    virtual size_t resultSortedOn() const { return _sortCol; }

    virtual void setTextLimit(size_t limit) {
      _subtree->setTextLimit(limit);
    }

    virtual size_t getSizeEstimate() const {
      return _subtree->getSizeEstimate();
    }

    QueryExecutionTree* getSubtree() const {
      return _subtree;
    }

    virtual size_t getCostEstimate() const {
      size_t size = getSizeEstimate();
      size_t logSize = std::max(size_t(2),
          static_cast<size_t>(logb(static_cast<double>(size))));
      size_t nlogn = size * logSize;
      size_t subcost = _subtree->getCostEstimate();
      return nlogn + subcost;
    }

    virtual bool knownEmptyResult() const {
      return _subtree->knownEmptyResult();
    }

  private:
    QueryExecutionTree *_subtree;
    size_t _sortCol;

    virtual void computeResult(ResultTable *result) const;
};
