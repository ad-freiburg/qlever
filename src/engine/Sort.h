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

  Sort(QueryExecutionContext* qec, const QueryExecutionTree& subtree,
      size_t sortCol);

  Sort(const Sort& other);

  Sort& operator=(const Sort& other);

  virtual ~Sort();

  virtual string asString() const;
  virtual size_t resultSortedOn() const { return _sortCol; }

private:
  QueryExecutionTree* _subtree;
  size_t _sortCol;

  virtual void computeResult(ResultTable* result) const;
};
