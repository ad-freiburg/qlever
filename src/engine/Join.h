// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <unordered_map>
#include <grp.h>
#include "./Operation.h"
#include "./IndexScan.h"

using std::list;
using std::unordered_map;

// Forward declare QueryExecutionTree, the type of subtrees iunder a join.
class QueryExecutionTree;

class Join : public Operation {
public:
  virtual size_t getResultWidth() const;

public:

  Join(QueryExecutionContext* qec, const QueryExecutionTree& t1,
      const QueryExecutionTree& t2, size_t t1JoinCol, size_t t2JoinCol,
      bool keepJoinColumn = true);

  Join(const Join& other);

  Join& operator=(const Join& other);

  virtual ~Join();

  virtual string asString() const;

  unordered_map<string, size_t> getVariableColumns() const;

private:
  QueryExecutionTree* _left;
  QueryExecutionTree* _right;

  size_t _leftJoinCol;
  size_t _rightJoinCol;

  bool _keepJoinColumn;

  virtual void computeResult(ResultTable* result) const;
};
