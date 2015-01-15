// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include "./Operation.h"
#include "./IndexScan.h"

using std::list;

// Forward declare QueryExecutionTree, the type of subtrees iunder a join.
class QueryExecutionTree;

class Join : public Operation {
public:

  Join(QueryExecutionContext* qec, const QueryExecutionTree& left,
      const QueryExecutionTree& right, bool keepJoinColumn = true);

  Join(const Join& other);
  Join& operator=(const Join& other);

  virtual ~Join();

//  void setLeftOperand(const QueryExecutionTree& op, size_t joinColumn) {
//    delete _left;
//    _left = new QueryExecutionTree(op);
//    _leftJoinCol = joinColumn;
//  }
//
//  void setRightOperand(const QueryExecutionTree& op, size_t joinColumn) {
//    delete _right;
//    _right = new QueryExecutionTree(op);
//    _rightJoinCol = joinColumn;
//  }

  virtual string asString() const;

private:
  QueryExecutionTree* _left;
  QueryExecutionTree* _right;

  size_t _leftJoinCol;
  size_t _rightJoinCol;

  bool _keepJoinColumn;

  virtual void computeResult(ResultTable* result) const;
};
