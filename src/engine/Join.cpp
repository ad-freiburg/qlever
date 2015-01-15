// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <string>
#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, const QueryExecutionTree& left,
    const QueryExecutionTree& right, bool keepJoinColumn) :
    Operation(qec),
    _left(new QueryExecutionTree(left)),
    _right(new QueryExecutionTree(right)),
    _keepJoinColumn(keepJoinColumn) {
}

// _____________________________________________________________________________
Join::~Join() {
  delete _left;
  delete _right;
}


// _____________________________________________________________________________
Join::Join(const Join& other) :
    Operation(other._executionContext),
    _left(new QueryExecutionTree(*other._left)),
    _right(new QueryExecutionTree(*other._right)),
    _keepJoinColumn(other._keepJoinColumn) {
}

// _____________________________________________________________________________
Join& Join::operator=(const Join& other) {
  _executionContext = other._executionContext;
  delete _left;
  _left = new QueryExecutionTree(*other._left);
  delete _right;
  _right = new QueryExecutionTree(*other._right);
  _keepJoinColumn = other._keepJoinColumn;
  return *this;
}

// _____________________________________________________________________________
string Join::asString() const {
  return "";
}

// _____________________________________________________________________________
void Join::computeResult(ResultTable* result) const {
}
