// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./Sort.h"
#include "QueryExecutionTree.h"

using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t Sort::getResultWidth() const {
  return _subtree->getResultWidth();
}

// _____________________________________________________________________________
Sort::Sort(QueryExecutionContext* qec, const QueryExecutionTree& subtree,
    size_t sortCol) :
    Operation(qec),
    _subtree(new QueryExecutionTree(subtree)),
    _sortCol(sortCol) { }

// _____________________________________________________________________________
Sort::Sort(const Sort& other) :
    Operation(other._executionContext),
    _subtree(new QueryExecutionTree(*other._subtree)),
    _sortCol(other._sortCol) { }

// _____________________________________________________________________________
Sort& Sort::operator=(const Sort& other) {
  delete _subtree;
  _executionContext = other._executionContext;
  _subtree = new QueryExecutionTree(*other._subtree);
  _sortCol = other._sortCol;
  return *this;
}

// _____________________________________________________________________________
Sort::~Sort() {
  delete _subtree;
}

// _____________________________________________________________________________
string Sort::asString() const {
  std::ostringstream os;
  os << "SORT " << _subtree->asString() << " on " << _sortCol;
  return os.str();
}

// _____________________________________________________________________________
void Sort::computeResult(ResultTable* result) const {

}
