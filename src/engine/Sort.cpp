// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include "./Sort.h"
#include "QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t Sort::getResultWidth() const {
  return _subtree->getResultWidth();
}

// _____________________________________________________________________________
Sort::Sort(QueryExecutionContext* qec, const QueryExecutionTree& subtree,
    size_t sortCol) :
    Operation(qec),
    _subtree(new QueryExecutionTree(subtree)),
    _sortCol(sortCol) {
}

// _____________________________________________________________________________
Sort::Sort(const Sort& other) :
    Operation(other._executionContext),
    _subtree(new QueryExecutionTree(*other._subtree)),
    _sortCol(other._sortCol) {
}

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
  LOG(DEBUG) << "Sort result computation..." << endl;
  const ResultTable& subRes = _subtree->getResult();
  result->_nofColumns = subRes._nofColumns;
  switch (subRes._nofColumns) {
    case 1: {
      auto res = new vector<array<Id, 1>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 1>>*>(subRes._fixedSizeData);
      getEngine().sort(*res, _sortCol);
    }
      break;
    case 2: {
      auto res = new vector<array<Id, 2>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 2>>*>(subRes._fixedSizeData);
      getEngine().sort(*res, _sortCol);
      break;
    }
    case 3: {
      auto res = new vector<array<Id, 3>>();
      *res = *static_cast<vector<array<Id, 3>>*>(subRes._fixedSizeData);
      getEngine().sort(*res, _sortCol);
      result->_fixedSizeData = res;
      break;
    }
    case 4: {
      auto res = new vector<array<Id, 4>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 4>>*>(subRes._fixedSizeData);
      getEngine().sort(*res, _sortCol);
      break;
    }
    case 5: {
      auto res = new vector<array<Id, 5>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 5>>*>(subRes._fixedSizeData);
      getEngine().sort(*res, _sortCol);
      break;
    }
    default: {
      result->_varSizeData = subRes._varSizeData;
      getEngine().sort(result->_varSizeData, _sortCol);
      break;
    }
  }
  result->_sortedBy = _sortCol;
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "Sort result computation done." << endl;
}
