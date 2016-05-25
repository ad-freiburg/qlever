// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "./Distinct.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t Distinct::getResultWidth() const {
  return _subtree->getResultWidth();
}

// _____________________________________________________________________________
Distinct::Distinct(QueryExecutionContext* qec,
                   const QueryExecutionTree& subtree,
                   const vector<size_t>& keepIndices) :
    Operation(qec),
    _subtree(new QueryExecutionTree(subtree)),
    _keepIndices(keepIndices) {
}

// _____________________________________________________________________________
Distinct::Distinct(const Distinct& other) :
    Operation(other._executionContext),
    _subtree(new QueryExecutionTree(*other._subtree)),
    _keepIndices(other._keepIndices) {
}

// _____________________________________________________________________________
Distinct& Distinct::operator=(const Distinct& other) {
  delete _subtree;
  _executionContext = other._executionContext;
  _subtree = new QueryExecutionTree(*other._subtree);
  _keepIndices = other._keepIndices;
  return *this;
}

// _____________________________________________________________________________
Distinct::~Distinct() {
  delete _subtree;
}

// _____________________________________________________________________________
string Distinct::asString() const {
  std::ostringstream os;
  os << "Distinct " << _subtree->asString();
  return os.str();
}

// _____________________________________________________________________________
void Distinct::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Distinct result computation..." << endl;
  const ResultTable& subRes = _subtree->getResult();
  result->_nofColumns = subRes._nofColumns;
  switch (subRes._nofColumns) {
    case 1: {
      typedef array<Id, 1> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes._fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 2: {
      typedef array<Id, 2> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes._fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 3: {
      typedef array<Id, 3> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes._fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 4: {
      typedef array<Id, 4> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes._fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 5: {
      typedef array<Id, 5> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes._fixedSizeData),
                           _keepIndices, res);
      break;
    }
    default: {
      getEngine().distinct(subRes._varSizeData, _keepIndices,
                           &result->_varSizeData);
      break;
    }
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "Distinct result computation done." << endl;
}
