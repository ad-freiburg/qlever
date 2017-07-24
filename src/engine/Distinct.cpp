// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include "./QueryExecutionTree.h"
#include "./Distinct.h"


using std::string;

// _____________________________________________________________________________
size_t Distinct::getResultWidth() const {
  return _subtree->getResultWidth();
}

// _____________________________________________________________________________
Distinct::Distinct(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   const vector<size_t>& keepIndices) :
    Operation(qec),
    _subtree(subtree),
    _keepIndices(keepIndices) {
}


// _____________________________________________________________________________
string Distinct::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "Distinct " << _subtree->asString(indent);
  return os.str();
}

// _____________________________________________________________________________
void Distinct::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Getting sub-result for distinct result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "Distinct result computation..." << endl;
  result->_nofColumns = subRes->_nofColumns;
  switch (subRes->_nofColumns) {
    case 1: {
      typedef array<Id, 1> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 2: {
      typedef array<Id, 2> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 3: {
      typedef array<Id, 3> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 4: {
      typedef array<Id, 4> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           _keepIndices, res);
      break;
    }
    case 5: {
      typedef array<Id, 5> RT;
      auto res = new vector<RT>();
      result->_fixedSizeData = res;
      getEngine().distinct(*static_cast<vector<RT>*>(subRes->_fixedSizeData),
                           _keepIndices, res);
      break;
    }
    default: {
      getEngine().distinct(subRes->_varSizeData, _keepIndices,
                           &result->_varSizeData);
      break;
    }
  }
  result->finish();
  LOG(DEBUG) << "Distinct result computation done." << endl;
}
