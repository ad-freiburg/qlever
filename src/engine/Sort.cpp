// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Sort.h"
#include <sstream>
#include "QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t Sort::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Sort::Sort(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree, size_t sortCol)
    : Operation(qec), _subtree(subtree), _sortCol(sortCol) {}

// _____________________________________________________________________________
string Sort::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "SORT on column:" << _sortCol << "\n" << _subtree->asString(indent);
  return os.str();
}

// _____________________________________________________________________________
void Sort::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Getting sub-result for Sort result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "Sort result computation..." << endl;
  result->_nofColumns = subRes->_nofColumns;
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  switch (subRes->_nofColumns) {
    case 1: {
      auto res = new vector<array<Id, 1>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 1>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, _sortCol);
    } break;
    case 2: {
      auto res = new vector<array<Id, 2>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 2>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, _sortCol);
      break;
    }
    case 3: {
      auto res = new vector<array<Id, 3>>();
      *res = *static_cast<vector<array<Id, 3>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, _sortCol);
      result->_fixedSizeData = res;
      break;
    }
    case 4: {
      auto res = new vector<array<Id, 4>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 4>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, _sortCol);
      break;
    }
    case 5: {
      auto res = new vector<array<Id, 5>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 5>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, _sortCol);
      break;
    }
    default: {
      result->_varSizeData = subRes->_varSizeData;
      getEngine().sort(result->_varSizeData, _sortCol);
      break;
    }
  }
  result->_sortedBy = _sortCol;
  result->finish();
  LOG(DEBUG) << "Sort result computation done." << endl;
}
