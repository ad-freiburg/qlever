// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>

#include "./Comparators.h"
#include "./OrderBy.h"
#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t OrderBy::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
OrderBy::OrderBy(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> subtree,
                 const vector<pair<size_t, bool>>& sortIndices)
    : Operation(qec), _subtree(subtree), _sortIndices(sortIndices) {}

// _____________________________________________________________________________
string OrderBy::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "OrderBy " << _subtree->asString(indent) << "\n";
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "order on ";
  for (auto ind : _sortIndices) {
    os << (ind.second ? "desc(" : "asc(") << ind.first << ") ";
  }
  return os.str();
}

// _____________________________________________________________________________
void OrderBy::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Gettign sub-result for OrderBy result computation..." << endl;
  AD_CHECK(_sortIndices.size() > 0);
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "OrderBy result computation..." << endl;
  result->_nofColumns = subRes->_nofColumns;
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  switch (subRes->_nofColumns) {
    case 1: {
      auto res = new vector<array<Id, 1>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 1>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, OBComp<array<Id, 1>>(_sortIndices));
    } break;
    case 2: {
      auto res = new vector<array<Id, 2>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 2>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, OBComp<array<Id, 2>>(_sortIndices));
      break;
    }
    case 3: {
      auto res = new vector<array<Id, 3>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 3>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, OBComp<array<Id, 3>>(_sortIndices));
      break;
    }
    case 4: {
      auto res = new vector<array<Id, 4>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 4>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, OBComp<array<Id, 4>>(_sortIndices));
      break;
    }
    case 5: {
      auto res = new vector<array<Id, 5>>();
      result->_fixedSizeData = res;
      *res = *static_cast<vector<array<Id, 5>>*>(subRes->_fixedSizeData);
      getEngine().sort(*res, OBComp<array<Id, 5>>(_sortIndices));
      break;
    }
    default: {
      result->_varSizeData = subRes->_varSizeData;
      getEngine().sort(result->_varSizeData, OBComp<vector<Id>>(_sortIndices));
      break;
    }
  }
  result->_sortedBy = (_sortIndices[0].second ? result->_nofColumns + 1
                                              : _sortIndices[0].first);
  result->finish();
  LOG(DEBUG) << "OrderBy result computation done." << endl;
}
