// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>

#include "CallFixedSize.h"
#include "Comparators.h"
#include "OrderBy.h"
#include "QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t OrderBy::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
OrderBy::OrderBy(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> subtree,
                 vector<pair<size_t, bool>> sortIndices)
    : Operation(qec),
      _subtree(std::move(subtree)),
      _sortIndices(std::move(sortIndices)) {}

// _____________________________________________________________________________
string OrderBy::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }

  std::stringstream columns;
  // TODO<joka921> This produces exactly the same format as SORT operations
  // which is crucial for caching. Please refactor those classes to one class
  // (this is only an optimization for sorts on a single column);
  for (auto ind : _sortIndices) {
    columns << (ind.second ? "desc(" : "asc(") << ind.first << ") ";
  }
  os << "SORT / ORDER BY on columns:" << columns.str() << "\n"
     << _subtree->asString(indent);
  return os.str();
}

// _____________________________________________________________________________
string OrderBy::getDescriptor() const {
  std::string orderByVars;
  for (const auto& p : _subtree->getVariableColumns()) {
    for (auto oc : _sortIndices) {
      if (oc.first == p.second) {
        if (oc.second) {
          orderByVars += "DESC(" + p.first + ") ";
        } else {
          orderByVars += "ASC(" + p.first + ") ";
        }
      }
    }
  }
  return "OrderBy (Sort) on " + orderByVars;
}

// _____________________________________________________________________________
vector<size_t> OrderBy::resultSortedOn() const {
  std::vector<size_t> sortedOn;
  sortedOn.reserve(_sortIndices.size());
  for (const pair<size_t, bool>& p : _sortIndices) {
    if (!p.second) {
      // Only ascending columns count as sorted.
      sortedOn.push_back(p.first);
    }
  }
  return sortedOn;
}

// _____________________________________________________________________________
void OrderBy::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Gettign sub-result for OrderBy result computation..." << endl;
  AD_CHECK(!_sortIndices.empty());
  shared_ptr<const ResultTable> subRes = _subtree->getResult();

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
  LOG(DEBUG) << "OrderBy result computation..." << endl;
  result->_data.setCols(subRes->_data.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  result->_data.insert(result->_data.end(), subRes->_data.begin(),
                       subRes->_data.end());

  int width = result->_data.cols();
  // TODO(florian): Check if the lambda is a performance problem
  CALL_FIXED_SIZE_1(width, getEngine().sort, &result->_data,
                    [this](const auto& a, const auto& b) {
                      for (auto& entry : _sortIndices) {
                        if (a[entry.first] < b[entry.first]) {
                          return !entry.second;
                        }
                        if (a[entry.first] > b[entry.first]) {
                          return entry.second;
                        }
                      }
                      return a[0] < b[0];
                    });
  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "OrderBy result computation done." << endl;
}
