// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "OrderBy.h"

#include <sstream>

#include "CallFixedSize.h"
#include "Comparators.h"
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
string OrderBy::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "SORT / ORDER BY on columns:";

  // TODO<joka921> This produces exactly the same format as SORT operations
  // which is crucial for caching. Please refactor those classes to one class
  // (this is only an optimization for sorts on a single column)
  for (auto ind : _sortIndices) {
    os << (ind.second ? "desc(" : "asc(") << ind.first << ") ";
  }
  os << "\n" << _subtree->asString(indent);
  return std::move(os).str();
}

// _____________________________________________________________________________
string OrderBy::getDescriptor() const {
  std::string orderByVars;
  for (const auto& p : _subtree->getVariableColumns()) {
    for (auto oc : _sortIndices) {
      if (oc.first == p.second) {
        if (oc.second) {
          orderByVars += "DESC(" + p.first.name() + ") ";
        } else {
          orderByVars += "ASC(" + p.first.name() + ") ";
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
  AD_CONTRACT_CHECK(!_sortIndices.empty());
  shared_ptr<const ResultTable> subRes = _subtree->getResult();

  // TODO<joka921> proper timeout for sorting operations
  auto remainingTime = _timeoutTimer->wlock()->remainingTime();
  auto sortEstimateCancellationFactor =
      RuntimeParameters().get<"sort-estimate-cancellation-factor">();
  if (getExecutionContext()->getSortPerformanceEstimator().estimatedSortTime(
          subRes->size(), subRes->width()) >
      remainingTime * sortEstimateCancellationFactor) {
    // The estimated time for this sort is much larger than the actually
    // remaining time, cancel this operation
    throw ad_utility::TimeoutException(
        "OrderBy operation was canceled, because time estimate exceeded "
        "remaining time by a factor of " +
        std::to_string(sortEstimateCancellationFactor));
  }

  LOG(DEBUG) << "OrderBy result computation..." << endl;
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;

  result->_idTable = subRes->_idTable.clone();
  /*
  result->_idTable.setNumColumns(subRes->_idTable.numColumns());
  result->_idTable.insert(result->_idTable.end(), subRes->_idTable.begin(),
                          subRes->_idTable.end());
                          */

  int width = result->_idTable.numColumns();

  // TODO(florian): Check if the lambda is a performance problem
  auto comparison = [this](const auto& a, const auto& b) {
    for (auto& entry : _sortIndices) {
      if (a[entry.first] < b[entry.first]) {
        return !entry.second;
      }
      if (a[entry.first] > b[entry.first]) {
        return entry.second;
      }
    }
    return a[0] < b[0];
  };

  // We cannot use the `CALL_FIXED_SIZE` macro here because the `sort` function
  // is templated not only on the integer `I` (which the `callFixedSize`
  // function deals with) but also on the `comparison`.
  DISABLE_WARNINGS_CLANG_13
  ad_utility::callFixedSize(width, [&result, &comparison]<int I>() {
    Engine::sort<I>(&result->_idTable, comparison);
  });
  ENABLE_WARNINGS_CLANG_13
  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "OrderBy result computation done." << endl;
}
