// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "./Sort.h"

#include <sstream>

#include "CallFixedSize.h"
#include "QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t Sort::getResultWidth() const { return subtree_->getResultWidth(); }

// _____________________________________________________________________________
Sort::Sort(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           std::vector<ColumnIndex> sortColumnIndices)
    : Operation{qec},
      subtree_{std::move(subtree)},
      sortColumnIndices_{std::move(sortColumnIndices)} {}

// _____________________________________________________________________________
string Sort::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }

  os << "SORT(internal) on columns:";

  for (const auto& sortCol : sortColumnIndices_) {
    os << "asc(" << sortCol << ") ";
  }
  os << "\n" << subtree_->asString(indent);
  return std::move(os).str();
}

// _____________________________________________________________________________
string Sort::getDescriptor() const {
  std::string orderByVars;
  // TODO<joka921> Clean this up, also in the `OrderBy` class.
  for (const auto& p : subtree_->getVariableColumns()) {
    for (auto oc : sortColumnIndices_) {
      if (oc == p.second) {
        orderByVars += p.first.name();
      }
    }
  }

  return "Sort (internal order) on " + orderByVars;
}

namespace {
void callFixedSizeForSort(auto& idTable, auto comparison) {
  DISABLE_WARNINGS_CLANG_13
  ad_utility::callFixedSize(idTable.numColumns(),
                            [&idTable, comparison]<int I>() {
                              Engine::sort<I>(&idTable, comparison);
                            });
  ENABLE_WARNINGS_CLANG_13

  // The actual implementation of the sorting.
  // TODO<joka921> Benchmark and test and everything else.
  // TODO<joka921> Is it worth to instantiate further versions of the lambda,
  // maybe even using `CALL_FIXED_SIZE`?
  void sortImpl(IdTable & idTable, const std::vector<ColumnIndex>& sortCols) {
    int width = idTable.numColumns();

    if (sortCols.size() == 1) {
      CALL_FIXED_SIZE(width, &Engine::sort, &idTable, sortCols.at(0));
    } else if (sortCols.size() == 2) {
      auto comparison = [c0 = sortCols[0], c1 = sortCols[1]](const auto& rowA,
                                                             const auto& rowB) {
        if (rowA[c0] != rowB[c0]) {
          return rowA[c0] < rowB[c0];
        } else {
          return rowA[c1] < rowB[c1];
        }
      };
      callFixedSizeForSort(idTable, comparison);
    } else {
      auto comparison = [&sortCols](const auto& a, const auto& b) {
        for (auto& col : sortCols) {
          if (a[col] != b[col]) {
            return a[col] < b[col];
          }
        }
        return false;
      };
      callFixedSizeForSort(idTable, comparison);
    }
  }
}

// _____________________________________________________________________________
void Sort::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for Sort result computation..." << endl;
  shared_ptr<const ResultTable> subRes = subtree_->getResult();

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
        "Sort operation was canceled, because time estimate exceeded "
        "remaining time by a factor of " +
        std::to_string(sortEstimateCancellationFactor));
  }

  LOG(DEBUG) << "Sort result computation..." << endl;
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  result->_idTable = subRes->_idTable.clone();
  result->_sortedBy = resultSortedOn();
  sortImpl(result->_idTable, sortColumnIndices_);

  LOG(DEBUG) << "Sort result computation done." << endl;
}
