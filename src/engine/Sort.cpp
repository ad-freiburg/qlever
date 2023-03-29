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
  const auto& varCols = subtree_->getVariableColumns();
  for (auto sortColumn : sortColumnIndices_) {
    for (const auto& [var, varIndex] : varCols) {
      if (sortColumn == varIndex) {
        orderByVars += " " + var.name();
      }
    }
  }

  return "Sort (internal order) on" + orderByVars;
}

namespace {

// The call to `callFixedSize` is put into a separate function to get rid
// of an internal compiler error in Clang 13.
// TODO<joka921, future compilers> Check if this problem goes away in future
// compiler versions as soon as we don't support Clang 13 anymore.
void callFixedSizeForSort(auto& idTable, auto comparison) {
  ad_utility::callFixedSize(idTable.numColumns(),
                            [&idTable, comparison]<int I>() {
                              Engine::sort<I>(&idTable, comparison);
                            });
}

// The actual implementation of sorting an `IdTable` according to the
// `sortCols`.
void sortImpl(IdTable& idTable, const std::vector<ColumnIndex>& sortCols) {
  int width = idTable.numColumns();

  // Instantiate specialized comparison lambdas for one and two sort columns
  // and use a generic comparison for a higher number of sort columns.
  // TODO<joka921> As soon as we have merged the benchmark, measure whether
  // this is in fact beneficial and whether it should also be applied for a
  // higher number of columns, maybe even using `CALL_FIXED_SIZE` for the
  // number of sort columns.
  // TODO<joka921> Also experiment with sorting algorithms that take the
  // column-based structure of the `IdTable` into account.
  if (sortCols.size() == 1) {
    CALL_FIXED_SIZE(width, &Engine::sort, &idTable, sortCols.at(0));
  } else if (sortCols.size() == 2) {
    auto comparison = [c0 = sortCols[0], c1 = sortCols[1]](const auto& row1,
                                                           const auto& row2) {
      if (row1[c0] != row2[c0]) {
        return row1[c0] < row2[c0];
      } else {
        return row1[c1] < row2[c1];
      }
    };
    callFixedSizeForSort(idTable, comparison);
  } else {
    auto comparison = [&sortCols](const auto& row1, const auto& row2) {
      for (auto& col : sortCols) {
        if (row1[col] != row2[col]) {
          return row1[col] < row2[col];
        }
      }
      return false;
    };
    callFixedSizeForSort(idTable, comparison);
  }
}
}  // namespace

// _____________________________________________________________________________
ResultTable Sort::computeResult() {
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
  IdTable idTable = subRes->idTable().clone();
  sortImpl(idTable, sortColumnIndices_);

  LOG(DEBUG) << "Sort result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}
