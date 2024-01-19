// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/OrderBy.h"

#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/Comparators.h"
#include "engine/QueryExecutionTree.h"
#include "global/ValueIdComparators.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
size_t OrderBy::getResultWidth() const { return subtree_->getResultWidth(); }

// _____________________________________________________________________________
OrderBy::OrderBy(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> subtree,
                 vector<pair<ColumnIndex, bool>> sortIndices)
    : Operation{qec},
      subtree_{std::move(subtree)},
      sortIndices_{std::move(sortIndices)} {
  AD_CONTRACT_CHECK(!sortIndices_.empty());
  AD_CONTRACT_CHECK(std::ranges::all_of(
      sortIndices_,
      [this](ColumnIndex index) { return index < getResultWidth(); },
      ad_utility::first));
}

// _____________________________________________________________________________
string OrderBy::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "ORDER BY on columns:";

  // TODO<joka921> This produces exactly the same format as SORT operations
  // which is crucial for caching. Please refactor those classes to one class
  // (this is only an optimization for sorts on a single column)
  for (auto ind : sortIndices_) {
    os << (ind.second ? "desc(" : "asc(") << ind.first << ") ";
  }
  os << "\n" << subtree_->getCacheKey();
  return std::move(os).str();
}

// _____________________________________________________________________________
string OrderBy::getDescriptor() const {
  std::string orderByVars;
  const auto& varCols = subtree_->getVariableColumns();
  for (auto [sortIndex, isDescending] : sortIndices_) {
    for (const auto& [var, varIndex] : varCols) {
      if (sortIndex == varIndex.columnIndex_) {
        using namespace std::string_literals;
        std::string s = isDescending ? " DESC("s : " ASC("s;
        orderByVars += s + var.name() + ")";
      }
    }
  }
  return "OrderBy on" + orderByVars;
}

// _____________________________________________________________________________
ResultTable OrderBy::computeResult() {
  LOG(DEBUG) << "Getting sub-result for OrderBy result computation..." << endl;
  shared_ptr<const ResultTable> subRes = subtree_->getResult();

  // TODO<joka921> proper timeout for sorting operations
  auto sortEstimateCancellationFactor =
      RuntimeParameters().get<"sort-estimate-cancellation-factor">();
  if (getExecutionContext()->getSortPerformanceEstimator().estimatedSortTime(
          subRes->size(), subRes->width()) >
      remainingTime() * sortEstimateCancellationFactor) {
    // The estimated time for this sort is much larger than the actually
    // remaining time, cancel this operation
    throw ad_utility::CancellationException(
        "OrderBy operation was canceled, because time estimate exceeded "
        "remaining time by a factor of " +
        std::to_string(sortEstimateCancellationFactor));
  }

  LOG(DEBUG) << "OrderBy result computation..." << endl;
  IdTable idTable = subRes->idTable().clone();

  size_t width = idTable.numColumns();

  // TODO<joka921> Measure (as soon as we have the benchmark merged)
  // whether it is beneficial to manually instantiate the comparison when
  // sorting by only one or two columns.

  // TODO<joka921> In the case of a single variable, it might be more efficient
  // to first sort by the ID values and then "repair" the resulting range by
  // some O(n) algorithms, or even by returning lazy generators that yield
  // the repaired order.

  // TODO<joka921> For proper sorting of the local vocab we also need to
  // add some logic for the proper sorting.

  // TODO<joka921> Undefined values should always be at the end, no matter
  // if the ordering is ascending or descending.

  // TODO<joka921> If we know, that all the sort columns contain only datatypes
  // for which the `internal` order is also the `semantic` order, or if a column
  // only contains a single datatype, then we can use more efficient
  // implementations here.

  // Return true iff `rowA` comes before `rowB` in the sort order specified by
  // `sortIndices_`.
  auto comparison = [this](const auto& row1, const auto& row2) -> bool {
    for (auto& [column, isDescending] : sortIndices_) {
      if (row1[column] == row2[column]) {
        continue;
      }
      bool isLessThan =
          toBoolNotUndef(valueIdComparators::compareIds<
                         valueIdComparators::ComparisonForIncompatibleTypes::
                             CompareByType>(
              row1[column], row2[column], valueIdComparators::Comparison::LT));
      return isLessThan != isDescending;
    }
    return false;
  };

  // We cannot use the `CALL_FIXED_SIZE` macro here because the `sort` function
  // is templated not only on the integer `I` (which the `callFixedSize`
  // function deals with) but also on the `comparison`.
  ad_utility::callFixedSize(width, [&idTable, &comparison]<size_t I>() {
    Engine::sort<I>(&idTable, comparison);
  });
  LOG(DEBUG) << "OrderBy result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}
