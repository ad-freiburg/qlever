// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/OrderBy.h"

#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/QueryExecutionTree.h"
#include "global/RuntimeParameters.h"
#include "global/ValueIdComparators.h"
#include "util/TransparentFunctors.h"

// _____________________________________________________________________________
size_t OrderBy::getResultWidth() const { return subtree_->getResultWidth(); }

// _____________________________________________________________________________
OrderBy::OrderBy(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> subtree,
                 std::vector<std::pair<ColumnIndex, bool>> sortIndices)
    : Operation{qec},
      subtree_{std::move(subtree)},
      sortIndices_{std::move(sortIndices)} {
  AD_CONTRACT_CHECK(!sortIndices_.empty());
  AD_CONTRACT_CHECK(ql::ranges::all_of(
      sortIndices_,
      [this](ColumnIndex index) { return index < getResultWidth(); },
      ad_utility::first));
}

// _____________________________________________________________________________
std::string OrderBy::getCacheKeyImpl() const {
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
std::string OrderBy::getDescriptor() const {
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
Result OrderBy::computeResult([[maybe_unused]] bool requestLaziness) {
  using std::endl;
  LOG(DEBUG) << "Getting sub-result for OrderBy result computation..." << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult();

  // TODO<joka921> proper timeout for sorting operations
  const auto& subTable = subRes->idTable();
  getExecutionContext()->getSortPerformanceEstimator().throwIfEstimateTooLong(
      subTable.numRows(), subTable.numColumns(), deadline_,
      "Sort for COUNT(DISTINCT *)");

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
  ad_utility::callFixedSizeVi(width, [&idTable, &comparison](auto I) {
    Engine::sort<I>(&idTable, comparison);
  });
  // We can't check during sort, so reset status here
  cancellationHandle_->resetWatchDogState();
  checkCancellation();
  LOG(DEBUG) << "OrderBy result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// ___________________________________________________________________
OrderBy::SortedVariables OrderBy::getSortedVariables() const {
  SortedVariables result;
  for (const auto& [colIdx, isDescending] : sortIndices_) {
    using enum AscOrDesc;
    result.emplace_back(subtree_->getVariableAndInfoByColumnIndex(colIdx).first,
                        isDescending ? Desc : Asc);
  }
  return result;
}

// _____________________________________________________________________________
std::unique_ptr<Operation> OrderBy::cloneImpl() const {
  return std::make_unique<OrderBy>(_executionContext, subtree_->clone(),
                                   sortIndices_);
}
