// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/OrderBy.h"

#include <cmath>
#include <limits>
#include <sstream>
#include <utility>

#include "engine/CallFixedSize.h"
#include "engine/ColumnStrippingHelpers.h"
#include "engine/IndexScan.h"
#include "engine/OrderByComparator.h"
#include "engine/OrderByOnSortedNumericInput.h"
#include "engine/QueryExecutionTree.h"
#include "global/RuntimeParameters.h"
#include "global/ValueIdComparators.h"
#include "index/IdTableUtils.h"
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
size_t OrderBy::getCostEstimate() {
  size_t size = getSizeEstimateBeforeLimit();
  size_t subcost = subtree_->getCostEstimate();

  // If there is a single sort column and the input is already sorted by it,
  // return the number of rows instead of `n log n` (plus the cost of the
  // input), because the result can then often be computed in linear time (see
  // `computeResultForSortedInput`).
  //
  // NOTE: The planner does not know whether the fast path applies (the column
  // must contain only ints or only doubles), so this assumes that it does. If
  // it does not, the cost is underestimated, and the input is sorted as
  // before. In the worst case, the planner then prefers a sorted subtree that
  // is more expensive than an unsorted alternative.
  if (hasSingleSortColumnWithSortedInput()) {
    return size + subcost;
  }

  // Otherwise, return the cost of sorting, `n log n`, plus the cost of the
  // input.
  //
  // NOTE: `logb(0)` is `-inf`, which must not be cast to an integer.
  size_t logSize = std::max(
      size_t(1), static_cast<size_t>(
                     logb(static_cast<double>(std::max(size, size_t(1))))));
  return size * logSize + subcost;
}

// _____________________________________________________________________________
bool OrderBy::hasSingleSortColumnWithSortedInput() const {
  const auto& sortedOn = subtree_->resultSortedOn();
  return sortIndices_.size() == 1 && !sortedOn.empty() &&
         sortedOn.front() == sortIndices_.front().first;
}

// _____________________________________________________________________________
std::optional<IdTable> OrderBy::computeResultForSortedInput(
    const IdTableView<0>& input) const {
  if (!hasSingleSortColumnWithSortedInput()) {
    return std::nullopt;
  }
  auto [column, isDescending] = sortIndices_.front();
  return orderByOnSortedNumericInput::computeOrderBy(
      input, column, isDescending, getLimitOffset(), allocator());
}

// _____________________________________________________________________________
Result OrderBy::computeResult([[maybe_unused]] bool requestLaziness) {
  using std::endl;
  AD_LOG_DEBUG << "Getting sub-result for OrderBy result computation..."
               << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult();
  const auto& subTable = subRes->idTableView();

  // Record how many blocks an `IndexScan` below reads because of the `LIMIT`
  // (see `onLimitOffsetChanged`).
  if (blocksReadForLimit_.has_value()) {
    runtimeInfo().addDetail("num-blocks-read-for-limit",
                            blocksReadForLimit_->numBlocksRead_);
    runtimeInfo().addDetail("num-blocks-total",
                            blocksReadForLimit_->numBlocksTotal_);
  }

  // Take the fast path for a sorted numeric input if it applies (see
  // `computeResultForSortedInput`), and record that in the runtime
  // information.
  if (auto result = computeResultForSortedInput(subTable)) {
    runtimeInfo().addDetail("sorted-numeric-input", true);
    checkCancellation();
    return {std::move(result).value(), resultSortedOn(),
            subRes->getSharedLocalVocab()};
  }

  // The blocks of an `IndexScan` are only restricted if the block metadata
  // guarantees that the fast path applies (see
  // `orderByOnSortedNumericInput::selectBlocksForLimit`), so the following must
  // not happen: sorting a subset of the rows would give a wrong result.
  AD_CORRECTNESS_CHECK(!blocksReadForLimit_.has_value());

  AD_LOG_DEBUG << "OrderBy result computation..." << endl;
  IdTable idTable = subRes->cloneIdTable(cancellationChecker());

  size_t width = idTable.numColumns();

  // TODO<joka921> Measure (as soon as we have the benchmark merged)
  // whether it is beneficial to manually instantiate the comparison when
  // sorting by only one or two columns.
  //
  // TODO<joka921> In the case of a single variable whose input is not sorted
  // by it, it might be more efficient to first sort by the ID values and then
  // "repair" the order in linear time, like `computeResultForSortedInput` does
  // for an input that is already sorted, or even to return lazy generators
  // that yield the repaired order. Besides the numbers, such a repair would
  // also have to move the IRIs of the vocabulary before its literals, which
  // come first in the internal order (see `OrderByComparator`).
  //
  // TODO<joka921> For proper sorting of the local vocab we also need to
  // add some logic for the proper sorting.
  //
  // TODO<joka921> If we know, that all the sort columns contain only datatypes
  // for which the `internal` order is also the `semantic` order, then we can
  // use more efficient implementations here. Note that a single datatype is
  // not enough: e.g. for `VocabIndex`, the internal order puts all literals
  // before all IRIs, but `ORDER BY` puts the IRIs first. So far, this is only
  // done for a single sort column with an input that is already sorted and
  // contains only ints or only doubles, see `computeResultForSortedInput`.
  //
  OrderByComparator orderByComparator{
      getExecutionContext()->getLocalVocabContext()};

  // Return true iff `row1` comes before `row2` in the sort order specified by
  // `sortIndices_`: the first column whose values differ decides, in the
  // direction of that column. Equal values of different `Id`s (e.g. `5` and
  // `5.0`) let the next column decide.
  auto comparison = [this, orderByComparator](const auto& row1,
                                              const auto& row2) -> bool {
    for (auto& [column, isDescending] : sortIndices_) {
      int result = orderByComparator.compare(row1[column], row2[column]);
      if (result != 0) {
        return isDescending ? result > 0 : result < 0;
      }
    }
    return false;
  };

  // We cannot use the `CALL_FIXED_SIZE` macro here because the `sort` function
  // is templated not only on the integer `I` (which the `callFixedSize`
  // function deals with) but also on the `comparison`.
  ad_utility::callFixedSizeVi(
      width, [this, &idTable, &comparison](auto numColumns) {
        IdTableUtils::sort<numColumns>(&idTable, comparison, sortConfig(),
                                       cancellationHandle());
      });
  AD_LOG_DEBUG << "OrderBy result computation done." << endl;
  Result result{std::move(idTable), resultSortedOn(),
                subRes->getSharedLocalVocab()};
  // If this operation was promised to handle the `LIMIT`/`OFFSET` itself (see
  // `handlesLimitOffset`), but the fast path did not apply, apply it here.
  if (handlesLimitOffset() == LimitOffsetHandling::FULL) {
    result.applyLimitOffset(getLimitOffset(), ad_utility::noop);
  }
  return result;
}

// _____________________________________________________________________________
LimitOffsetHandling OrderBy::handlesLimitOffset() const {
  return hasSingleSortColumnWithSortedInput() ? LimitOffsetHandling::FULL
                                              : LimitOffsetHandling::NONE;
}

// _____________________________________________________________________________
void OrderBy::onLimitOffsetChanged(const LimitOffsetClause&, bool) {
  // The complete `LIMIT`/`OFFSET` of this operation (all calls merged).
  const auto& limitOffset = getLimitOffset();
  if (handlesLimitOffset() != LimitOffsetHandling::FULL ||
      !limitOffset._limit.has_value()) {
    return;
  }
  auto scan =
      std::dynamic_pointer_cast<IndexScan>(subtree_->getRootOperation());
  if (scan == nullptr) {
    return;
  }
  size_t numRows = limitOffset.upperBound(std::numeric_limits<size_t>::max());
  bool isDescending = sortIndices_.front().second;
  std::optional<NumBlocksReadForLimit> blocksReadForLimit;
  auto restrictedScan = scan->makeCopyWithSelectedBlocks(
      [numRows, isDescending,
       &blocksReadForLimit](const IndexScan::BlocksOfSortedVariable& blocks) {
        auto selected = orderByOnSortedNumericInput::selectBlocksForLimit(
            blocks, numRows, isDescending);
        if (selected.has_value()) {
          blocksReadForLimit =
              NumBlocksReadForLimit{selected->size(), blocks.blocks_.size()};
        }
        return selected;
      });
  if (restrictedScan.has_value()) {
    subtree_ = std::move(restrictedScan).value();
    blocksReadForLimit_ = blocksReadForLimit;
  }
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
  auto result = std::make_unique<OrderBy>(_executionContext, subtree_->clone(),
                                          sortIndices_);
  // The cloned subtree keeps the selected blocks.
  result->blocksReadForLimit_ = blocksReadForLimit_;
  return result;
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
OrderBy::makeTreeWithStrippedColumns(
    const std::set<Variable>& requestedVariables) const {
  // The subtree has to provide the requested variables and the variables that
  // the `OrderBy` sorts by (those at `sortIndices_`).
  columnStrippingHelpers::VarsRequiredFromSubtree varsRequiredFromSubtree(
      &requestedVariables);
  std::vector<std::pair<Variable, bool>> sortVars;
  for (const auto& [columnIndex, sortOrder] : sortIndices_) {
    const auto& sortVariable =
        subtree_->getVariableAndInfoByColumnIndex(columnIndex).first;
    sortVars.push_back(std::pair{sortVariable, sortOrder});
    varsRequiredFromSubtree.add(sortVariable);
  }

  // Strip the subtree and translate `sortIndices_` to the column indices of
  // the stripped subtree.
  auto subtree = QueryExecutionTree::makeTreeWithStrippedColumns(
      subtree_, varsRequiredFromSubtree.get());
  std::vector<std::pair<ColumnIndex, bool>> newSortIndices;
  for (const auto& [sortVariable, sortOrder] : sortVars) {
    newSortIndices.emplace_back(subtree->getVariableColumn(sortVariable),
                                sortOrder);
  }

  // The new `OrderBy` also exports sorting variables that the parent did
  // not request, the helper adds a `StripColumns` operation for them.
  return columnStrippingHelpers::makeTreeWithOptionalStripOperation<OrderBy>(
      getExecutionContext(), requestedVariables, std::move(subtree),
      newSortIndices);
}
