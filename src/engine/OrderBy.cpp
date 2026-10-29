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

namespace {
// A contiguous range `[begin_, end_)` of rows of an `IdTable`. If `reversed_`
// is true, the rows are to be output in reverse order.
struct RowRange {
  size_t begin_;
  size_t end_;
  bool reversed_;
};

// If the `column`, which must be sorted in the internal order of the `Id`s
// (that is, by their bits), contains only `Int`s or only `Double`s, possibly
// preceded by `Undefined` values, return the row ranges which, concatenated,
// yield the column in the ascending order of `ORDER BY`. Otherwise return
// `std::nullopt`.
//
// The datatype bits are the most significant bits of an `Id`, so a column that
// is sorted by bits is grouped by datatype, and the check is O(1) apart from
// the binary searches for the range boundaries. The internal order deviates
// from the semantic order as follows (see `valueIdComparators::compareByBits`):
// ints are `[0 .. max, min .. -1]`, doubles are `[0.0 .. +inf, NaN, -0.0 ..
// -inf, -NaN]`, where the sign bit of a `NaN` may be set as well. `ORDER BY`
// puts all `NaN`s after all other doubles (see `OrderByComparator`) and
// `Undefined` before everything else.
std::optional<std::vector<RowRange>> getRowRangesForSortedNumericColumn(
    ConstIdColumnRef column) {
  // Return the index of the first row in `[begin, end)` for which the
  // `predicate` is false. The predicate must be monotone on that range.
  auto partitionPoint = [&column](size_t begin, size_t end, auto predicate) {
    return static_cast<size_t>(
        ql::ranges::partition_point(column.begin() + begin,
                                    column.begin() + end, predicate) -
        column.begin());
  };

  // Find the first row that is not `Undefined` (the `Undefined` values come
  // first, because their datatype bits are all zero). If there is no such row,
  // return the whole column as a single range (it is then already in the order
  // of `ORDER BY`).
  size_t firstDefined = partitionPoint(0, column.size(), Id::isUndefinedL);
  if (firstDefined == column.size()) {
    return std::vector<RowRange>{{0, column.size(), false}};
  }

  // Return `std::nullopt` unless the rows from `firstDefined` on are all ints
  // or all doubles (a mix of the two is not handled). Since the column is
  // grouped by datatype, it suffices to compare the datatypes of the first and
  // the last of these rows.
  Datatype type = column[firstDefined].getDatatype();
  if (column.back().getDatatype() != type ||
      (type != Datatype::Int && type != Datatype::Double)) {
    return std::nullopt;
  }

  // Output the `Undefined` values first, as `ORDER BY` does (the range is
  // empty if there are none).
  std::vector<RowRange> ranges;
  ranges.push_back({0, firstDefined, false});

  // For ints, output the negative ints first and then the non-negative ones (in
  // the column, the non-negative ints come first, and each of the two parts is
  // sorted by value).
  if (type == Datatype::Int) {
    size_t firstNegative = partitionPoint(
        firstDefined, column.size(), [](Id id) { return id.getInt() >= 0; });
    ranges.push_back({firstNegative, column.size(), false});
    ranges.push_back({firstDefined, firstNegative, false});
    return ranges;
  }

  // For doubles, find where the negative doubles begin (those with the sign bit
  // set, including `-0.0`, which come after the non-negative ones), and where
  // the `NaN`s at the end of each of the two parts begin.
  auto isNotNan = [](Id id) { return !std::isnan(id.getDouble()); };
  size_t firstNegative = partitionPoint(firstDefined, column.size(), [](Id id) {
    return !std::signbit(id.getDouble());
  });
  size_t firstPositiveNan =
      partitionPoint(firstDefined, firstNegative, isNotNan);
  size_t firstNegativeNan =
      partitionPoint(firstNegative, column.size(), isNotNan);

  // Output the negative doubles in reverse order (in the column, they are
  // sorted by descending value, because their magnitude increases with the
  // bits), then the non-negative doubles, and then all `NaN`s.
  //
  // NOTE: The SPARQL standard does not say where `NaN`s go. Putting them last
  // is how the regular comparator of `ORDER BY` does it (see
  // `OrderByComparator`).
  ranges.push_back({firstNegative, firstNegativeNan, true});
  ranges.push_back({firstDefined, firstPositiveNan, false});
  ranges.push_back({firstPositiveNan, firstNegative, false});
  ranges.push_back({firstNegativeNan, column.size(), false});
  return ranges;
}

// The "runs" of a sorted numeric column in the internal order of the `Id`s,
// numbered in that order (see `getRowRangesForSortedNumericColumn` above):
// for `Int`s, 0 = non-negative and 1 = negative; for `Double`s, 0 = `[0.0 ..
// +inf]`, 1 = positive `NaN`, 2 = `[-0.0 .. -inf]`, 3 = negative `NaN`. Return
// `std::nullopt` for all other datatypes.
std::optional<std::pair<Datatype, size_t>> getRun(Id id) {
  switch (id.getDatatype()) {
    case Datatype::Int:
      return std::pair{Datatype::Int, id.getInt() >= 0 ? 0 : 1};
    case Datatype::Double: {
      double value = id.getDouble();
      return std::pair{Datatype::Double,
                       2 * size_t{std::signbit(value)} + std::isnan(value)};
    }
    default:
      return std::nullopt;
  }
}

// A run (see `getRun`) and whether `ORDER BY` reads it backwards.
struct RunToTraverse {
  size_t run_;
  bool backwards_;
};

// The runs of the given datatype in the order of `ORDER BY`. Must match
// `getRowRangesForSortedNumericColumn`.
std::vector<RunToTraverse> getRunsInOrderByOrder(Datatype type,
                                                 bool isDescending) {
  AD_CORRECTNESS_CHECK(type == Datatype::Int || type == Datatype::Double);
  using Runs = std::vector<RunToTraverse>;
  auto runs = type == Datatype::Int
                  ? Runs{{1, false}, {0, false}}
                  : Runs{{2, true}, {0, false}, {1, false}, {3, false}};
  if (isDescending) {
    ql::ranges::reverse(runs);
    for (auto& run : runs) {
      run.backwards_ = !run.backwards_;
    }
  }
  return runs;
}

// Return the datatype of the scan if it is a single numeric one. The datatype
// bits are the most significant ones, so the first and last row suffice.
std::optional<Datatype> getSingleNumericDatatype(
    const IndexScan::BlocksOfSortedVariable& blocks) {
  auto firstRun = getRun(blocks.firstIdOfScan_);
  auto lastRun = getRun(blocks.lastIdOfScan_);
  if (!firstRun.has_value() || !lastRun.has_value() ||
      firstRun->first != lastRun->first) {
    return std::nullopt;
  }
  return firstRun->first;
}

// Return the run of each "pure" block, `std::nullopt` for the others. A block
// is pure if it lies completely inside the scan and inside a single run, so
// its position in the `ORDER BY` order is known from its metadata.
std::vector<std::optional<size_t>> getRunOfEachPureBlock(
    const IndexScan::BlocksOfSortedVariable& blocks, Datatype type) {
  std::vector<std::optional<size_t>> runOfEachPureBlock(blocks.blocks_.size());
  for (size_t i = 0; i < blocks.blocks_.size(); ++i) {
    const auto& block = blocks.blocks_[i];
    if (!block.completelyInsideScan_) {
      continue;
    }
    auto runOfFirst = getRun(block.first_);
    auto runOfLast = getRun(block.last_);
    // Only guards against inconsistent metadata.
    AD_CORRECTNESS_CHECK(runOfFirst.has_value() && runOfLast.has_value() &&
                         runOfFirst->first == type &&
                         runOfLast->first == type &&
                         runOfFirst->second <= runOfLast->second);
    if (runOfFirst->second == runOfLast->second) {
      runOfEachPureBlock[i] = runOfFirst->second;
    }
  }
  return runOfEachPureBlock;
}

// Return the pure blocks that `ORDER BY` reaches first, until they guarantee
// `numRows` rows, or `std::nullopt` if all of them together do not.
std::optional<std::vector<size_t>> getFirstPureBlocks(
    const IndexScan::BlocksOfSortedVariable& blocks,
    const std::vector<std::optional<size_t>>& runOfEachPureBlock, Datatype type,
    size_t numRows, bool isDescending) {
  std::vector<size_t> result;
  size_t numRowsGuaranteed = 0;
  for (auto [run, backwards] : getRunsInOrderByOrder(type, isDescending)) {
    std::vector<size_t> pureBlocksOfRun;
    for (size_t i = 0; i < runOfEachPureBlock.size(); ++i) {
      if (runOfEachPureBlock[i] == run) {
        pureBlocksOfRun.push_back(i);
      }
    }
    if (backwards) {
      ql::ranges::reverse(pureBlocksOfRun);
    }
    for (size_t i : pureBlocksOfRun) {
      result.push_back(i);
      numRowsGuaranteed += blocks.blocks_[i].numRowsLowerBound_;
      if (numRowsGuaranteed >= numRows) {
        return result;
      }
    }
  }
  return std::nullopt;
}

// Select the blocks of an `IndexScan` that can contain the first `numRows`
// rows of an `ORDER BY` on its sorted variable: the pure blocks that come first
// (see `getFirstPureBlocks`), and all others, whose position in the order is
// unknown. Return `std::nullopt` if the scan is not of a single numeric
// datatype, or if all blocks are needed.
std::optional<std::vector<size_t>> selectBlocksForSortedNumericLimit(
    const IndexScan::BlocksOfSortedVariable& blocks, size_t numRows,
    bool isDescending) {
  auto type = getSingleNumericDatatype(blocks);
  if (!type.has_value()) {
    return std::nullopt;
  }
  auto runOfEachPureBlock = getRunOfEachPureBlock(blocks, type.value());
  auto firstPureBlocks = getFirstPureBlocks(
      blocks, runOfEachPureBlock, type.value(), numRows, isDescending);
  if (!firstPureBlocks.has_value()) {
    return std::nullopt;
  }

  // Add the blocks that are not pure.
  auto selected = std::move(firstPureBlocks).value();
  for (size_t i = 0; i < runOfEachPureBlock.size(); ++i) {
    if (!runOfEachPureBlock[i].has_value()) {
      selected.push_back(i);
    }
  }
  if (selected.size() == blocks.blocks_.size()) {
    return std::nullopt;
  }
  ql::ranges::sort(selected);
  return selected;
}
}  // namespace

// _____________________________________________________________________________
std::optional<IdTable> OrderBy::computeResultForSortedInput(
    const IdTableView<0>& input) const {
  // Get the row ranges of the sort column in the order of `ORDER BY ASC`.
  // Return `std::nullopt` if the fast path does not apply (see
  // `hasSingleSortColumnWithSortedInput` and
  // `getRowRangesForSortedNumericColumn`).
  if (!hasSingleSortColumnWithSortedInput()) {
    return std::nullopt;
  }
  auto [column, isDescending] = sortIndices_.front();
  auto ranges = getRowRangesForSortedNumericColumn(input.getColumn(column));
  if (!ranges.has_value()) {
    return std::nullopt;
  }

  // For `ORDER BY DESC`, reverse the order of the ranges and the direction of
  // each range. This yields the reverse of the result for `ORDER BY ASC`,
  // without an extra pass over the data that reversing that result would need.
  if (isDescending) {
    ql::ranges::reverse(ranges.value());
    for (auto& range : ranges.value()) {
      range.reversed_ = !range.reversed_;
    }
  }

  // Copy the rows selected by the `LIMIT`/`OFFSET` (all rows if there is
  // none, see `handlesLimitOffset`) to the result, one range after the other,
  // column by column and each range in its direction.
  //
  // NOTE: `std::copy` and not `ql::ranges::copy`, because only the former
  // reliably becomes a `memmove` (see the description of #3436).
  const auto& limitOffset = getLimitOffset();
  size_t numRowsToSkip = limitOffset._offset;
  IdTable result{input.numColumns(), allocator()};
  result.resize(limitOffset.actualSize(input.numRows()));
  size_t offset = 0;
  for (const auto& [rangeBegin, rangeEnd, reversed] : ranges.value()) {
    // Stop when the result is complete, and skip a range that lies completely
    // before the `OFFSET`.
    if (offset == result.numRows()) {
      break;
    }
    size_t rangeSize = rangeEnd - rangeBegin;
    if (numRowsToSkip >= rangeSize) {
      numRowsToSkip -= rangeSize;
      continue;
    }

    // Determine the part of the range to copy, in the internal (ascending)
    // order (for a reversed range, the skipped rows are at its end).
    size_t numRows =
        std::min(result.numRows() - offset, rangeSize - numRowsToSkip);
    size_t begin = reversed ? rangeEnd - numRowsToSkip - numRows
                            : rangeBegin + numRowsToSkip;
    numRowsToSkip = 0;

    // Copy that part.
    for (size_t i = 0; i < input.numColumns(); ++i) {
      auto source = input.getColumn(i).subspan(begin, numRows);
      auto target = result.getColumn(i).begin() + offset;
      if (reversed) {
        ql::ranges::reverse_copy(source, target);
      } else {
        std::copy(source.begin(), source.end(), target);
      }
    }
    offset += numRows;
  }
  return result;
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
  if (numBlocksAfterAndBeforeLimit_.has_value()) {
    runtimeInfo().addDetail("num-blocks-read-for-limit",
                            numBlocksAfterAndBeforeLimit_->first);
    runtimeInfo().addDetail("num-blocks-total",
                            numBlocksAfterAndBeforeLimit_->second);
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
  // `selectBlocksForSortedNumericLimit`), so the following must not happen:
  // sorting a subset of the rows would give a wrong result.
  AD_CORRECTNESS_CHECK(!numBlocksAfterAndBeforeLimit_.has_value());

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
  std::optional<std::pair<size_t, size_t>> numBlocksAfterAndBefore;
  auto restrictedScan = scan->makeCopyWithSelectedBlocks(
      [numRows, isDescending, &numBlocksAfterAndBefore](
          const IndexScan::BlocksOfSortedVariable& blocks) {
        auto selected =
            selectBlocksForSortedNumericLimit(blocks, numRows, isDescending);
        if (selected.has_value()) {
          numBlocksAfterAndBefore.emplace(selected->size(),
                                          blocks.blocks_.size());
        }
        return selected;
      });
  if (restrictedScan.has_value()) {
    subtree_ = std::move(restrictedScan).value();
    numBlocksAfterAndBeforeLimit_ = numBlocksAfterAndBefore;
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
  result->numBlocksAfterAndBeforeLimit_ = numBlocksAfterAndBeforeLimit_;
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
