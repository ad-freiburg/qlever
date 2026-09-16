// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/OrderBy.h"

#include <cmath>
#include <limits>
#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
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
  // If the input is already sorted by the first sort column, the result can
  // often be computed in linear time, see `computeResultForSortedInput`.
  if (isInputSortedOnFirstSortColumn()) {
    return size + subcost;
  }
  size_t logSize =
      std::max(size_t(1), static_cast<size_t>(logb(static_cast<double>(size))));
  return size * logSize + subcost;
}

// _____________________________________________________________________________
bool OrderBy::isInputSortedOnFirstSortColumn() const {
  const auto& sortedOn = subtree_->resultSortedOn();
  return !sortedOn.empty() && sortedOn.front() == sortIndices_.front().first;
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
// puts all `NaN`s after all other doubles (see `makeComparatorForNans`) and
// `Undefined` before everything else.
std::optional<std::vector<RowRange>> getRowRangesForSortedNumericColumn(
    ql::span<const Id> column) {
  // Return the index of the first row at or after `firstDefined` for which the
  // `predicate` is false. The predicate must be monotone on the column.
  auto partitionPoint = [&column](size_t firstDefined, auto predicate) {
    return static_cast<size_t>(
        std::partition_point(column.begin() + firstDefined, column.end(),
                             predicate) -
        column.begin());
  };
  size_t firstDefined =
      partitionPoint(0, [](Id id) { return id.isUndefined(); });
  if (firstDefined == column.size()) {
    return std::nullopt;
  }
  Datatype type = column[firstDefined].getDatatype();
  if (column.back().getDatatype() != type) {
    return std::nullopt;
  }

  std::vector<RowRange> ranges;
  if (firstDefined > 0) {
    ranges.push_back({0, firstDefined, false});
  }
  if (type == Datatype::Int) {
    size_t firstNegative =
        partitionPoint(firstDefined, [](Id id) { return id.getInt() >= 0; });
    ranges.push_back({firstNegative, column.size(), false});
    ranges.push_back({firstDefined, firstNegative, false});
  } else if (type == Datatype::Double) {
    auto isNegative = [](Id id) { return std::signbit(id.getDouble()); };
    auto isNan = [](Id id) { return std::isnan(id.getDouble()); };
    size_t firstPositiveNan = partitionPoint(
        firstDefined, [&](Id id) { return !isNegative(id) && !isNan(id); });
    size_t firstNegative =
        partitionPoint(firstDefined, [&](Id id) { return !isNegative(id); });
    size_t firstNegativeNan = partitionPoint(
        firstDefined, [&](Id id) { return !(isNegative(id) && isNan(id)); });
    ranges.push_back({firstNegative, firstNegativeNan, true});
    ranges.push_back({firstDefined, firstPositiveNan, false});
    ranges.push_back({firstPositiveNan, firstNegative, false});
    ranges.push_back({firstNegativeNan, column.size(), false});
  } else {
    return std::nullopt;
  }
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

// The runs of a column with the given datatype in the order required by
// `ORDER BY`, each with the information whether its rows are traversed
// backwards. This must match the ranges of `getRowRangesForSortedNumericColumn`
// above.
std::vector<std::pair<size_t, bool>> getRunsInOrderByOrder(Datatype type,
                                                           bool isDescending) {
  AD_CORRECTNESS_CHECK(type == Datatype::Int || type == Datatype::Double);
  std::vector<std::pair<size_t, bool>> runs =
      type == Datatype::Int
          ? std::vector<std::pair<size_t, bool>>{{1, false}, {0, false}}
          : std::vector<std::pair<size_t, bool>>{
                {2, true}, {0, false}, {1, false}, {3, false}};
  if (isDescending) {
    ql::ranges::reverse(runs);
    for (auto& run : runs) {
      run.second = !run.second;
    }
  }
  return runs;
}

// For an `ORDER BY` on the sorted variable of an `IndexScan` whose first
// `numRows` rows (in the order given by `isDescending`) are needed: select the
// blocks that can contain these rows, if the scan result consists of a single
// numeric datatype according to the block metadata. Return `std::nullopt` if
// this is not the case, or if all blocks are needed anyway.
//
// The rows of the scan are sorted by the bits of the `Id`, so they form the
// runs described above, and `ORDER BY` traverses these runs in the order and
// direction given by `getRunsInOrderByOrder`. A block that lies completely
// inside one run is "pure", and the first `numRows` rows are found by taking
// pure blocks run by run from the end where the traversal starts, until they
// alone contain enough rows. All other blocks (those at the border of the
// relation, and those which straddle two runs) are always kept, so that a
// block with rows of a run is never lost. Since the datatype bits are the most
// significant bits of an `Id`, a single datatype in the first and the last row
// of the scan means a single datatype in all rows.
std::optional<std::vector<size_t>> selectBlocksForSortedNumericLimit(
    const IndexScan::BlocksOfSortedVariable& blocks, size_t numRows,
    bool isDescending) {
  auto firstRun = getRun(blocks.firstIdOfScan_);
  auto lastRun = getRun(blocks.lastIdOfScan_);
  if (!firstRun.has_value() || !lastRun.has_value() ||
      firstRun->first != lastRun->first) {
    return std::nullopt;
  }
  Datatype type = firstRun->first;
  // The run of each pure block, `nullopt` for all other blocks.
  std::vector<std::optional<size_t>> pureRun(blocks.blocks_.size());
  for (size_t i = 0; i < blocks.blocks_.size(); ++i) {
    const auto& block = blocks.blocks_[i];
    if (!block.completelyInsideScan_) {
      continue;
    }
    auto runOfFirst = getRun(block.first_);
    auto runOfLast = getRun(block.last_);
    // The datatype has been checked above via the bounds of the scan, this
    // only guards against inconsistent metadata.
    AD_CORRECTNESS_CHECK(runOfFirst.has_value() && runOfLast.has_value() &&
                         runOfFirst->first == type &&
                         runOfLast->first == type &&
                         runOfFirst->second <= runOfLast->second);
    if (runOfFirst->second == runOfLast->second) {
      pureRun[i] = runOfFirst->second;
    }
  }
  std::vector<bool> keep(blocks.blocks_.size(), false);
  for (size_t i = 0; i < keep.size(); ++i) {
    keep[i] = !pureRun[i].has_value();
  }
  size_t numRowsKept = 0;
  bool enough = false;
  for (auto [run, backwards] : getRunsInOrderByOrder(type, isDescending)) {
    std::vector<size_t> pureBlocksOfRun;
    for (size_t i = 0; i < pureRun.size(); ++i) {
      if (pureRun[i] == run) {
        pureBlocksOfRun.push_back(i);
      }
    }
    if (backwards) {
      ql::ranges::reverse(pureBlocksOfRun);
    }
    for (size_t i : pureBlocksOfRun) {
      keep[i] = true;
      numRowsKept += blocks.blocks_[i].numRowsLowerBound_;
      if (numRowsKept >= numRows) {
        enough = true;
        break;
      }
    }
    if (enough) {
      break;
    }
  }
  if (!enough) {
    return std::nullopt;
  }
  std::vector<size_t> selected;
  for (size_t i = 0; i < keep.size(); ++i) {
    if (keep[i]) {
      selected.push_back(i);
    }
  }
  if (selected.size() == keep.size()) {
    return std::nullopt;
  }
  return selected;
}
}  // namespace

// _____________________________________________________________________________
std::optional<IdTable> OrderBy::computeResultForSortedInput(
    const IdTableView<0>& input) const {
  if (sortIndices_.size() != 1 || !isInputSortedOnFirstSortColumn()) {
    return std::nullopt;
  }
  auto [column, isDescending] = sortIndices_.front();
  auto ranges = getRowRangesForSortedNumericColumn(input.getColumn(column));
  if (!ranges.has_value()) {
    return std::nullopt;
  }
  if (isDescending) {
    ql::ranges::reverse(ranges.value());
    for (auto& range : ranges.value()) {
      range.reversed_ = !range.reversed_;
    }
  }

  // Only copy the rows selected by the `LIMIT`/`OFFSET` (which is unconstrained
  // if there is none, see `handlesLimitOffset`).
  const auto& limitOffset = getLimitOffset();
  size_t numRowsToSkip = limitOffset._offset;
  size_t numRowsToCopy = limitOffset.limitOrDefault();
  IdTable result{input.numColumns(), allocator()};
  result.reserve(limitOffset.actualSize(input.numRows()));
  for (const auto& [rangeBegin, rangeEnd, reversed] : ranges.value()) {
    if (numRowsToCopy == 0) {
      break;
    }
    size_t rangeSize = rangeEnd - rangeBegin;
    if (numRowsToSkip >= rangeSize) {
      numRowsToSkip -= rangeSize;
      continue;
    }
    // The part of the range to copy, in the internal (ascending) order. For a
    // reversed range the skipped rows are at its end.
    size_t numRows = std::min(numRowsToCopy, rangeSize - numRowsToSkip);
    size_t begin = reversed ? rangeEnd - numRowsToSkip - numRows
                            : rangeBegin + numRowsToSkip;
    size_t end = begin + numRows;
    numRowsToSkip = 0;
    numRowsToCopy -= numRows;
    if (!reversed) {
      result.insertAtEnd(input, begin, end);
      continue;
    }
    size_t oldSize = result.numRows();
    result.resize(oldSize + numRows);
    for (size_t i = 0; i < input.numColumns(); ++i) {
      ql::ranges::reverse_copy(input.getColumn(i).subspan(begin, numRows),
                               result.getColumn(i).begin() + oldSize);
    }
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

  if (numBlocksAfterAndBeforeLimit_.has_value()) {
    runtimeInfo().addDetail("num-blocks-read-for-limit",
                            numBlocksAfterAndBeforeLimit_->first);
    runtimeInfo().addDetail("num-blocks-total",
                            numBlocksAfterAndBeforeLimit_->second);
  }
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

  // TODO<joka921> proper timeout for sorting operations
  getExecutionContext()->getSortPerformanceEstimator().throwIfEstimateTooLong(
      subTable.numRows(), subTable.numColumns(), deadline_,
      "Sort for COUNT(DISTINCT *)");

  AD_LOG_DEBUG << "OrderBy result computation..." << endl;
  IdTable idTable = subRes->cloneIdTable();

  size_t width = idTable.numColumns();

  // TODO<joka921> Measure (as soon as we have the benchmark merged)
  // whether it is beneficial to manually instantiate the comparison when
  // sorting by only one or two columns.

  // TODO<joka921> In the case of a single variable, it might be more efficient
  // to first sort by the ID values and then "repair" the resulting range by
  // some O(n) algorithms (see `computeResultForSortedInput`), or even by
  // returning lazy generators that yield the repaired order.

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
    IdTableUtils::sort<I>(&idTable, comparison);
  });
  // We can't check during sort, so reset status here
  cancellationHandle_->resetWatchDogState();
  checkCancellation();
  AD_LOG_DEBUG << "OrderBy result computation done." << endl;
  Result result{std::move(idTable), resultSortedOn(),
                subRes->getSharedLocalVocab()};
  // If this operation was promised to handle the `LIMIT`/`OFFSET` itself (see
  // `handlesLimitOffset`), but the fast path did not apply, apply it here.
  if (handlesLimitOffset() == LimitOffsetHandling::FULL) {
    result.applyLimitOffset(getLimitOffset(), [](auto, const auto&) {});
  }
  return result;
}

// _____________________________________________________________________________
LimitOffsetHandling OrderBy::handlesLimitOffset() const {
  return sortIndices_.size() == 1 && isInputSortedOnFirstSortColumn()
             ? LimitOffsetHandling::FULL
             : LimitOffsetHandling::NONE;
}

// _____________________________________________________________________________
void OrderBy::onLimitOffsetChanged([[maybe_unused]] const LimitOffsetClause&) {
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
      [&](const IndexScan::BlocksOfSortedVariable& blocks) {
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
  return std::make_unique<OrderBy>(_executionContext, subtree_->clone(),
                                   sortIndices_);
}
