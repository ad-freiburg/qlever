// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@cs.uni-freiburg.de)

#include "engine/OrderByOnSortedNumericInput.h"

#include <array>
#include <cmath>

namespace orderByOnSortedNumericInput {

namespace {
constexpr size_t numRuns =
    static_cast<size_t>(NumericRun::DoubleNegativeNan) + 1;

size_t toIndex(NumericRun run) { return static_cast<size_t>(run); }

// Return the datatype of the values in the `run`.
Datatype getDatatype(NumericRun run) {
  return run <= NumericRun::IntNegative ? Datatype::Int : Datatype::Double;
}

// Reverse the order of the `elements` and the direction (`reversed_`) of each
// of them. This turns the output order for `ORDER BY ASC` into the one for
// `ORDER BY DESC`.
template <typename T>
void reverseOrderAndDirection(std::vector<T>& elements) {
  ql::ranges::reverse(elements);
  for (auto& element : elements) {
    element.reversed_ = !element.reversed_;
  }
}

// Return the runs of the given datatype in the internal order.
std::vector<NumericRun> getRunsInInternalOrder(Datatype type) {
  if (type == Datatype::Int) {
    return {NumericRun::IntNonNegative, NumericRun::IntNegative};
  }
  AD_CORRECTNESS_CHECK(type == Datatype::Double);
  return {NumericRun::DoubleNonNegative, NumericRun::DoublePositiveNan,
          NumericRun::DoubleNegative, NumericRun::DoubleNegativeNan};
}

// A run, and whether `ORDER BY` traverses it in reverse internal order.
struct RunToOutput {
  NumericRun run_;
  bool reversed_;
};

// Return the runs of the given datatype in the order of `ORDER BY`: the
// negative values first, then the non-negative ones, then the `NaN`s (all
// reversed for `DESC`).
std::vector<RunToOutput> getRunsInOrderByOrder(Datatype type,
                                               bool isDescending) {
  std::vector<RunToOutput> runs;
  if (type == Datatype::Int) {
    runs = {{NumericRun::IntNegative, false},
            {NumericRun::IntNonNegative, false}};
  } else {
    AD_CORRECTNESS_CHECK(type == Datatype::Double);
    runs = {{NumericRun::DoubleNegative, true},
            {NumericRun::DoubleNonNegative, false},
            {NumericRun::DoublePositiveNan, false},
            {NumericRun::DoubleNegativeNan, false}};
  }
  if (isDescending) {
    reverseOrderAndDirection(runs);
  }
  return runs;
}

// SECTION: Computing the result from a sorted input.

// A contiguous range `[begin_, end_)` of rows of an `IdTable`. If `reversed_`
// is true, the rows are to be output in reverse order.
struct RowRange {
  size_t begin_;
  size_t end_;
  bool reversed_;
};

// If the `column`, which must be sorted in the internal order, contains only
// `Int`s or only `Double`s, possibly preceded by `Undefined` values, return
// the row ranges which, concatenated, yield the column in the ascending order
// of `ORDER BY`. Otherwise return `std::nullopt`.
//
// The datatype bits are the most significant bits of an `Id`, so a column that
// is sorted by bits is grouped by datatype, and the check is O(1) apart from
// the binary searches for the range boundaries.
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

  // Find the rows of each run. The runs are in the internal order, so each one
  // begins where the previous one ends (a run may be empty).
  std::array<RowRange, numRuns> rowsOfRun{};
  size_t begin = firstDefined;
  for (NumericRun run : getRunsInInternalOrder(type)) {
    size_t end = partitionPoint(begin, column.size(), [run](Id id) {
      return getRunIfNumeric(id) == run;
    });
    rowsOfRun[toIndex(run)] = {begin, end, false};
    begin = end;
  }

  // Output the `Undefined` values first, as `ORDER BY` does (the range is
  // empty if there are none), then the runs in the order of `ORDER BY`.
  std::vector<RowRange> ranges{{0, firstDefined, false}};
  for (auto [run, reversed] : getRunsInOrderByOrder(type, false)) {
    RowRange range = rowsOfRun[toIndex(run)];
    range.reversed_ = reversed;
    ranges.push_back(range);
  }
  return ranges;
}

// Return the rows selected by the `limitOffset` from the concatenation of the
// `ranges` of the `input`, each range in its direction.
IdTable copyRowRanges(const IdTableView<0>& input,
                      const std::vector<RowRange>& ranges,
                      const LimitOffsetClause& limitOffset,
                      const ad_utility::AllocatorWithLimit<Id>& allocator) {
  // NOTE: `std::copy` and not `ql::ranges::copy`, because only the former
  // reliably becomes a `memmove` (see the description of #3436).
  size_t numRowsToSkip = limitOffset._offset;
  IdTable result{input.numColumns(), allocator};
  result.resize(limitOffset.actualSize(input.numRows()));
  size_t offset = 0;
  for (const auto& [rangeBegin, rangeEnd, reversed] : ranges) {
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

// SECTION: Selecting the blocks of an `IndexScan` for a `LIMIT`.

// Return the datatype of the sorted variable of the scan if it is `Int` in all
// rows or `Double` in all rows. The datatype bits are the most significant
// ones, so the first and the last row suffice.
std::optional<Datatype> getNumericDatatypeOfScan(
    const IndexScan::BlocksOfSortedVariable& blocks) {
  auto runOfFirst = getRunIfNumeric(blocks.firstIdOfScan_);
  auto runOfLast = getRunIfNumeric(blocks.lastIdOfScan_);
  if (!runOfFirst.has_value() || !runOfLast.has_value() ||
      getDatatype(runOfFirst.value()) != getDatatype(runOfLast.value())) {
    return std::nullopt;
  }
  return getDatatype(runOfFirst.value());
}

// The blocks of a scan (by index), grouped by whether their position in the
// order of `ORDER BY` is known from their metadata.
struct BlocksByRun {
  // For each run, the blocks that lie completely inside the scan and inside
  // that run, in the order of the scan. None of the rows of such a block comes
  // before a row of a previous block of the same run in the internal order.
  std::array<std::vector<size_t>, numRuns> blocksInsideRun_;
  // All other blocks: the ones at the border of the scan, whose first or last
  // triple may belong to another relation, and the ones that span more than
  // one run.
  std::vector<size_t> otherBlocks_;
};

// Group the `blocks` of a scan of the given numeric datatype by run.
BlocksByRun groupBlocksByRun(const IndexScan::BlocksOfSortedVariable& blocks,
                             Datatype type) {
  BlocksByRun result;
  for (size_t i = 0; i < blocks.blocks_.size(); ++i) {
    const auto& block = blocks.blocks_[i];
    if (!block.completelyInsideScan_) {
      result.otherBlocks_.push_back(i);
      continue;
    }
    auto runOfFirst = getRunIfNumeric(block.first_);
    auto runOfLast = getRunIfNumeric(block.last_);
    // Only guards against inconsistent metadata.
    AD_CORRECTNESS_CHECK(runOfFirst.has_value() && runOfLast.has_value() &&
                         getDatatype(runOfFirst.value()) == type &&
                         getDatatype(runOfLast.value()) == type &&
                         runOfFirst.value() <= runOfLast.value());
    if (runOfFirst == runOfLast) {
      result.blocksInsideRun_[toIndex(runOfFirst.value())].push_back(i);
    } else {
      result.otherBlocks_.push_back(i);
    }
  }
  return result;
}

// Among the blocks that lie inside a run (see `BlocksByRun`), return the ones
// that hold the first `numRows` rows in the order of `ORDER BY`: go through the
// runs, and through the blocks of each run, in the order of `ORDER BY`, and
// collect blocks until they are guaranteed to hold `numRows` rows (see
// `numRowsLowerBound_`). No row of a block that is not collected comes before
// a row of a collected block in that order. If the blocks inside runs are not
// guaranteed to hold `numRows` rows altogether, return all of them.
std::vector<size_t> getBlocksWithFirstRows(
    const IndexScan::BlocksOfSortedVariable& blocks,
    const BlocksByRun& blocksByRun, Datatype type, size_t numRows,
    bool isDescending) {
  std::vector<size_t> result;
  size_t numRowsGuaranteed = 0;
  for (auto [run, reversed] : getRunsInOrderByOrder(type, isDescending)) {
    auto blocksOfRun = blocksByRun.blocksInsideRun_[toIndex(run)];
    if (reversed) {
      ql::ranges::reverse(blocksOfRun);
    }
    for (size_t i : blocksOfRun) {
      result.push_back(i);
      numRowsGuaranteed += blocks.blocks_[i].numRowsLowerBound_;
      if (numRowsGuaranteed >= numRows) {
        return result;
      }
    }
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
std::optional<NumericRun> getRunIfNumeric(Id id) {
  switch (id.getDatatype()) {
    case Datatype::Int:
      return id.getInt() >= 0 ? NumericRun::IntNonNegative
                              : NumericRun::IntNegative;
    case Datatype::Double: {
      double value = id.getDouble();
      if (std::signbit(value)) {
        return std::isnan(value) ? NumericRun::DoubleNegativeNan
                                 : NumericRun::DoubleNegative;
      }
      return std::isnan(value) ? NumericRun::DoublePositiveNan
                               : NumericRun::DoubleNonNegative;
    }
    default:
      return std::nullopt;
  }
}

// _____________________________________________________________________________
std::optional<IdTable> computeOrderBy(
    const IdTableView<0>& input, ColumnIndex sortColumn, bool isDescending,
    const LimitOffsetClause& limitOffset,
    const ad_utility::AllocatorWithLimit<Id>& allocator) {
  auto ranges = getRowRangesForSortedNumericColumn(input.getColumn(sortColumn));
  if (!ranges.has_value()) {
    return std::nullopt;
  }
  // This yields the reverse of the result for `ORDER BY ASC`, without the extra
  // pass over the data that reversing that result would need.
  if (isDescending) {
    reverseOrderAndDirection(ranges.value());
  }
  return copyRowRanges(input, ranges.value(), limitOffset, allocator);
}

// _____________________________________________________________________________
std::optional<std::vector<size_t>> selectBlocksForLimit(
    const IndexScan::BlocksOfSortedVariable& blocks, size_t numRows,
    bool isDescending) {
  // Only then is the fast path guaranteed to apply to the result of the scan.
  auto type = getNumericDatatypeOfScan(blocks);
  if (!type.has_value()) {
    return std::nullopt;
  }

  // Read the blocks that hold the first rows among the blocks inside a run
  // (see `getBlocksWithFirstRows`), and all other blocks, whose position in
  // the order of `ORDER BY` is unknown. All remaining blocks inside a run only
  // hold rows that come after `numRows` rows of the read blocks.
  //
  // NOTE: The blocks need not be split at the boundaries of the runs, because a
  // block that spans a boundary is always read. The scan is sorted, so there
  // is at most one such block per boundary (one boundary for `Int`s, three for
  // `Double`s), and only the first and the last block of the scan can stick
  // out of it. So at most five blocks are read that might be skipped
  // otherwise.
  auto blocksByRun = groupBlocksByRun(blocks, type.value());
  auto selected = getBlocksWithFirstRows(blocks, blocksByRun, type.value(),
                                         numRows, isDescending);
  selected.insert(selected.end(), blocksByRun.otherBlocks_.begin(),
                  blocksByRun.otherBlocks_.end());
  if (selected.size() == blocks.blocks_.size()) {
    return std::nullopt;
  }
  ql::ranges::sort(selected);
  return selected;
}

}  // namespace orderByOnSortedNumericInput
