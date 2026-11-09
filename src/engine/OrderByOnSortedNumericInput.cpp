// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/OrderByOnSortedNumericInput.h"

#include <array>

#include "engine/NumericRun.h"

namespace orderByOnSortedNumericInput {

using numericRun::toIndex;

namespace {

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
// the row ranges which, concatenated, yield the column in the semantic order
// of `ORDER BY ASC`: one range for the `Undefined` values, and one for each
// run (see `NumericRun`). Otherwise return `std::nullopt`.
//
// The column is grouped by datatype and each run is contiguous, so this only
// needs a few binary searches for the range boundaries.
std::optional<std::vector<RowRange>> getRowRangesInAscendingSemanticOrder(
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
  // or all doubles (a mix of the two is not handled).
  auto type =
      numericRun::getCommonNumericDatatype(column[firstDefined], column.back());
  if (!type.has_value()) {
    return std::nullopt;
  }

  // Find the rows of each run. The runs are in the internal order, so each one
  // begins where the previous one ends (a run may be empty).
  std::array<RowRange, numNumericRuns> rowsOfRun{};
  size_t begin = firstDefined;
  for (NumericRun run : numericRun::getRunsInInternalOrder(type.value())) {
    size_t end = partitionPoint(begin, column.size(), [run](Id id) {
      return numericRun::getRunOfId(id) == run;
    });
    rowsOfRun[toIndex(run)] = {begin, end, false};
    begin = end;
  }

  // Output the `Undefined` values first, as `ORDER BY` does (the range is
  // empty if there are none), then the runs in the semantic order.
  std::vector<RowRange> ranges{{0, firstDefined, false}};
  for (auto [run, reversed] :
       numericRun::getRunsInSemanticOrder(type.value(), false)) {
    RowRange range = rowsOfRun[toIndex(run)];
    range.reversed_ = reversed;
    ranges.push_back(range);
  }
  return ranges;
}

// Return the rows selected by the `limitOffset` from the concatenation of the
// `ranges` of the `input`, each range in its direction.
IdTable copyRowRangesWithLimitOffset(
    const IdTableView<0>& input, const std::vector<RowRange>& ranges,
    const LimitOffsetClause& limitOffset,
    const ad_utility::AllocatorWithLimit<Id>& allocator) {
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

    // Copy that part, column by column.
    //
    // NOTE: `std::copy` and not `ql::ranges::copy`, because only the former
    // reliably becomes a `memmove` (see the description of #3436).
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

// The blocks of a scan (by index), split by whether their position in the
// semantic order is known from their metadata.
struct BlocksSplitByRun {
  // For each run, the blocks that lie completely inside the scan and all of
  // whose rows belong to that run, in the order of the scan. In the internal
  // order, no row of such a block comes before a row of a previous block of the
  // same run. These are the only blocks that may be skipped.
  std::array<std::vector<size_t>, numNumericRuns> blocksWithinOneRun_;
  // All other blocks, which are always read: the ones at the border of the
  // scan (their first or last triple may belong to another relation), the ones
  // that span more than one run, and the ones whose first or last `Id` is not
  // of the datatype of the scan.
  //
  // NOTE: The latter happens with updates. The first and last triple of a
  // block are then only bounds, which also cover the deleted triples located
  // in the block, even nonexistent ones, and which are not narrowed when a
  // triple is deleted (see `LocatedTriplesPerBlock::updateAugmentedMetadata`).
  std::vector<size_t> blocksToAlwaysRead_;
};

// Split the `blocks` of a scan of the given numeric datatype by run.
BlocksSplitByRun splitBlocksByRun(
    const IndexScan::BlocksOfSortedVariable& blocks, Datatype type) {
  BlocksSplitByRun result;
  for (size_t i = 0; i < blocks.blocks_.size(); ++i) {
    const auto& block = blocks.blocks_[i];
    if (!block.completelyInsideScan_) {
      result.blocksToAlwaysRead_.push_back(i);
      continue;
    }
    auto runOfFirst = numericRun::getRunOfId(block.first_);
    auto runOfLast = numericRun::getRunOfId(block.last_);
    bool isWithinOneRunOfType =
        runOfFirst.has_value() && runOfFirst == runOfLast &&
        numericRun::getDatatype(runOfFirst.value()) == type;
    if (isWithinOneRunOfType) {
      result.blocksWithinOneRun_[toIndex(runOfFirst.value())].push_back(i);
    } else {
      result.blocksToAlwaysRead_.push_back(i);
    }
  }
  return result;
}

// Among the blocks that lie within one run (see `BlocksSplitByRun`), return
// the ones that hold the first `numRows` rows in the semantic order: go through
// the runs, and through the blocks of each run, in the semantic order, and
// collect blocks until they are guaranteed to hold `numRows` rows (see
// `numRowsLowerBound_`). No row of a block that is not collected comes before a
// row of a collected block in that order. If the blocks within one run are not
// guaranteed to hold `numRows` rows altogether, return all of them.
std::vector<size_t> getBlocksWithinOneRunHoldingTheFirstRows(
    const IndexScan::BlocksOfSortedVariable& blocks,
    const BlocksSplitByRun& blocksSplitByRun, Datatype type, size_t numRows,
    bool isDescending) {
  std::vector<size_t> result;
  size_t numRowsGuaranteed = 0;
  for (auto [run, reversed] :
       numericRun::getRunsInSemanticOrder(type, isDescending)) {
    auto blocksOfRun = blocksSplitByRun.blocksWithinOneRun_[toIndex(run)];
    if (reversed) {
      ql::ranges::reverse(blocksOfRun);
    }
    for (size_t i : blocksOfRun) {
      if (numRowsGuaranteed >= numRows) {
        return result;
      }
      result.push_back(i);
      numRowsGuaranteed += blocks.blocks_[i].numRowsLowerBound_;
    }
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
std::optional<IdTable> computeOrderBy(
    const IdTableView<0>& input, ColumnIndex sortColumn, bool isDescending,
    const LimitOffsetClause& limitOffset,
    const ad_utility::AllocatorWithLimit<Id>& allocator) {
  auto ranges =
      getRowRangesInAscendingSemanticOrder(input.getColumn(sortColumn));
  if (!ranges.has_value()) {
    return std::nullopt;
  }
  // This yields the reverse of the result for `ORDER BY ASC`, without the extra
  // pass over the data that reversing that result would need.
  if (isDescending) {
    numericRun::reverseOrderAndDirection(ranges.value());
  }
  return copyRowRangesWithLimitOffset(input, ranges.value(), limitOffset,
                                      allocator);
}

// _____________________________________________________________________________
std::optional<std::vector<size_t>> selectBlocksForLimit(
    const IndexScan::BlocksOfSortedVariable& blocks, size_t numRows,
    bool isDescending) {
  // Only if the scan contains only `Int`s or only `Double`s is the fast path
  // guaranteed to apply to its result.
  auto type = numericRun::getCommonNumericDatatype(blocks.firstIdOfScan_,
                                                   blocks.lastIdOfScan_);
  if (!type.has_value()) {
    return std::nullopt;
  }

  // Read the blocks within one run that hold the first rows (see
  // `getBlocksWithinOneRunHoldingTheFirstRows`), and all blocks whose position
  // in the semantic order is unknown. All remaining blocks within one run only
  // hold rows that come after `numRows` rows of the read blocks.
  //
  // NOTE: The blocks need not be split at the boundaries of the runs, because a
  // block that spans a boundary is always read. The scan is sorted, so there
  // is at most one such block per boundary (one boundary for `Int`s, three for
  // `Double`s), and only the first and the last block of the scan can stick
  // out of it. So without updates, at most five blocks are read that might be
  // skipped otherwise (with updates, see `BlocksSplitByRun`).
  auto blocksSplitByRun = splitBlocksByRun(blocks, type.value());
  auto selected = getBlocksWithinOneRunHoldingTheFirstRows(
      blocks, blocksSplitByRun, type.value(), numRows, isDescending);
  selected.insert(selected.end(), blocksSplitByRun.blocksToAlwaysRead_.begin(),
                  blocksSplitByRun.blocksToAlwaysRead_.end());
  if (selected.size() == blocks.blocks_.size()) {
    return std::nullopt;
  }
  ql::ranges::sort(selected);
  return selected;
}

}  // namespace orderByOnSortedNumericInput
