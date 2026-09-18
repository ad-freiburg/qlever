// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/IdTableHelpers.h"
#include "engine/idTable/RowMajorIdTable.h"
#include "engine/idTable/RowMajorMergeBlock.h"

namespace {

using ad_utility::RowMajorIdTable;
using ad_utility::RowMajorMergeBlock;
using ad_utility::testing::makeAllocator;

// The `Id` that encodes the given `row` and `column`, such that a
// transposition that mixes up the two is immediately visible.
Id makeId(size_t row, size_t column) {
  return Id::makeFromInt(static_cast<int64_t>(1000 * row + column));
}

// A column-major table with `numRows` rows and `NumCols` columns, filled via
// `makeId`.
template <size_t NumCols>
IdTableStatic<NumCols> makeColumnMajorTable(size_t numRows) {
  IdTableStatic<NumCols> table{NumCols, makeAllocator()};
  table.resize(numRows);
  for (size_t row = 0; row < numRows; ++row) {
    for (size_t column = 0; column < NumCols; ++column) {
      table(row, column) = makeId(row, column);
    }
  }
  return table;
}

// Check that the `table` has exactly the contents that
// `makeColumnMajorTable<NumCols>(numRows)` has.
template <size_t NumCols, typename Table>
void checkContents(const Table& table, size_t numRows) {
  ASSERT_EQ(table.numRows(), numRows);
  for (size_t row = 0; row < numRows; ++row) {
    for (size_t column = 0; column < NumCols; ++column) {
      EXPECT_EQ(table[row][column], makeId(row, column))
          << "row " << row << ", column " << column;
    }
  }
}

// The row counts that the tests below use. They are chosen around the size of a
// single tile of the transposition (see
// `rowMajorIdTable::transpositionTileNumRows`), such that the cases of a single
// partial tile, of several complete tiles, and of a partial tile at the end are
// all covered.
template <size_t NumCols>
std::vector<size_t> testNumRows() {
  constexpr size_t tile =
      ad_utility::rowMajorIdTable::transpositionTileNumRows<NumCols>();
  return {0, 1, 2, tile - 1, tile, tile + 1, 3 * tile, 3 * tile + 7};
}

// _____________________________________________________________________________
template <size_t NumCols>
void testTranspositionRoundTrip() {
  for (size_t numRows : testNumRows<NumCols>()) {
    auto columnMajor = makeColumnMajorTable<NumCols>(numRows);
    RowMajorIdTable<NumCols> rowMajor{makeAllocator()};
    rowMajor.appendTransposed(columnMajor, 0, numRows);
    checkContents<NumCols>(rowMajor, numRows);
    auto backAgain = rowMajor.toColumnMajor(makeAllocator());
    checkContents<NumCols>(backAgain, numRows);
  }
}

// _____________________________________________________________________________
TEST(RowMajorIdTable, transpositionRoundTrip) {
  testTranspositionRoundTrip<1>();
  testTranspositionRoundTrip<2>();
  testTranspositionRoundTrip<4>();
  testTranspositionRoundTrip<7>();
}

// _____________________________________________________________________________
TEST(RowMajorIdTable, appendPartialRanges) {
  constexpr size_t numCols = 3;
  constexpr size_t numRows = 100;
  auto columnMajor = makeColumnMajorTable<numCols>(numRows);
  // Append the table in three pieces, which has to yield exactly the same
  // result as appending it in one go.
  RowMajorIdTable<numCols> rowMajor{makeAllocator()};
  rowMajor.appendTransposed(columnMajor, 0, 17);
  rowMajor.appendTransposed(columnMajor, 17, 17);
  rowMajor.appendTransposed(columnMajor, 17, numRows);
  checkContents<numCols>(rowMajor, numRows);

  // The same for the way back.
  IdTableStatic<numCols> target{numCols, makeAllocator()};
  rowMajor.appendToColumnMajor(target, 0, 42);
  rowMajor.appendToColumnMajor(target, 42, numRows);
  checkContents<numCols>(target, numRows);
}

// _____________________________________________________________________________
TEST(RowMajorIdTable, pushBackAndIteration) {
  constexpr size_t numCols = 2;
  auto columnMajor = makeColumnMajorTable<numCols>(5);
  RowMajorIdTable<numCols> rowMajor{makeAllocator()};
  for (const auto& row : columnMajor) {
    rowMajor.push_back(row);
  }
  checkContents<numCols>(rowMajor, 5);
  EXPECT_EQ(rowMajor.numColumns(), numCols);
  EXPECT_FALSE(rowMajor.empty());
  // The rows are dense `std::array`s, so iterating over them is ordinary
  // contiguous iteration.
  EXPECT_EQ(static_cast<size_t>(rowMajor.end() - rowMajor.begin()), 5u);
  EXPECT_EQ(*rowMajor.begin(),
            (std::array<Id, numCols>{makeId(0, 0), makeId(0, 1)}));
  rowMajor.clear();
  EXPECT_TRUE(rowMajor.empty());
}

// _____________________________________________________________________________
TEST(RowMajorIdTable, toDynamicRow) {
  std::array<Id, 3> row{makeId(4, 0), makeId(4, 1), makeId(4, 2)};
  auto dynamicRow = ad_utility::rowMajorIdTable::toDynamicRow<3>(row);
  ASSERT_EQ(dynamicRow.size(), 3u);
  for (size_t column = 0; column < 3; ++column) {
    EXPECT_EQ(dynamicRow[column], makeId(4, column));
  }
}

// _____________________________________________________________________________
TEST(RowMajorMergeBlock, rowMajorAndColumnMajorState) {
  constexpr size_t numCols = 4;
  constexpr size_t numRows = 20;
  auto columnMajor = makeColumnMajorTable<numCols>(numRows);

  // A block that is built row-major.
  RowMajorMergeBlock<numCols> block{makeAllocator()};
  EXPECT_TRUE(block.isRowMajor());
  for (const auto& row : columnMajor) {
    block.push_back(row);
  }
  EXPECT_EQ(block.size(), numRows);
  checkContents<numCols>(block, numRows);
  auto transposed = std::move(block).toColumnMajor(makeAllocator());
  checkContents<numCols>(transposed, numRows);

  // A block that was read back from a spill file is already column-major, so
  // `toColumnMajor` hands out its table without touching it.
  RowMajorMergeBlock<numCols> spilled{makeColumnMajorTable<numCols>(numRows)};
  EXPECT_FALSE(spilled.isRowMajor());
  EXPECT_EQ(spilled.size(), numRows);
  checkContents<numCols>(std::move(spilled).toColumnMajor(makeAllocator()),
                         numRows);
}

}  // namespace
