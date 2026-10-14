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

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/CompressedIdTableBlocks.h"

namespace {

using ad_utility::CompressedBlockFile;
using ad_utility::compressedIdTable::BlockMetadata;
using ad_utility::compressedIdTable::readBlock;
using ad_utility::compressedIdTable::writeBlock;

// A row of a table, as plain integers, so that whole tables can be compared
// conveniently.
using Row = std::vector<int64_t>;

// The row that is identified by `value` and that has `numColumns` columns. The
// value of a column depends on its index, so that a permutation of the columns
// would be noticed.
Row makeRow(int64_t value, size_t numColumns) {
  Row row;
  for (size_t columnIdx : ql::views::iota(size_t{0}, numColumns)) {
    row.push_back(value + static_cast<int64_t>(columnIdx) * 1000);
  }
  return row;
}

// The rows that are identified by `values`, with `numColumns` columns each.
std::vector<Row> makeRows(size_t numColumns,
                          const std::vector<int64_t>& values) {
  std::vector<Row> rows;
  for (int64_t value : values) {
    rows.push_back(makeRow(value, numColumns));
  }
  return rows;
}

// Create a table with `numColumns` columns that holds the rows which are
// identified by `values`, see `makeRow`.
template <size_t NumCols = 0>
IdTableStatic<NumCols> makeTable(size_t numColumns,
                                 const std::vector<int64_t>& values) {
  IdTableStatic<NumCols> table{numColumns,
                               ad_utility::testing::makeAllocator()};
  for (int64_t value : values) {
    table.emplace_back();
    Row row = makeRow(value, numColumns);
    for (size_t columnIdx : ql::views::iota(size_t{0}, numColumns)) {
      table(table.numRows() - 1, columnIdx) = Id::makeFromInt(row[columnIdx]);
    }
  }
  return table;
}

// The rows of `table`, see `Row`.
template <typename Table>
std::vector<Row> tableRows(const Table& table) {
  std::vector<Row> rows;
  for (size_t rowIdx : ql::views::iota(size_t{0}, table.numRows())) {
    Row row;
    for (size_t columnIdx : ql::views::iota(size_t{0}, table.numColumns())) {
      row.push_back(table(rowIdx, columnIdx).getInt());
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

// The compression levels that the round trips below are run with: the default
// ZSTD level, and no compression at all. A block has to arrive unchanged either
// way, see `CompressedBlockFile::CompressionLevel`.
const std::vector<CompressedBlockFile::CompressionLevel>& compressionLevels() {
  static const std::vector<CompressedBlockFile::CompressionLevel> result{
      ad_utility::ZSTD_DEFAULT_LEVEL, ad_utility::NO_BLOCK_COMPRESSION};
  return result;
}

// Write the rows `[beginRow, endRow)` of `table` to `file`, read them back, and
// return the metadata of the block together with the rows that came back.
template <size_t NumCols = 0, typename Table>
std::pair<BlockMetadata, std::vector<Row>> roundTrip(CompressedBlockFile& file,
                                                     const Table& table,
                                                     size_t beginRow,
                                                     size_t endRow) {
  BlockMetadata metadata = writeBlock(file, table, beginRow, endRow);
  IdTableStatic<NumCols> block =
      readBlock<NumCols>(file, metadata, ad_utility::testing::makeAllocator());
  return {std::move(metadata), tableRows(block)};
}

// The values that identify the rows of the tables below. They are more than a
// single compressed frame's worth of trivially compressible data, so that the
// compressed and the uncompressed path really differ in size.
std::vector<int64_t> manyValues() {
  std::vector<int64_t> values;
  for (size_t i : ql::views::iota(size_t{0}, size_t{5000})) {
    values.push_back(static_cast<int64_t>(i));
  }
  return values;
}

}  // namespace

// _____________________________________________________________________________
TEST(CompressedIdTableBlocks, roundTripWithDynamicNumberOfColumns) {
  for (const auto& compression : compressionLevels()) {
    SCOPED_TRACE(compression.has_value() ? "compressed" : "uncompressed");
    // The number of columns deliberately includes the degenerate case of a
    // single column.
    for (size_t numColumns : {size_t{1}, size_t{3}, size_t{7}}) {
      SCOPED_TRACE(numColumns);
      CompressedBlockFile file{gtestCurrentTestName(), compression};
      auto table = makeTable(numColumns, manyValues());
      auto [metadata, rows] = roundTrip(file, table, 0, table.numRows());
      EXPECT_EQ(metadata.numRows_, table.numRows());
      EXPECT_EQ(metadata.numColumns(), numColumns);
      EXPECT_EQ(rows, makeRows(numColumns, manyValues()));
    }
  }
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlocks, roundTripWithStaticNumberOfColumns) {
  static constexpr size_t numColumns = 3;
  for (const auto& compression : compressionLevels()) {
    SCOPED_TRACE(compression.has_value() ? "compressed" : "uncompressed");
    CompressedBlockFile file{gtestCurrentTestName(), compression};
    auto table = makeTable<numColumns>(numColumns, manyValues());
    auto [metadata, rows] =
        roundTrip<numColumns>(file, table, 0, table.numRows());
    EXPECT_EQ(metadata.numColumns(), numColumns);
    EXPECT_EQ(rows, makeRows(numColumns, manyValues()));
  }
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlocks, onlyTheRequestedRowsAreWritten) {
  CompressedBlockFile file{gtestCurrentTestName()};
  std::vector<int64_t> values{0, 1, 2, 3, 4, 5, 6, 7};
  auto table = makeTable(2, values);

  // A proper subrange in the middle of the table.
  auto [metadata, rows] = roundTrip(file, table, 2, 5);
  EXPECT_EQ(metadata.numRows_, 3u);
  EXPECT_EQ(rows, makeRows(2, {2, 3, 4}));

  // A suffix of the table, and a single row.
  EXPECT_EQ(roundTrip(file, table, 6, table.numRows()).second,
            makeRows(2, {6, 7}));
  EXPECT_EQ(roundTrip(file, table, 0, 1).second, makeRows(2, {0}));

  // An empty range, both in the middle and at the very end of the table.
  auto [emptyMetadata, emptyRows] = roundTrip(file, table, 4, 4);
  EXPECT_EQ(emptyMetadata.numRows_, 0u);
  EXPECT_EQ(emptyMetadata.numColumns(), 2u);
  EXPECT_THAT(emptyRows, ::testing::IsEmpty());
  EXPECT_THAT(roundTrip(file, table, table.numRows(), table.numRows()).second,
              ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlocks, blocksOfSeveralTablesAreIndependent) {
  CompressedBlockFile file{gtestCurrentTestName()};
  auto first = makeTable(2, {0, 1, 2, 3});
  auto second = makeTable(5, {100, 101});

  // Write the blocks of the two tables interleaved. Nothing but the metadata of
  // a block is needed to read it back, so blocks with a different number of
  // columns may freely share a file.
  std::vector<BlockMetadata> metadata;
  metadata.push_back(writeBlock(file, first, 0, 2));
  metadata.push_back(writeBlock(file, second, 0, 2));
  metadata.push_back(writeBlock(file, first, 2, 4));

  // Read the blocks back in reverse order, to make sure that reading doesn't
  // depend on the order in which the blocks were written.
  std::vector<std::vector<Row>> rows;
  for (size_t i : ql::views::iota(size_t{0}, metadata.size())) {
    size_t idx = metadata.size() - 1 - i;
    rows.push_back(tableRows(readBlock(file, metadata.at(idx),
                                       ad_utility::testing::makeAllocator())));
  }
  EXPECT_EQ(rows.at(0), makeRows(2, {2, 3}));
  EXPECT_EQ(rows.at(1), makeRows(5, {100, 101}));
  EXPECT_EQ(rows.at(2), makeRows(2, {0, 1}));
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlocks, aBlockWithoutColumnsKeepsItsNumberOfRows) {
  CompressedBlockFile file{gtestCurrentTestName()};
  IdTableStatic<0> table{0, ad_utility::testing::makeAllocator()};
  table.resize(7);

  auto [metadata, rows] = roundTrip(file, table, 1, 6);
  EXPECT_EQ(metadata.numColumns(), 0u);
  EXPECT_EQ(metadata.numRows_, 5u);
  // The rows are all empty, but there have to be exactly five of them.
  EXPECT_EQ(rows, std::vector<Row>(5));
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlocks, theMetadataDescribesTheBlock) {
  for (const auto& compression : compressionLevels()) {
    SCOPED_TRACE(compression.has_value() ? "compressed" : "uncompressed");
    CompressedBlockFile file{gtestCurrentTestName(), compression};
    auto table = makeTable(3, manyValues());
    BlockMetadata metadata = writeBlock(file, table, 0, table.numRows());

    // There is exactly one byte range per column, and each of them holds the
    // `Id`s of one column of the block.
    ASSERT_EQ(metadata.numColumns(), 3u);
    size_t expectedOffset = 0;
    for (const auto& column : metadata.columns_) {
      EXPECT_EQ(column.uncompressedSize_, metadata.numRows_ * sizeof(Id));
      // The columns are stored one after the other, without gaps or overlaps.
      EXPECT_EQ(column.offsetInFile_, expectedOffset);
      expectedOffset += column.compressedSize_;
      if (compression.has_value()) {
        // The values of a column are consecutive integers, so they compress.
        EXPECT_LT(column.compressedSize_, column.uncompressedSize_);
      } else {
        EXPECT_EQ(column.compressedSize_, column.uncompressedSize_);
      }
    }
  }
}

// _____________________________________________________________________________
TEST(CompressedIdTableBlocks, anInvalidRowRangeThrows) {
  CompressedBlockFile file{gtestCurrentTestName()};
  auto table = makeTable(2, {0, 1, 2});
  AD_EXPECT_THROW_WITH_MESSAGE(
      writeBlock(file, table, 2, 1),
      ::testing::HasSubstr("beginRow <= endRow && endRow <= table.numRows()"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      writeBlock(file, table, 0, 4),
      ::testing::HasSubstr("beginRow <= endRow && endRow <= table.numRows()"));
}
