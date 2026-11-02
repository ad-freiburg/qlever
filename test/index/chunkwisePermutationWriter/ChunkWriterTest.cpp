// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "global/Constants.h"
#include "index/CompressedRelationReader.h"
#include "index/LocatedTriples.h"
#include "index/chunkwisePermutationWriter/ChunkWriter.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/File.h"
#include "util/GlobalExecutor.h"

// NOTE: The `ChunkWriter` only exists in the C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace chunkwisePermutationWriter;
using ad_utility::source_location;

// Return an `Id` of type `VocabIndex` from `index`.
Id V(int64_t index) {
  AD_CONTRACT_CHECK(index >= 0);
  return Id::makeFromVocabIndex(VocabIndex::make(index));
}

// A row of the test input (with the four columns `col0`, `col1`, `col2`, and
// the graph), as integers which are converted via `V`.
using TestRow = std::vector<int64_t>;
using TestRows = std::vector<TestRow>;

constexpr size_t NUM_COLUMNS = 4;
// The blocksize of the permutations, so relations with more than 16 rows are
// large.
constexpr size_t BLOCKSIZE = 20;

// Return the `numRows` rows of the relation with the given `col0`, sorted.
// Every pair of consecutive rows forms the same triple with two different
// graphs (which therefore have to end up in the same block), and every value
// of `col1` occurs (up to) four times.
TestRows relation(int64_t col0, size_t numRows) {
  TestRows rows;
  for (size_t i = 0; i < numRows; ++i) {
    auto j = static_cast<int64_t>(i);
    rows.push_back({col0, j / 4, j / 2, j % 2});
  }
  return rows;
}

// Concatenate the `relations`.
TestRows concat(const std::vector<TestRows>& relations) {
  TestRows result;
  for (const auto& rows : relations) {
    result.insert(result.end(), rows.begin(), rows.end());
  }
  return result;
}

// Return the rows of the twin permutation of the `rows` (columns 1 and 2
// swapped), sorted.
TestRows twinRows(TestRows rows) {
  for (auto& row : rows) {
    std::swap(row[1], row[2]);
  }
  ql::ranges::sort(rows);
  return rows;
}

// Return the rows `[begin, end)` of the `rows`.
TestRows subRows(const TestRows& rows, size_t begin, size_t end) {
  return {rows.begin() + begin, rows.begin() + end};
}

// Convert the `rows` to a `Block`.
Block makeBlock(const TestRows& rows) {
  Block block{NUM_COLUMNS, ad_utility::makeUnlimitedAllocator<Id>()};
  block.resize(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    for (size_t col = 0; col < NUM_COLUMNS; ++col) {
      block(i, col) = V(rows[i][col]);
    }
  }
  return block;
}

// Convert the rows of an `IdTable` (with `NUM_COLUMNS` columns) to `TestRows`.
template <typename Table>
TestRows toTestRows(const Table& table) {
  TestRows rows;
  for (size_t i = 0; i < table.numRows(); ++i) {
    TestRow row;
    for (size_t col = 0; col < NUM_COLUMNS; ++col) {
      row.push_back(static_cast<int64_t>(table(i, col).getVocabIndex().get()));
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

// The two `CompressedRelationWriter`s of a pair of twin permutations, together
// with the `ChunkWriterContext` of the chunks that write them.
class TwinWriters {
 public:
  std::string basename_;
  std::unique_ptr<CompressedRelationWriter> writer1_;
  std::unique_ptr<CompressedRelationWriter> writer2_;
  ChunkWriterContext context_;
  std::vector<CompressedBlockMetadata> blocks1_;
  std::vector<CompressedBlockMetadata> blocks2_;

  // Create the writers for the permutation with the given `keyOrder`, whose
  // files are named after the `basename`.
  explicit TwinWriters(std::string basename,
                       qlever::KeyOrder keyOrder = qlever::KeyOrder{0, 1, 2, 3},
                       bool removeDuplicates = false)
      : basename_{std::move(basename)},
        writer1_{makeWriter(basename_ + ".perm1")},
        writer2_{makeWriter(basename_ + ".perm2")},
        context_{
            *writer1_,
            *writer2_,
            keyOrder.keys().at(0),
            ChunkWriterContext::makePermutedColIndices(keyOrder, NUM_COLUMNS),
            removeDuplicates,
            basename_,
            ad_utility::MemorySize::kilobytes(100),
            ad_utility::globalExecutor()} {
    ChunkWriter::connectTwinWriters(*writer1_, *writer2_);
  }

  ~TwinWriters() {
    ad_utility::deleteFile(basename_ + ".perm1", false);
    ad_utility::deleteFile(basename_ + ".perm2", false);
  }

  // Finish both writers and store their blocks.
  void finish() {
    blocks1_ = std::move(*writer1_).getFinishedBlocks();
    blocks2_ = std::move(*writer2_).getFinishedBlocks();
  }

  // Scan the relation with the given `col0` from the first (`twin == false`)
  // or the second permutation. Return all its rows, including `col0`.
  TestRows scan(int64_t col0, bool twin) const {
    const auto& blocks = twin ? blocks2_ : blocks1_;
    CompressedRelationReader reader{
        ad_utility::makeUnlimitedAllocator<Id>(),
        ad_utility::File{basename_ + (twin ? ".perm2" : ".perm1"), "r"}};
    BlockMetadataSpan blockSpan{blocks};
    BlockMetadataRanges blockRanges{{blockSpan.begin(), blockSpan.end()}};
    ScanSpecification scanSpec{V(col0), std::nullopt, std::nullopt};
    static const LocatedTriplesPerBlock emptyLocatedTriples{};
    auto scanned = reader.scan(
        CompressedRelationReader::ScanSpecAndBlocks{scanSpec, blockRanges},
        std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID},
        std::make_shared<ad_utility::CancellationHandle<>>(),
        emptyLocatedTriples, ad_utility::globalExecutor());
    TestRows rows;
    for (size_t i = 0; i < scanned.numRows(); ++i) {
      TestRow row{col0};
      for (size_t col = 0; col < scanned.numColumns(); ++col) {
        row.push_back(
            static_cast<int64_t>(scanned(i, col).getVocabIndex().get()));
      }
      rows.push_back(std::move(row));
    }
    return rows;
  }

  // Check that both permutations contain exactly the `rows` of the complete
  // relation with the given `col0` (in the first permutation) and their twins
  // (in the second permutation).
  void expectCompleteRelation(int64_t col0, const TestRows& rows,
                              source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(scan(col0, false), rows);
    EXPECT_EQ(scan(col0, true), twinRows(rows));
  }

 private:
  static std::unique_ptr<CompressedRelationWriter> makeWriter(
      const std::string& filename) {
    return std::make_unique<CompressedRelationWriter>(
        NUM_COLUMNS, ad_utility::File{filename, "w"}, BLOCKSIZE,
        ad_utility::globalExecutor());
  }
};

// Process the `rows` with the `writer`, cut into input blocks of
// `inputBlockSize` rows each, and finish the chunk.
ChunkResult processRows(ChunkWriter& writer, const TestRows& rows,
                        size_t inputBlockSize) {
  for (size_t begin = 0; begin < rows.size(); begin += inputBlockSize) {
    size_t end = std::min(begin + inputBlockSize, rows.size());
    auto shared = writer.processBlock(makeBlock(subRows(rows, begin, end)));
    EXPECT_NE(shared, nullptr);
  }
  return writer.finishChunk();
}

// Return all the rows that the `twinRuns_` of the `part` contain, sorted.
TestRows rowsOfTwinRuns(const BoundaryPart& part) {
  AD_CONTRACT_CHECK(part.twinRuns_.has_value());
  const auto& runs = part.twinRuns_.value();
  TestRows rows;
  for (size_t run = 0; run < runs.numRuns(); ++run) {
    for (size_t block = 0; block < runs.numBlocks(run); ++block) {
      auto blockRows = toTestRows(runs.getBlock(run, block));
      rows.insert(rows.end(), blockRows.begin(), blockRows.end());
    }
  }
  ql::ranges::sort(rows);
  return rows;
}

// Check the `part` of the relation with the given `col0`, which consists of
// the `rows`: the rows that the part keeps in memory, together with the rows
// that the chunk has written directly (which are scanned from the first
// permutation of the `writers`), have to be exactly the `rows`, the twin
// permutation must not contain anything of the relation yet, and the twin rows
// of a large part have to be in its `twinRuns_`.
void expectBoundaryPart(const BoundaryPart& part, int64_t col0,
                        const TestRows& rows, bool isLarge,
                        const TwinWriters& writers,
                        source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_EQ(part.col0Id_, V(col0));
  EXPECT_EQ(part.numRows_, rows.size());
  EXPECT_EQ(part.isLarge(), isLarge);
  EXPECT_EQ(part.twinRuns_.has_value(), isLarge);
  TestRows written = toTestRows(part.rows_);
  auto direct = writers.scan(col0, false);
  written.insert(written.end(), direct.begin(), direct.end());
  if (part.tail_.has_value()) {
    EXPECT_TRUE(isLarge);
    auto tail = toTestRows(part.tail_.value());
    written.insert(written.end(), tail.begin(), tail.end());
  }
  EXPECT_EQ(written, rows);
  if (!isLarge) {
    EXPECT_TRUE(direct.empty());
  } else {
    EXPECT_FALSE(direct.empty());
    EXPECT_EQ(rowsOfTwinRuns(part), twinRows(rows));
  }
  EXPECT_TRUE(writers.scan(col0, true).empty());

  // The distinct `col1` IDs of the part.
  std::set<int64_t> col1Values;
  for (const auto& row : rows) {
    col1Values.insert(row[1]);
  }
  EXPECT_EQ(part.distinctCol1_.count_, col1Values.size());
  EXPECT_EQ(part.distinctCol1_.first_, V(rows.front()[1]));
  EXPECT_EQ(part.distinctCol1_.last_, V(rows.back()[1]));
}

// Check the metadata of the large relation with the given `col0` and the
// `rows` (in the first permutation) in `metadata`.
void expectMetadataOfLargeRelation(
    const std::pair<CompressedRelationMetadata, CompressedRelationMetadata>&
        metadata,
    int64_t col0, const TestRows& rows,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  const auto& [md1, md2] = metadata;
  EXPECT_EQ(md1.col0Id_, V(col0));
  EXPECT_EQ(md2.col0Id_, V(col0));
  EXPECT_EQ(md1.numRows_, rows.size());
  EXPECT_EQ(md2.numRows_, rows.size());
  std::set<int64_t> col1Values;
  std::set<int64_t> col2Values;
  for (const auto& row : rows) {
    col1Values.insert(row[1]);
    col2Values.insert(row[2]);
  }
  EXPECT_FLOAT_EQ(md1.multiplicityCol1_,
                  CompressedRelationWriter::computeMultiplicity(
                      rows.size(), col1Values.size()));
  EXPECT_FLOAT_EQ(md2.multiplicityCol1_,
                  CompressedRelationWriter::computeMultiplicity(
                      rows.size(), col2Values.size()));
}
}  // namespace

// _____________________________________________________________________________
TEST(ChunkWriter, singleChunkWritesAllRelations) {
  // Small relations (some of them batched), a medium relation that spans
  // several input blocks, and large relations, the last of which ends the
  // chunk.
  auto r1 = relation(1, 3);
  auto r2 = relation(2, 16);
  auto r3 = relation(3, 1);
  auto r4 = relation(4, 17);
  auto r5 = relation(5, 96);
  auto r6 = relation(6, 2);
  auto r7 = relation(7, 62);
  auto rows = concat({r1, r2, r3, r4, r5, r6, r7});
  for (size_t inputBlockSize : {1ul, 7ul, 33ul, 1000ul}) {
    TwinWriters writers{
        absl::StrCat(gtestCurrentTestName(), ".", inputBlockSize)};
    ChunkResult result;
    {
      ChunkWriter writer{writers.context_, 0, 1};
      result = processRows(writer, rows, inputBlockSize);
    }
    writers.finish();
    EXPECT_FALSE(result.firstPart_.has_value());
    EXPECT_FALSE(result.lastPart_.has_value());
    EXPECT_FALSE(result.spansWholeChunk_);
    EXPECT_EQ(result.numRows_, rows.size());
    EXPECT_EQ(result.numMiddleRelations_, 7);
    EXPECT_EQ(writers.context_.numInputRows_.load(), rows.size());
    EXPECT_EQ(writers.context_.numRowsProcessed_.load(), rows.size());
    // Only the large relations have metadata.
    ASSERT_EQ(result.middleMetadata_.size(), 3);
    expectMetadataOfLargeRelation(result.middleMetadata_[0], 4, r4);
    expectMetadataOfLargeRelation(result.middleMetadata_[1], 5, r5);
    expectMetadataOfLargeRelation(result.middleMetadata_[2], 7, r7);
    for (const auto& [col0, relationRows] :
         std::vector<std::pair<int64_t, TestRows>>{
             {1, r1}, {2, r2}, {3, r3}, {4, r4}, {5, r5}, {6, r6}, {7, r7}}) {
      writers.expectCompleteRelation(col0, relationRows);
    }
    // No block of a large relation contains rows of another relation, and the
    // two rows of a triple (which only differ in the graph) are never split
    // across blocks.
    for (const auto& block : writers.blocks1_) {
      EXPECT_LE(block.numRows_, 2 * BLOCKSIZE);
      const auto& first = block.firstTriple_;
      const auto& last = block.lastTriple_;
      if (first.col0Id_ == V(5) || first.col0Id_ == V(7)) {
        EXPECT_EQ(first.col0Id_, last.col0Id_);
        EXPECT_EQ(first.graphId_, V(0));
        EXPECT_EQ(last.graphId_, V(1));
      }
    }
  }
}

// _____________________________________________________________________________
TEST(ChunkWriter, middleChunkReturnsItsBoundaryParts) {
  // The first relation of a middle chunk is small and the last one is large,
  // with middle relations in between.
  auto first = relation(10, 5);
  auto r11 = relation(11, 4);
  auto r12 = relation(12, 50);
  auto last = relation(13, 77);
  auto rows = concat({first, r11, r12, last});
  for (size_t inputBlockSize : {3ul, 25ul, 1000ul}) {
    TwinWriters writers{
        absl::StrCat(gtestCurrentTestName(), ".", inputBlockSize)};
    ChunkResult result;
    {
      ChunkWriter writer{writers.context_, 1, 3};
      result = processRows(writer, rows, inputBlockSize);
    }
    ASSERT_TRUE(result.firstPart_.has_value());
    ASSERT_TRUE(result.lastPart_.has_value());
    EXPECT_FALSE(result.spansWholeChunk_);
    EXPECT_EQ(result.numRows_, rows.size());
    EXPECT_EQ(result.numMiddleRelations_, 2);
    ASSERT_EQ(result.middleMetadata_.size(), 1);
    expectMetadataOfLargeRelation(result.middleMetadata_[0], 12, r12);
    writers.finish();
    writers.expectCompleteRelation(11, r11);
    writers.expectCompleteRelation(12, r12);
    expectBoundaryPart(result.firstPart_.value(), 10, first, false, writers);
    expectBoundaryPart(result.lastPart_.value(), 13, last, true, writers);
    // The last part was started inside the chunk, so all its rows before its
    // first block have been written directly.
    EXPECT_TRUE(result.lastPart_->rows_.empty());
  }
}

// _____________________________________________________________________________
TEST(ChunkWriter, largeFirstRelationKeepsItsHead) {
  // The first relation of a chunk is large and ends inside the chunk, and the
  // last relation is small.
  auto first = relation(20, 90);
  auto r21 = relation(21, 30);
  auto last = relation(22, 6);
  auto rows = concat({first, r21, last});
  for (size_t inputBlockSize : {4ul, 1000ul}) {
    TwinWriters writers{
        absl::StrCat(gtestCurrentTestName(), ".", inputBlockSize)};
    ChunkResult result;
    {
      ChunkWriter writer{writers.context_, 1, 3};
      result = processRows(writer, rows, inputBlockSize);
    }
    writers.finish();
    ASSERT_TRUE(result.firstPart_.has_value());
    ASSERT_TRUE(result.lastPart_.has_value());
    const auto& firstPart = result.firstPart_.value();
    // The head (the first block of rows) is kept in memory, and the relation
    // ends inside the chunk, so there is no tail.
    EXPECT_EQ(firstPart.rows_.numRows(), BLOCKSIZE);
    EXPECT_FALSE(firstPart.tail_.has_value());
    expectBoundaryPart(firstPart, 20, first, true, writers);
    expectBoundaryPart(result.lastPart_.value(), 22, last, false, writers);
    writers.expectCompleteRelation(21, r21);
    EXPECT_EQ(result.numMiddleRelations_, 1);
  }
}

// _____________________________________________________________________________
TEST(ChunkWriter, chunkWithASingleRelation) {
  // A chunk that consists of a single relation, which may continue in both
  // neighboring chunks, first with a small and then with a large relation.
  for (size_t numRows : {7ul, 130ul}) {
    auto rows = relation(30, numRows);
    TwinWriters writers{absl::StrCat(gtestCurrentTestName(), ".", numRows)};
    ChunkResult result;
    {
      ChunkWriter writer{writers.context_, 1, 3};
      result = processRows(writer, rows, 9);
    }
    writers.finish();
    ASSERT_TRUE(result.firstPart_.has_value());
    EXPECT_FALSE(result.lastPart_.has_value());
    EXPECT_TRUE(result.spansWholeChunk_);
    EXPECT_TRUE(result.middleMetadata_.empty());
    EXPECT_EQ(result.numMiddleRelations_, 0);
    bool isLarge = numRows > BLOCKSIZE;
    expectBoundaryPart(result.firstPart_.value(), 30, rows, isLarge, writers);
    // The relation reaches the end of the chunk, so a large part keeps its
    // last rows as the tail.
    if (isLarge) {
      EXPECT_TRUE(result.firstPart_->tail_.has_value());
    }
  }
}

// _____________________________________________________________________________
TEST(ChunkWriter, lastChunkCompletesItsLastRelation) {
  // In the last chunk the last relation is complete, so it is written like a
  // middle relation, both if it is small and if it is large.
  auto first = relation(40, 3);
  for (size_t numRows : {5ul, 70ul}) {
    auto last = relation(41, numRows);
    auto rows = concat({first, last});
    TwinWriters writers{absl::StrCat(gtestCurrentTestName(), ".", numRows)};
    ChunkResult result;
    {
      ChunkWriter writer{writers.context_, 2, 3};
      result = processRows(writer, rows, 11);
    }
    writers.finish();
    ASSERT_TRUE(result.firstPart_.has_value());
    EXPECT_FALSE(result.lastPart_.has_value());
    EXPECT_FALSE(result.spansWholeChunk_);
    EXPECT_EQ(result.numMiddleRelations_, 1);
    EXPECT_EQ(result.middleMetadata_.size(), numRows > BLOCKSIZE ? 1 : 0);
    expectBoundaryPart(result.firstPart_.value(), 40, first, false, writers);
    writers.expectCompleteRelation(41, last);
  }
}

// _____________________________________________________________________________
TEST(ChunkWriter, removeDuplicates) {
  auto r1 = relation(1, 6);
  auto r2 = relation(2, 40);
  auto rows = concat({r1, r2});
  // Every row occurs three times, so the duplicates span the boundaries of the
  // input blocks.
  TestRows rowsWithDuplicates;
  for (const auto& row : rows) {
    rowsWithDuplicates.insert(rowsWithDuplicates.end(), 3, row);
  }
  TwinWriters writers{gtestCurrentTestName(), qlever::KeyOrder{0, 1, 2, 3},
                      true};
  ChunkResult result;
  {
    ChunkWriter writer{writers.context_, 0, 1};
    // A block that only consists of duplicates of the last row of the
    // previous block is dropped completely.
    EXPECT_NE(writer.processBlock(makeBlock(subRows(rowsWithDuplicates, 0, 2))),
              nullptr);
    EXPECT_EQ(writer.processBlock(makeBlock(subRows(rowsWithDuplicates, 2, 3))),
              nullptr);
    auto shared =
        writer.processBlock(makeBlock(subRows(rowsWithDuplicates, 3, 10)));
    ASSERT_NE(shared, nullptr);
    // The block that is returned for the callbacks has no duplicates either.
    EXPECT_EQ(toTestRows(*shared), subRows(rows, 1, 4));
    for (size_t begin = 10; begin < rowsWithDuplicates.size(); begin += 10) {
      writer.processBlock(
          makeBlock(subRows(rowsWithDuplicates, begin,
                            std::min(begin + 10, rowsWithDuplicates.size()))));
    }
    result = writer.finishChunk();
  }
  writers.finish();
  EXPECT_EQ(result.numRows_, rows.size());
  EXPECT_EQ(writers.context_.numInputRows_.load(), rowsWithDuplicates.size());
  EXPECT_EQ(writers.context_.numUniqueRows_.load(), rows.size());
  EXPECT_EQ(writers.context_.numRowsProcessed_.load(), rows.size());
  writers.expectCompleteRelation(1, r1);
  writers.expectCompleteRelation(2, r2);
}

// _____________________________________________________________________________
TEST(ChunkWriter, nonIdentityPermutation) {
  // The input is sorted by column 1, which becomes `col0` of the permutation.
  auto r1 = relation(1, 9);
  auto r2 = relation(2, 55);
  auto permutedRows = concat({r1, r2});
  TestRows input = permutedRows;
  for (auto& row : input) {
    std::swap(row[0], row[1]);
  }
  TwinWriters writers{gtestCurrentTestName(), qlever::KeyOrder{1, 0, 2, 3}};
  ChunkResult result;
  {
    ChunkWriter writer{writers.context_, 0, 1};
    result = processRows(writer, input, 8);
  }
  writers.finish();
  EXPECT_EQ(result.numMiddleRelations_, 2);
  ASSERT_EQ(result.middleMetadata_.size(), 1);
  expectMetadataOfLargeRelation(result.middleMetadata_[0], 2, r2);
  writers.expectCompleteRelation(1, r1);
  writers.expectCompleteRelation(2, r2);
}

// _____________________________________________________________________________
TEST(ChunkWriter, writeRowsOfLargeRelation) {
  // The rows of a large relation are cut into blocks of about the blocksize,
  // both via the queue of the writer and in the calling thread, without
  // splitting the two rows of a triple.
  for (bool useQueue : {false, true}) {
    auto rows = relation(50, 94);
    TwinWriters writers{absl::StrCat(gtestCurrentTestName(), ".", useQueue)};
    ChunkWriter::writeRowsOfLargeRelation(writers.context_.writer1_, V(50),
                                          makeBlock(rows), useQueue);
    writers.finish();
    EXPECT_EQ(writers.scan(50, false), rows);
    EXPECT_TRUE(writers.scan(50, true).empty());
    ASSERT_EQ(writers.blocks1_.size(), 5);
    for (const auto& block : writers.blocks1_) {
      EXPECT_EQ(block.firstTriple_.graphId_, V(0));
      EXPECT_EQ(block.lastTriple_.graphId_, V(1));
    }
  }
}

// _____________________________________________________________________________
TEST(ChunkWriter, writeSortedBlocksOfLargeRelation) {
  // Blocks of a sorted twin, whose boundaries split the two rows of a triple,
  // are realigned, written, and the metadata is computed from all of them.
  auto rows = relation(60, 50);
  std::vector<Block> blocks;
  for (auto [begin, end] : std::vector<std::pair<size_t, size_t>>{
           {0, 7}, {7, 8}, {8, 8}, {8, 31}, {31, 50}}) {
    blocks.push_back(makeBlock(subRows(rows, begin, end)));
  }
  TwinWriters writers{gtestCurrentTestName()};
  auto metadata = ChunkWriter::writeSortedBlocksOfLargeRelation(
      writers.context_.writer2_, V(60),
      ad_utility::InputRangeTypeErased{std::move(blocks)});
  writers.finish();
  EXPECT_EQ(metadata.col0Id_, V(60));
  EXPECT_EQ(metadata.numRows_, rows.size());
  EXPECT_FLOAT_EQ(metadata.multiplicityCol1_,
                  CompressedRelationWriter::computeMultiplicity(50, 13));
  EXPECT_EQ(writers.scan(60, true), rows);
  for (const auto& block : writers.blocks2_) {
    EXPECT_EQ(block.firstTriple_.graphId_, V(0));
    EXPECT_EQ(block.lastTriple_.graphId_, V(1));
  }
}

// _____________________________________________________________________________
TEST(ChunkWriter, connectTwinWritersChecksTheWriters) {
  auto makeWriter = [](const std::string& filename, size_t blocksize) {
    return CompressedRelationWriter{NUM_COLUMNS,
                                    ad_utility::File{filename, "w"}, blocksize,
                                    ad_utility::globalExecutor()};
  };
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename + ".1", false);
    ad_utility::deleteFile(filename + ".2", false);
  };
  auto writer1 = makeWriter(filename + ".1", BLOCKSIZE);
  auto writer2 = makeWriter(filename + ".2", BLOCKSIZE + 1);
  EXPECT_ANY_THROW(ChunkWriter::connectTwinWriters(writer1, writer2));
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
