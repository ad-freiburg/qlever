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
#include <future>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "./ChunkWriterTestHelpers.h"
#include "global/Constants.h"
#include "index/CompressedRelationReader.h"
#include "index/LocatedTriples.h"
#include "index/chunkwisePermutationWriter/ChunkWriter.h"
#include "index/compressedRelationWriter/CompressedRelationWriter.h"
#include "util/File.h"
#include "util/HashSet.h"

// NOTE: The `ChunkWriter` only exists in the C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace chunkWriterTestHelpers;

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
  TestRows written = toTestRows(part.rowsBeforeWrittenBlocks_);
  auto direct = writers.scan(col0, false);
  written.insert(written.end(), direct.begin(), direct.end());
  if (part.rowsAfterWrittenBlocks_.has_value()) {
    EXPECT_TRUE(isLarge);
    auto tail = toTestRows(part.rowsAfterWrittenBlocks_.value());
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
  ad_utility::HashSet<int64_t> col1Values;
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
  ad_utility::HashSet<int64_t> col1Values;
  ad_utility::HashSet<int64_t> col2Values;
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

// Test that a merge with a single chunk writes all its relations
// completely, like the sequential writer, for several sizes of the input
// blocks.
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

// Test that a middle chunk writes its middle relations and returns its first
// and its last relation as boundary parts.
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
    EXPECT_TRUE(result.lastPart_->rowsBeforeWrittenBlocks_.empty());
  }
}

// Test that a large first relation keeps its first block of rows in memory
// and writes the rest directly.
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
    EXPECT_EQ(firstPart.rowsBeforeWrittenBlocks_.numRows(), BLOCKSIZE);
    EXPECT_FALSE(firstPart.rowsAfterWrittenBlocks_.has_value());
    expectBoundaryPart(firstPart, 20, first, true, writers);
    expectBoundaryPart(result.lastPart_.value(), 22, last, false, writers);
    writers.expectCompleteRelation(21, r21);
    EXPECT_EQ(result.numMiddleRelations_, 1);
  }
}

// Test a chunk that consists of a single relation, which is returned as a
// first part that spans the whole chunk.
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
      EXPECT_TRUE(result.firstPart_->rowsAfterWrittenBlocks_.has_value());
    }
  }
}

// Test that the last chunk writes its last relation completely instead of
// returning it as a boundary part.
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

// Test the removal of duplicate rows, also across the input blocks of the
// chunk, and the row statistics of the context.
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

// Test a permutation whose `col0` is not the first column of the input.
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

// Test `writeRowsOfLargeRelation` via the queue of the writer and in the
// calling thread.
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
  // Writing no rows at all writes no block.
  for (bool useQueue : {false, true}) {
    TwinWriters writers{
        absl::StrCat(gtestCurrentTestName(), ".empty.", useQueue)};
    ChunkWriter::writeRowsOfLargeRelation(writers.context_.writer1_, V(50),
                                          makeBlock({}), useQueue);
    writers.finish();
    EXPECT_TRUE(writers.blocks1_.empty());
  }
}

// Test `writeSortedBlocksOfLargeRelation` with blocks whose boundaries split
// a triple.
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

// Test that the `ChunkWriterContext` rejects writers that don't fit together.
TEST(ChunkWriter, contextChecksTheWriters) {
  auto makeWriter = [](const std::string& filename, size_t blocksize,
                       size_t numColumns = NUM_COLUMNS) {
    return CompressedRelationWriter{numColumns, ad_utility::File{filename, "w"},
                                    blocksize,
                                    ad_utility::testing::multiThreadExecutor()};
  };
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename + ".1", false);
    ad_utility::deleteFile(filename + ".2", false);
    ad_utility::deleteFile(filename + ".3", false);
  };
  auto writer1 = makeWriter(filename + ".1", BLOCKSIZE);
  auto writer2 = makeWriter(filename + ".2", BLOCKSIZE + 1);
  auto writer3 = makeWriter(filename + ".3", BLOCKSIZE, NUM_COLUMNS + 1);
  qlever::KeyOrder keyOrder{0, 1, 2, 3};
  auto makeContext = [&](CompressedRelationWriter& twin, size_t numColumns) {
    return std::make_unique<ChunkWriterContext>(
        writer1, twin, 0,
        compressedRelationHelpers::permutedColIndices(keyOrder, numColumns),
        false, filename, ad_utility::MemorySize::kilobytes(100));
  };
  // The writers have different blocksizes.
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeContext(writer2, NUM_COLUMNS),
      ::testing::HasSubstr("writer1.blocksize() == writer2.blocksize()"));
  // The column indices don't match the number of columns of the writers.
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeContext(writer2, NUM_COLUMNS + 1),
      ::testing::HasSubstr("permutedColIndices_.size()"));
  // The writers have different numbers of columns.
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeContext(writer3, NUM_COLUMNS),
      ::testing::HasSubstr("writer1.numColumns() == writer2.numColumns()"));
}

// Test that the small relations of a chunk are written as several blocks
// when they exceed the capacity of a single block.
TEST(ChunkWriter, smallRelationsFillSeveralBlocks) {
  // The small relations of a chunk together have more rows than a single block
  // of small relations can hold, so they are written as several blocks, both
  // when they are batched directly from the input blocks and when they are
  // buffered first.
  std::vector<TestRows> relations;
  for (int64_t col0 = 70; col0 < 76; ++col0) {
    relations.push_back(relation(col0, 6));
  }
  auto rows = concat(relations);
  for (size_t inputBlockSize : {5ul, 1000ul}) {
    TwinWriters writers{
        absl::StrCat(gtestCurrentTestName(), ".", inputBlockSize)};
    ChunkResult result;
    {
      ChunkWriter writer{writers.context_, 0, 1};
      result = processRows(writer, rows, inputBlockSize);
    }
    writers.finish();
    EXPECT_EQ(result.numMiddleRelations_, relations.size());
    EXPECT_TRUE(result.middleMetadata_.empty());
    EXPECT_GE(writers.blocks1_.size(), 2);
    EXPECT_GE(writers.blocks2_.size(), 2);
    for (const auto& rel : relations) {
      writers.expectCompleteRelation(rel.front()[0], rel);
    }
  }
}

// Test a chunk without any input.
TEST(ChunkWriter, emptyChunk) {
  // A chunk without any input (for example because all its rows were removed
  // as duplicates) has no parts and writes nothing.
  for (size_t chunkIndex : {0ul, 1ul, 2ul}) {
    TwinWriters writers{absl::StrCat(gtestCurrentTestName(), ".", chunkIndex)};
    ChunkResult result;
    {
      ChunkWriter writer{writers.context_, chunkIndex, 3};
      result = writer.finishChunk();
    }
    writers.finish();
    EXPECT_FALSE(result.firstPart_.has_value());
    EXPECT_FALSE(result.lastPart_.has_value());
    EXPECT_EQ(result.numRows_, 0);
    EXPECT_EQ(result.numMiddleRelations_, 0);
    EXPECT_TRUE(result.middleMetadata_.empty());
    EXPECT_TRUE(writers.blocks1_.empty());
    EXPECT_TRUE(writers.blocks2_.empty());
  }
}

// Test that a chunk that is destroyed before `finishChunk` neither throws nor
// writes anything.
TEST(ChunkWriter, destroyUnfinishedChunk) {
  // A chunk that is destroyed before `finishChunk` (e.g. because of an
  // exception) while it still buffers small relations must neither throw nor
  // write the buffered relations.
  auto rows = concat({relation(80, 6), relation(81, 6), relation(82, 6)});
  TwinWriters writers{gtestCurrentTestName()};
  {
    ChunkWriter writer{writers.context_, 0, 1};
    EXPECT_NE(writer.processBlock(makeBlock(rows)), nullptr);
  }
  writers.finish();
  EXPECT_TRUE(writers.blocks1_.empty());
  EXPECT_TRUE(writers.blocks2_.empty());
}

// Test that several chunks which run on different threads at the same time
// share the two writers correctly: every relation that starts and ends inside
// its chunk is complete in both permutations, and the boundary parts of all
// the chunks are right.
TEST(ChunkWriter, concurrentChunksShareTheWriters) {
  constexpr size_t NUM_CHUNKS = 8;
  // Chunk `i` consists of the relations `10 * i + 1` to `10 * i + 5`: a small
  // first relation, a large, a small, and a large middle relation, and a large
  // last relation.
  auto relationsOfChunk = [](size_t i) {
    auto c = static_cast<int64_t>(10 * i);
    return std::vector<TestRows>{relation(c + 1, 5), relation(c + 2, 50),
                                 relation(c + 3, 3), relation(c + 4, 30),
                                 relation(c + 5, 45)};
  };
  TwinWriters writers{gtestCurrentTestName()};
  std::vector<std::future<ChunkResult>> futures;
  for (size_t i = 0; i < NUM_CHUNKS; ++i) {
    futures.push_back(
        std::async(std::launch::async, [&writers, &relationsOfChunk, i] {
          ChunkWriter writer{writers.context_, i, NUM_CHUNKS};
          return processRows(writer, concat(relationsOfChunk(i)), 7);
        }));
  }
  std::vector<ChunkResult> results;
  for (auto& future : futures) {
    results.push_back(future.get());
  }
  writers.finish();

  // The first relation of a chunk is a boundary part unless the chunk is the
  // first one, the last relation unless the chunk is the last one. All the
  // other relations are complete.
  for (size_t i = 0; i < NUM_CHUNKS; ++i) {
    auto relations = relationsOfChunk(i);
    const auto& result = results.at(i);
    const bool isFirstChunk = i == 0;
    const bool isLastChunk = i + 1 == NUM_CHUNKS;
    EXPECT_EQ(result.firstPart_.has_value(), !isFirstChunk);
    EXPECT_EQ(result.lastPart_.has_value(), !isLastChunk);
    EXPECT_EQ(result.numMiddleRelations_, 3 + isFirstChunk + isLastChunk);
    for (size_t r = 0; r < relations.size(); ++r) {
      bool isBoundary = (r == 0 && !isFirstChunk) ||
                        (r + 1 == relations.size() && !isLastChunk);
      if (!isBoundary) {
        writers.expectCompleteRelation(relations[r].front()[0], relations[r]);
      }
    }
    auto c = static_cast<int64_t>(10 * i);
    if (!isFirstChunk) {
      expectBoundaryPart(result.firstPart_.value(), c + 1, relations.front(),
                         false, writers);
    }
    if (!isLastChunk) {
      expectBoundaryPart(result.lastPart_.value(), c + 5, relations.back(),
                         true, writers);
    }
  }
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
