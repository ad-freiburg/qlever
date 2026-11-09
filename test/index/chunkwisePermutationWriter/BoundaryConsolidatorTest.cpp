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

#include <future>
#include <map>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IndexTestHelpers.h"
#include "./ChunkWriterTestHelpers.h"
#include "index/chunkwisePermutationWriter/BoundaryConsolidator.h"
#include "index/compressedRelationWriter/CompressedRelationHelpersImpl.h"

// NOTE: The `BoundaryConsolidator` only exists in the C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace chunkWriterTestHelpers;

// Split the `rows` (which have to be sorted) into chunks at the given
// `boundaries` (the indices of the first rows of all the chunks but the first
// one, in ascending order; equal indices yield empty chunks), write every
// chunk with a `ChunkWriter` of its own (with input blocks of `inputBlockSize`
// rows), feed their results to a `BoundaryConsolidator` in order, and check
// that both permutations and their metadata contain exactly the `rows`. The
// chunks run one after the other, or all at the same time on threads of their
// own if `concurrentChunks` is set (then the consolidation of the first chunks
// overlaps with the writing of the later ones, as in a real merge).
void consolidateAndCheck(const TestRows& rows,
                         const std::vector<size_t>& boundaries,
                         size_t inputBlockSize = 7,
                         bool concurrentChunks = false,
                         source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  TwinWriters writers{gtestCurrentTestName()};
  std::vector<CompressedRelationMetadata> metadata1;
  std::vector<CompressedRelationMetadata> metadata2;
  BoundaryConsolidator consolidator{
      writers.context_,
      [&metadata1](ql::span<const CompressedRelationMetadata> md) {
        metadata1.insert(metadata1.end(), md.begin(), md.end());
      },
      [&metadata2](ql::span<const CompressedRelationMetadata> md) {
        metadata2.insert(metadata2.end(), md.begin(), md.end());
      },
      ad_utility::testing::multiThreadExecutor(),
      ad_utility::MemorySize::megabytes(10)};

  std::vector<size_t> begins{0};
  begins.insert(begins.end(), boundaries.begin(), boundaries.end());
  const size_t numChunks = begins.size();
  auto writeChunk = [&](size_t chunkIndex) {
    size_t begin = begins.at(chunkIndex);
    size_t end =
        chunkIndex + 1 < numChunks ? begins.at(chunkIndex + 1) : rows.size();
    AD_CONTRACT_CHECK(begin <= end);
    ChunkWriter writer{writers.context_, chunkIndex, numChunks};
    return processRows(writer, subRows(rows, begin, end), inputBlockSize);
  };
  std::vector<std::future<ChunkResult>> futures;
  if (concurrentChunks) {
    for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
      futures.push_back(std::async(std::launch::async, writeChunk, chunkIndex));
    }
  }
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    ChunkResult result = concurrentChunks ? futures.at(chunkIndex).get()
                                          : writeChunk(chunkIndex);
    consolidator.addChunkResult(chunkIndex, std::move(result));
  }
  EXPECT_EQ(consolidator.numRows(), rows.size());
  auto result = consolidator.finish();
  writers.blocks1_ = std::move(result.blockMetadata_);
  writers.blocks2_ = std::move(result.blockMetadataSwitched_);

  // Group the expected rows by their relation. A `std::map` (and not a hash
  // map) is used, because the relations are iterated in their order below.
  std::map<int64_t, TestRows> relations;
  for (const auto& row : rows) {
    relations[row[0]].push_back(row);
  }
  EXPECT_EQ(result.numDistinctCol0_, relations.size());
  std::vector<std::pair<int64_t, size_t>> expectedMetadata;
  for (const auto& [col0, relationRows] : relations) {
    writers.expectCompleteRelation(col0, relationRows);
    if (!compressedRelationHelpers::isSmallRelation(relationRows.size(),
                                                    BLOCKSIZE)) {
      expectedMetadata.emplace_back(col0, relationRows.size());
    }
  }
  // Exactly the large relations have metadata, in the order of the relations.
  auto toPairs = [](const std::vector<CompressedRelationMetadata>& metadata) {
    return ::ranges::to<std::vector<std::pair<int64_t, size_t>>>(
        metadata | ql::views::transform([](const auto& md) {
          return std::pair{
              static_cast<int64_t>(md.col0Id_.getVocabIndex().get()),
              size_t{md.numRows_}};
        }));
  };
  EXPECT_EQ(toPairs(metadata1), expectedMetadata);
  EXPECT_EQ(toPairs(metadata2), expectedMetadata);
}
}  // namespace

// Test a single chunk, which needs no consolidation of boundary parts.
TEST(BoundaryConsolidator, singleChunk) {
  consolidateAndCheck(concat({relation(1, 5), relation(2, 30), relation(3, 3)}),
                      {});
}

// Test a small relation that is split into small parts by several chunks,
// with small relations before and after it.
TEST(BoundaryConsolidator, smallRelationAcrossThreeChunks) {
  auto rows = concat({relation(1, 4), relation(2, 12), relation(3, 2)});
  // Relation 2 occupies the rows `[4, 16)`.
  consolidateAndCheck(rows, {7, 11});
}

// Test a relation that only consists of small parts, but that is large as a
// whole, so its twin rows are sorted in memory.
TEST(BoundaryConsolidator, largeRelationFromSmallParts) {
  auto rows = concat({relation(1, 3), relation(2, 30), relation(3, 3)});
  // Relation 2 occupies the rows `[3, 33)`, which are split into parts of 10,
  // 10 (a chunk that consists of that relation only), and 10 rows.
  consolidateAndCheck(rows, {13, 23});
}

// Test a large relation that consists of large and small parts, so the runs of
// the twin sorters of the large parts and the twin rows of the small parts are
// merged.
TEST(BoundaryConsolidator, largeRelationFromLargeAndSmallParts) {
  auto rows = concat({relation(1, 5), relation(2, 120), relation(3, 4)});
  // Relation 2 occupies the rows `[5, 125)`: a large last part of chunk 0, a
  // small part that spans the whole chunk 1, a large part that spans the
  // whole chunk 2, and a large first part of chunk 3.
  consolidateAndCheck(rows, {50, 55, 100});
  // The same with different input block sizes, so that the chunks cut their
  // blocks at different positions.
  consolidateAndCheck(rows, {50, 55, 100}, 1);
  consolidateAndCheck(rows, {50, 55, 100}, 64);
}

// Test a large relation that only consists of large parts, so the sorter for
// the twin rows of the small parts stays empty.
TEST(BoundaryConsolidator, largeRelationFromLargePartsOnly) {
  auto rows = concat({relation(1, 5), relation(2, 100), relation(3, 4)});
  // Relation 2 occupies the rows `[5, 105)`: a large last part of chunk 0 and
  // a large first part of chunk 1, with 50 rows each.
  consolidateAndCheck(rows, {55});
}

// Test that the metadata of the boundary relations and of the relations in
// the middle of the chunks is emitted in the order of the relations.
TEST(BoundaryConsolidator, boundaryAndMiddleRelationsInOrder) {
  auto rows = concat({relation(1, 30), relation(2, 20), relation(3, 25),
                      relation(4, 6), relation(5, 40), relation(6, 2)});
  // Relation 2 (rows `[30, 50)`) is split into two small parts, relation 5
  // (rows `[81, 121)`) into a small and a large part. Relations 3 and 4 lie in
  // the middle of chunk 1.
  consolidateAndCheck(rows, {40, 85});
}

// Test chunks without any rows, also between the parts of a relation.
TEST(BoundaryConsolidator, emptyChunks) {
  auto rows = concat({relation(1, 10), relation(2, 50), relation(3, 1)});
  consolidateAndCheck(rows, {0, 20, 20, 40, 61, 61});
  consolidateAndCheck({}, {0, 0});
}

// Test chunks that run at the same time on different threads, while the
// consolidator already processes the results of the first chunks: the two
// writers are shared by the chunks and the consolidator. The relations cover
// small and large boundary parts as well as relations in the middle of the
// chunks.
TEST(BoundaryConsolidator, concurrentChunks) {
  auto rows = concat({relation(1, 30), relation(2, 100), relation(3, 5),
                      relation(4, 60), relation(5, 3), relation(6, 80),
                      relation(7, 40)});
  // The large relations 2, 4, 6, and 7 occupy the rows `[30, 130)`,
  // `[135, 195)`, `[198, 278)`, and `[278, 318)`, so chunks 2 and 4 consist of
  // a single relation, and chunks 1, 3, 5, and 6 have two boundary parts.
  std::vector<size_t> boundaries{20, 60, 110, 140, 170, 230, 280};
  for (size_t i = 0; i < 3; ++i) {
    consolidateAndCheck(rows, boundaries, 7, true);
  }
}

// Test a boundary between two different relations, so that the first part of
// a chunk belongs to a new relation.
TEST(BoundaryConsolidator, boundaryBetweenRelations) {
  auto rows = concat({relation(1, 10), relation(2, 40), relation(3, 10)});
  consolidateAndCheck(rows, {10, 50});
}

// Test that the chunks have to be added in order, and that equal rows at the
// boundary of two consecutive chunks are detected.
TEST(BoundaryConsolidator, invariantsAreChecked) {
  // Rows 7 and 8 are equal (the merge would never split them across chunks).
  auto rows = concat({relation(1, 5), relation(2, 10)});
  rows.insert(rows.begin() + 8, rows.at(7));
  TwinWriters writers{gtestCurrentTestName()};
  auto ignoreMetadata =
      []([[maybe_unused]] ql::span<const CompressedRelationMetadata> md) {};
  BoundaryConsolidator consolidator{writers.context_, ignoreMetadata,
                                    ignoreMetadata,
                                    ad_utility::testing::multiThreadExecutor(),
                                    ad_utility::MemorySize::megabytes(10)};
  auto writeChunk = [&writers, &rows](size_t chunkIndex, size_t begin,
                                      size_t end) {
    ChunkWriter writer{writers.context_, chunkIndex, 2};
    return processRows(writer, subRows(rows, begin, end), 7);
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      consolidator.addChunkResult(1, writeChunk(1, 8, rows.size())),
      ::testing::HasSubstr("in order"));
  consolidator.addChunkResult(0, writeChunk(0, 0, 8));
  AD_EXPECT_THROW_WITH_MESSAGE(
      consolidator.addChunkResult(1, writeChunk(1, 8, rows.size())),
      ::testing::HasSubstr("equal rows"));
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
