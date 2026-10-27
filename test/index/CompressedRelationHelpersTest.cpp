// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <utility>
#include <vector>

#include "../util/IdTableHelpers.h"
#include "../util/IdTestHelpers.h"
#include "index/CompressedRelationHelpersImpl.h"

using namespace compressedRelationHelpers;

namespace {
auto V = ad_utility::testing::VocabId;

// Return the `col0` column of a table with the given `col0` IDs.
std::vector<Id> col0Column(const std::vector<int64_t>& ids) {
  std::vector<Id> result;
  for (auto id : ids) {
    result.push_back(V(id));
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(CompressedRelationHelpers, isSmallRelation) {
  EXPECT_TRUE(isSmallRelation(0, 10));
  EXPECT_TRUE(isSmallRelation(8, 10));
  EXPECT_FALSE(isSmallRelation(9, 10));
  EXPECT_FALSE(isSmallRelation(100, 10));
}

// _____________________________________________________________________________
TEST(CompressedRelationHelpers, findEndOfRun) {
  auto col0 = col0Column({1, 1, 2, 3, 3, 3, 3, 3, 3, 3, 4});
  EXPECT_EQ(findEndOfRun(col0, 0), 2);
  EXPECT_EQ(findEndOfRun(col0, 1), 2);
  EXPECT_EQ(findEndOfRun(col0, 2), 3);
  EXPECT_EQ(findEndOfRun(col0, 3), 10);
  EXPECT_EQ(findEndOfRun(col0, 10), 11);
}

// _____________________________________________________________________________
TEST(CompressedRelationHelpers, findFirstTripleChange) {
  // The fourth column (the graph) is ignored.
  auto rows = makeIdTableFromVector(
      {{1, 2, 3, 0}, {1, 2, 3, 1}, {1, 2, 3, 2}, {1, 2, 4, 0}, {1, 3, 0, 0}});
  auto triple = pickFirstThreeColumnsOfIdsWithoutLocalVocab(rows[0]);
  EXPECT_EQ(findFirstTripleChange(rows, 0, 5, triple), 3);
  EXPECT_EQ(findFirstTripleChange(rows, 1, 5, triple), 3);
  EXPECT_EQ(findFirstTripleChange(rows, 3, 5, triple), 3);
  // No change within the range.
  EXPECT_EQ(findFirstTripleChange(rows, 0, 2, triple), 2);
  auto lastTriple = pickFirstThreeColumnsOfIdsWithoutLocalVocab(rows[4]);
  EXPECT_EQ(findFirstTripleChange(rows, 4, 5, lastTriple), 5);
}

// _____________________________________________________________________________
TEST(CompressedRelationHelpers, forEachSliceOfLargeBlock) {
  auto slices = [](const IdTable& rows, size_t blocksize) {
    std::vector<std::pair<size_t, size_t>> result;
    forEachSliceOfLargeBlock(rows, blocksize, [&](size_t begin, size_t end) {
      result.emplace_back(begin, end);
    });
    return result;
  };
  using P = std::pair<size_t, size_t>;
  // All triples are distinct.
  auto distinct = makeIdTableFromVector(
      {{1, 1, 1}, {1, 1, 2}, {1, 1, 3}, {1, 1, 4}, {1, 2, 0}});
  EXPECT_THAT(slices(distinct, 2),
              ::testing::ElementsAre(P{0, 2}, P{2, 4}, P{4, 5}));
  EXPECT_THAT(slices(distinct, 5), ::testing::ElementsAre(P{0, 5}));
  EXPECT_THAT(slices(distinct, 10), ::testing::ElementsAre(P{0, 5}));

  // Equal triples (which differ only in the fourth column) are never split.
  auto withEqualTriples = makeIdTableFromVector({{1, 1, 1, 0},
                                                 {1, 1, 2, 0},
                                                 {1, 1, 2, 1},
                                                 {1, 1, 2, 2},
                                                 {1, 1, 3, 0},
                                                 {1, 1, 4, 0}});
  EXPECT_THAT(slices(withEqualTriples, 2),
              ::testing::ElementsAre(P{0, 4}, P{4, 6}));
  EXPECT_THAT(slices(withEqualTriples, 1),
              ::testing::ElementsAre(P{0, 1}, P{1, 4}, P{4, 5}, P{5, 6}));
}

// _____________________________________________________________________________
TEST(CompressedRelationHelpers, TripleAlignedBlockBuffer) {
  auto toVector = [](const std::optional<IdTable>& block) {
    std::vector<int64_t> result;
    for (const auto& row : block.value()) {
      result.push_back(row[2].getVocabIndex().get() * 10 +
                       row[3].getVocabIndex().get());
    }
    return result;
  };
  // Encode a row by its third (`c`) and fourth (`g`) column only.
  auto block = [](std::vector<std::pair<int64_t, int64_t>> cAndG) {
    VectorTable rows;
    for (auto [c, g] : cAndG) {
      rows.push_back({1, 1, c, g});
    }
    return makeIdTableFromVector(rows);
  };

  TripleAlignedBlockBuffer<IdTable> buffer;
  EXPECT_FALSE(buffer.push(block({{1, 0}, {2, 0}})).has_value());
  // The next block starts with a different triple, so the first one is
  // complete.
  auto complete = buffer.push(block({{3, 0}, {4, 0}, {4, 1}}));
  EXPECT_THAT(toVector(complete), ::testing::ElementsAre(10, 20));
  // Empty blocks are ignored.
  EXPECT_FALSE(buffer.push(block({})).has_value());
  // A block that consists only of rows that continue the buffered block
  // completes nothing.
  EXPECT_FALSE(buffer.push(block({{4, 2}, {4, 3}})).has_value());
  // A block that partially continues the buffered block.
  complete = buffer.push(block({{4, 4}, {5, 0}, {6, 0}}));
  EXPECT_THAT(toVector(complete),
              ::testing::ElementsAre(30, 40, 41, 42, 43, 44));
  auto last = buffer.finish();
  EXPECT_THAT(toVector(last), ::testing::ElementsAre(50, 60));
  EXPECT_FALSE(buffer.finish().has_value());

  // Nothing pushed at all.
  TripleAlignedBlockBuffer<IdTable> emptyBuffer;
  EXPECT_FALSE(emptyBuffer.finish().has_value());
}

// _____________________________________________________________________________
TEST(CompressedRelationWriter, makeLargeRelationMetadata) {
  auto md = CompressedRelationWriter::makeLargeRelationMetadata(V(7), 10, 5);
  EXPECT_EQ(md.col0Id_, V(7));
  EXPECT_EQ(md.numRows_, 10);
  EXPECT_FLOAT_EQ(md.multiplicityCol1_, 2.0f);
  EXPECT_FLOAT_EQ(md.multiplicityCol2_, 2.0f);
  EXPECT_EQ(md.offsetInBlock_, std::numeric_limits<uint64_t>::max());
  // All IDs of column 1 are distinct, so the relation is functional.
  EXPECT_FLOAT_EQ(
      CompressedRelationWriter::makeLargeRelationMetadata(V(7), 10, 10)
          .multiplicityCol1_,
      1.0f);
  EXPECT_ANY_THROW(
      CompressedRelationWriter::makeLargeRelationMetadata(V(7), 0, 0));
}
