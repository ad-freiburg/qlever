// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "index/SubBlockMetadata.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {
using SubBlock = SubBlockTable::SubBlock;
using PermutedTriple = CompressedBlockMetadata::PermutedTriple;
using ad_utility::testing::IntId;
using ad_utility::testing::VocabId;

// Shorthand for an `Id` with the given bits.
Id B(uint64_t bits) { return Id::fromBits(bits); }

// Create a sub-block with the given first triple, number of rows, compressed
// sizes, and the remaining fields.
SubBlock makeSubBlock(std::array<Id, 3> firstTriple, size_t numRows,
                      std::vector<size_t> compressedSizes,
                      uint8_t numColumnsSharedWithNext = 0,
                      bool containsDuplicates = false) {
  return {firstTriple, numRows, std::move(compressedSizes),
          numColumnsSharedWithNext, containsDuplicates};
}

// Check that all sub-blocks of `table` decode to the given `blocks` (which have
// been added to the table in this order).
void expectTableContains(
    const SubBlockTable& table,
    const std::vector<std::vector<SubBlock>>& blocks,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ASSERT_EQ(table.numBlocks(), blocks.size());
  size_t subBlockIndex = 0;
  for (size_t blockIndex = 0; blockIndex < blocks.size(); ++blockIndex) {
    const auto& subBlocks = blocks[blockIndex];
    EXPECT_EQ(table.subBlocksOfBlock(blockIndex),
              (std::pair{subBlockIndex, subBlockIndex + subBlocks.size()}));
    size_t offsetOfColumn0 = 0;
    for (const auto& expected : subBlocks) {
      EXPECT_EQ(table.blockOfSubBlock(subBlockIndex), blockIndex);
      EXPECT_EQ(table.getWithCompressedSizes(subBlockIndex), expected);
      auto decoded = table.get(subBlockIndex);
      EXPECT_EQ(decoded.firstTriple_, expected.firstTriple_);
      EXPECT_EQ(decoded.numRows_, expected.numRows_);
      EXPECT_EQ(decoded.numColumnsSharedWithNext_,
                expected.numColumnsSharedWithNext_);
      EXPECT_EQ(decoded.containsDuplicatesWithDifferentGraphs_,
                expected.containsDuplicatesWithDifferentGraphs_);
      EXPECT_EQ(table.getOffsetAndCompressedSize(subBlockIndex, 0),
                (std::pair{offsetOfColumn0, expected.compressedSizes_[0]}));
      offsetOfColumn0 += expected.compressedSizes_[0];
      ++subBlockIndex;
    }
  }
  EXPECT_EQ(table.numSubBlocks(), subBlockIndex);
}

// A table with three blocks (with 3, 1, and 150 sub-blocks), the blocks have
// two columns.
std::vector<std::vector<SubBlock>> makeTestBlocks() {
  std::vector<std::vector<SubBlock>> blocks;
  blocks.push_back(
      {makeSubBlock({VocabId(3), VocabId(17), IntId(-3)}, 4096, {1000, 17}, 2,
                    false),
       makeSubBlock({VocabId(3), VocabId(18), IntId(12)}, 4000, {1234, 0}, 1,
                    true),
       makeSubBlock({VocabId(3), VocabId(4000), IntId(0)}, 13, {12, 3})});
  blocks.push_back(
      {makeSubBlock({VocabId(4), VocabId(5), VocabId(6)}, 1, {8, 8})});
  std::vector<SubBlock> largeBlock;
  for (size_t i = 0; i < 150; ++i) {
    largeBlock.push_back(
        makeSubBlock({VocabId(5), VocabId(i * 1000), VocabId(i % 7)}, 4096 + i,
                     {i, 2 * i}, static_cast<uint8_t>(i % 3), i % 2 == 0));
  }
  blocks.push_back(std::move(largeBlock));
  return blocks;
}

// Create the table for the given `blocks`.
SubBlockTable makeTable(const std::vector<std::vector<SubBlock>>& blocks,
                        size_t numColumns = 2) {
  SubBlockTable table{numColumns};
  for (const auto& block : blocks) {
    table.addBlock(block);
  }
  return table;
}
}  // namespace

// _____________________________________________________________________________
TEST(SubBlockTable, empty) {
  SubBlockTable table;
  EXPECT_TRUE(table.empty());
  EXPECT_EQ(table.numBlocks(), 0);
  EXPECT_EQ(table.numSubBlocks(), 0);
  EXPECT_EQ(table, SubBlockTable{});
}

// _____________________________________________________________________________
TEST(SubBlockTable, addAndGet) {
  auto blocks = makeTestBlocks();
  auto table = makeTable(blocks);
  EXPECT_FALSE(table.empty());
  EXPECT_EQ(table.numColumns(), 2);
  expectTableContains(table, blocks);

  // The offsets of the second column.
  EXPECT_EQ(table.getOffsetAndCompressedSize(1, 1), (std::pair{17ul, 0ul}));
  EXPECT_EQ(table.getOffsetAndCompressedSize(2, 1), (std::pair{17ul, 3ul}));

  // The packed representation is much smaller than the unpacked one.
  EXPECT_LT(table.getMemoryUsageInBytes(), 154 * 20);

  // Wrong usage.
  EXPECT_ANY_THROW(table.addBlock({}));
  EXPECT_ANY_THROW(
      table.addBlock(std::vector{makeSubBlock({B(1), B(2), B(3)}, 1, {1})}));
  EXPECT_ANY_THROW(table.addBlock(
      std::vector{makeSubBlock({B(1), B(2), B(3)}, 1, {1, 1}, 3)}));
  EXPECT_ANY_THROW(table.getOffsetAndCompressedSize(0, 2));
  EXPECT_ANY_THROW(table.blockOfSubBlock(table.numSubBlocks()));
}

// _____________________________________________________________________________
TEST(SubBlockTable, extremeValues) {
  // Values that require 0 and 64 bits.
  constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
  std::vector<std::vector<SubBlock>> blocks;
  blocks.push_back({makeSubBlock({B(0), B(max), B(42)}, 0, {0, max, 0}),
                    makeSubBlock({B(max), B(0), B(42)}, uint64_t{1} << 40,
                                 {max, 0, 1}, 2, true)});
  // Blocks with a single sub-block, s.t. the records are not aligned to words.
  for (uint64_t i = 0; i < 70; ++i) {
    blocks.push_back({makeSubBlock({B(i), B(i * i), B(max - i)}, i,
                                   {i, i + 1, i % 5}, i % 3, i % 2)});
  }
  auto table = makeTable(blocks, 3);
  expectTableContains(table, blocks);
}

// _____________________________________________________________________________
TEST(SubBlockTable, serialization) {
  auto blocks = makeTestBlocks();
  auto table = makeTable(blocks);
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << table;
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::move(writer).data()};
  SubBlockTable deserialized;
  reader >> deserialized;
  EXPECT_EQ(deserialized, table);
  EXPECT_NE(deserialized, SubBlockTable{});
  // The samples for `blockOfSubBlock` are recomputed.
  expectTableContains(deserialized, blocks);
}

namespace {
// Create a block with the given first and last triple, and default values for
// all other members.
CompressedBlockMetadata makeBlock(PermutedTriple first, PermutedTriple last,
                                  size_t blockIndex, size_t numRows = 10,
                                  bool containsDuplicates = false) {
  using O = CompressedBlockMetadata::OffsetAndCompressedSize;
  std::vector<O> offsets{O{1000 * static_cast<off_t>(blockIndex), 100},
                         O{1000 * static_cast<off_t>(blockIndex) + 100, 50}};
  return {{std::move(offsets), numRows, first, last,
           std::vector{VocabId(blockIndex)}, containsDuplicates},
          blockIndex};
}

// Create the sub-blocks for the two blocks of `makeTestPermutation()`, with
// the given sub-blocks for the first block, and a single sub-block for the
// second block.
SubBlockTable makeSubBlockTable(std::vector<SubBlock> firstBlockSubBlocks) {
  SubBlockTable table{2};
  table.addBlock(std::move(firstBlockSubBlocks));
  table.addBlock(std::vector{makeSubBlock({B(30), B(0), B(0)}, 7, {100, 50})});
  return table;
}

// Return two blocks, the first one has three sub-blocks, the second one has a
// single sub-block.
BlocksAndSubBlocks makeTestPermutation() {
  BlocksAndSubBlocks result;
  result.blocks_.push_back(makeBlock({B(10), B(10), B(10), B(1)},
                                     {B(20), B(5), B(5), B(2)}, 0, 9, true));
  result.blocks_.push_back(
      makeBlock({B(30), B(0), B(0), B(1)}, {B(30), B(4), B(4), B(1)}, 1, 7));
  // The last triple of the first sub-block shares no columns with the first
  // triple of the second sub-block, and the second shares two columns with the
  // third.
  result.subBlocks_ = makeSubBlockTable(
      std::vector{makeSubBlock({B(10), B(10), B(10)}, 2, {30, 20}, 0, false),
                  makeSubBlock({B(12), B(0), B(7)}, 4, {50, 10}, 2, true),
                  makeSubBlock({B(12), B(0), B(9)}, 3, {20, 20}, 0, false)});
  return result;
}

// Check that both bounds of the `range` are `nullopt`.
void expectUnboundedRange(
    const SubBlockTripleRange& range,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_FALSE(range.begin_.has_value());
  EXPECT_FALSE(range.end_.has_value());
}

// The largest bits of an `Id`.
constexpr uint64_t maxBits = Id::max().getBits();
}  // namespace

// _____________________________________________________________________________
TEST(PermutationBlockMetadata, withoutSubBlocks) {
  auto blocks = makeTestPermutation().blocks_;
  PermutationBlockMetadata metadata{blocks};
  EXPECT_EQ(metadata.numBlocks(), 2);
  EXPECT_EQ(metadata.numSubBlocks(), 2);
  EXPECT_TRUE(metadata.subBlockTable().empty());
  for (size_t i = 0; i < 2; ++i) {
    const auto& block = blocks[i];
    auto subBlock = metadata.subBlock(i);
    EXPECT_EQ(
        subBlock,
        (SubBlockMetadata{block.firstTriple_, block.lastTriple_, block.numRows_,
                          block.containsDuplicatesWithDifferentGraphs_, i, i, 1,
                          &metadata}));
    EXPECT_EQ(subBlock.permutationMetadata_, &metadata);
    EXPECT_EQ(subBlock.graphInfo(), block.graphInfo_);
    EXPECT_EQ(&subBlock.completeBlock(), &metadata.blocks().at(i));
    EXPECT_EQ(metadata.subBlocksOfBlock(i), (std::pair{i, i + 1}));
    EXPECT_EQ(metadata.getOffsetAndCompressedSize(subBlock, 1),
              block.getOffsetAndCompressedSizeForColumn(1));
    expectUnboundedRange(metadata.getTripleRange(subBlock));
  }
  EXPECT_ANY_THROW(metadata.subBlock(2));
  EXPECT_ANY_THROW(metadata.subBlocksOfBlock(2));
}

// _____________________________________________________________________________
TEST(PermutationBlockMetadata, withSubBlocks) {
  auto permutation = makeTestPermutation();
  PermutationBlockMetadata metadata{permutation};
  EXPECT_EQ(metadata.numBlocks(), 2);
  EXPECT_EQ(metadata.numSubBlocks(), 4);
  EXPECT_EQ(metadata.subBlocksOfBlock(0), (std::pair{0ul, 3ul}));
  EXPECT_EQ(metadata.subBlocksOfBlock(1), (std::pair{3ul, 4ul}));

  // The first sub-block has the first triple of the block. Its last triple is
  // derived from the next sub-block, which starts with `12 0 7`, and with which
  // it shares no columns, so the last triple has `11` in the first column.
  const auto& first = permutation.blocks_[0];
  EXPECT_EQ(metadata.subBlock(0),
            (SubBlockMetadata{first.firstTriple_,
                              {B(11), B(maxBits), B(maxBits), Id::max()},
                              2,
                              false,
                              0,
                              0}));
  // The second sub-block shares two columns with the third one (which starts
  // with `12 0 9`), so the last triple is `12 0 8`. The first triple has the
  // smallest possible graph.
  EXPECT_EQ(metadata.subBlock(1),
            (SubBlockMetadata{{B(12), B(0), B(7), Id::min()},
                              {B(12), B(0), B(8), Id::max()},
                              4,
                              true,
                              0,
                              1}));
  // The last sub-block of a block has the last triple of the block.
  EXPECT_EQ(
      metadata.subBlock(2),
      (SubBlockMetadata{
          {B(12), B(0), B(9), Id::min()}, first.lastTriple_, 3, false, 0, 2}));
  // A block with a single sub-block.
  const auto& second = permutation.blocks_[1];
  EXPECT_EQ(metadata.subBlock(3),
            (SubBlockMetadata{second.firstTriple_, second.lastTriple_, 7, false,
                              1, 3}));

  // All sub-blocks have the graph info of their block.
  for (size_t i = 0; i < 4; ++i) {
    auto subBlock = metadata.subBlock(i);
    EXPECT_EQ(subBlock.graphInfo(),
              permutation.blocks_.at(subBlock.blockIndex_).graphInfo_);
  }

  // The offsets in the file.
  using O = CompressedBlockMetadata::OffsetAndCompressedSize;
  auto offsetOfSubBlock = [&metadata](size_t subBlockIndex, size_t column) {
    return metadata.getOffsetAndCompressedSize(metadata.subBlock(subBlockIndex),
                                               column);
  };
  EXPECT_EQ(offsetOfSubBlock(0, 0), (O{0, 30}));
  EXPECT_EQ(offsetOfSubBlock(1, 0), (O{30, 50}));
  EXPECT_EQ(offsetOfSubBlock(2, 1), (O{100 + 30, 20}));
  // Several sub-blocks at once.
  auto subBlocks12 = metadata.subBlock(1);
  subBlocks12.mergeWithNext(metadata.subBlock(2));
  EXPECT_EQ(subBlocks12.numSubBlocks_, 2);
  EXPECT_EQ(subBlocks12.numRows_, 7);
  EXPECT_TRUE(subBlocks12.containsDuplicatesWithDifferentGraphs_);
  EXPECT_EQ(subBlocks12.lastTriple_, first.lastTriple_);
  EXPECT_EQ(metadata.getOffsetAndCompressedSize(subBlocks12, 0), (O{30, 70}));
  auto allSubBlocks = metadata.subBlock(0);
  allSubBlocks.mergeWithNext(subBlocks12);
  EXPECT_EQ(metadata.getOffsetAndCompressedSize(allSubBlocks, 1),
            first.getOffsetAndCompressedSizeForColumn(1));
  EXPECT_EQ(offsetOfSubBlock(3, 1),
            second.getOffsetAndCompressedSizeForColumn(1));

  // The ranges of triples for the `LocatedTriples`.
  auto range = metadata.getTripleRange(metadata.subBlock(0));
  EXPECT_FALSE(range.begin_.has_value());
  EXPECT_THAT(range.end_, ::testing::Optional(std::array{B(12), B(0), B(7)}));
  range = metadata.getTripleRange(subBlocks12);
  EXPECT_THAT(range.begin_, ::testing::Optional(std::array{B(12), B(0), B(7)}));
  EXPECT_FALSE(range.end_.has_value());
  EXPECT_TRUE(range.contains({B(12), B(0), B(7)}));
  EXPECT_TRUE(range.contains({B(100), B(0), B(0)}));
  EXPECT_FALSE(range.contains({B(12), B(0), B(6)}));
  range = metadata.getTripleRange(metadata.subBlock(1));
  EXPECT_TRUE(range.contains({B(12), B(0), B(8)}));
  EXPECT_FALSE(range.contains({B(12), B(0), B(9)}));

  // The sub-blocks can only be merged with their direct successor in the same
  // block.
  EXPECT_FALSE(metadata.subBlock(0).canBeMergedWith(metadata.subBlock(2)));
  EXPECT_FALSE(metadata.subBlock(2).canBeMergedWith(metadata.subBlock(3)));
  EXPECT_FALSE(metadata.subBlock(1).canBeMergedWith(metadata.subBlock(1)));
  EXPECT_FALSE(SubBlockMetadata{}.canBeMergedWith(SubBlockMetadata{}));
  auto subBlock0 = metadata.subBlock(0);
  EXPECT_ANY_THROW(subBlock0.mergeWithNext(metadata.subBlock(3)));
}

// _____________________________________________________________________________
TEST(PermutationBlockMetadata, lastTripleWithBorrow) {
  // If the first triple of the next sub-block has a zero in the column after
  // the shared ones, the bound has to borrow from the previous column.
  BlocksAndSubBlocks permutation{
      {makeBlock({B(1), B(1), B(1), B(0)}, {B(5), B(5), B(5), B(0)}, 0)},
      SubBlockTable{2}};
  permutation.subBlocks_.addBlock(
      std::vector{makeSubBlock({B(1), B(1), B(1)}, 5, {1, 1}, 1),
                  makeSubBlock({B(1), B(3), B(0)}, 5, {1, 1}, 0)});
  PermutationBlockMetadata metadata{std::move(permutation)};
  EXPECT_EQ(metadata.subBlock(0).lastTriple_,
            (PermutedTriple{B(1), B(2), B(maxBits), Id::max()}));
}

// _____________________________________________________________________________
TEST(PermutationBlockMetadata, withAugmentedBlocks) {
  auto permutation = makeTestPermutation();
  PermutationBlockMetadata metadata{permutation};

  // Widen the bounds of the blocks, add a block that only consists of
  // `LocatedTriples`, and mark the first block as updated.
  auto blocks = permutation.blocks_;
  blocks[0].firstTriple_ = {B(9), B(0), B(0), B(0)};
  blocks[0].containsDuplicatesWithDifferentGraphs_ = false;
  blocks.push_back(CompressedBlockMetadata{
      {std::nullopt, 0, PermutedTriple{B(40), B(0), B(0), B(0)},
       PermutedTriple{B(41), B(0), B(0), B(0)}, std::nullopt, true},
      2});
  auto augmented = metadata.withAugmentedBlocks(blocks, {true, false, true});
  EXPECT_EQ(augmented.numBlocks(), 3);
  EXPECT_EQ(augmented.numSubBlocks(), 5);
  EXPECT_EQ(&augmented.subBlockTable(), &metadata.subBlockTable());

  // The first sub-block has the widened first triple, and the bounds of the
  // last triples of the sub-blocks of the updated block are the first triples
  // of the next sub-blocks (see `PermutationBlockMetadata::blocksWithUpdates_`
  // for the reason).
  EXPECT_EQ(augmented.subBlock(0),
            (SubBlockMetadata{blocks[0].firstTriple_,
                              {B(12), B(0), B(7), Id::min()},
                              2,
                              false,
                              0,
                              0}));
  EXPECT_EQ(augmented.subBlock(1).lastTriple_,
            (PermutedTriple{B(12), B(0), B(9), Id::min()}));
  // The duplicates of the sub-blocks of an updated block are those of the
  // block.
  EXPECT_TRUE(augmented.subBlock(1).containsDuplicatesWithDifferentGraphs_);
  EXPECT_FALSE(augmented.subBlock(2).containsDuplicatesWithDifferentGraphs_);
  // The block without updates is unchanged.
  EXPECT_EQ(augmented.subBlock(3), metadata.subBlock(3));
  // The block that only consists of `LocatedTriples`.
  auto last = augmented.subBlock(4);
  EXPECT_EQ(last, (SubBlockMetadata{blocks[2].firstTriple_,
                                    blocks[2].lastTriple_, 0, true, 2, 4}));
  EXPECT_EQ(augmented.getOffsetAndCompressedSize(last, 0),
            (CompressedBlockMetadata::OffsetAndCompressedSize{0, 0}));
  expectUnboundedRange(augmented.getTripleRange(last));

  // Too many or too few blocks.
  EXPECT_ANY_THROW(metadata.withAugmentedBlocks({}, {}));
}

// _____________________________________________________________________________
TEST(SubBlockMetadataSpan, iteration) {
  auto permutation = makeTestPermutation();
  PermutationBlockMetadata metadata{permutation};
  auto span = metadata.subBlocks();
  ASSERT_EQ(span.size(), 4);
  EXPECT_FALSE(span.empty());
  std::vector<SubBlockMetadata> materialized;
  for (size_t i = 0; i < 4; ++i) {
    materialized.push_back(metadata.subBlock(i));
  }
  EXPECT_THAT(span, ::testing::ElementsAreArray(materialized));
  EXPECT_EQ(span.front(), materialized.front());
  EXPECT_EQ(span.back(), materialized.back());
  EXPECT_EQ(span[2], materialized[2]);
  EXPECT_EQ(span.begin()->numRows_, materialized[0].numRows_);
  EXPECT_EQ(span.end() - span.begin(), 4);
  EXPECT_EQ(*(span.begin() + 3), materialized[3]);
  EXPECT_EQ(span.begin()[1], materialized[1]);
  auto it = span.end();
  --it;
  EXPECT_EQ(*it, materialized[3]);
  EXPECT_LT(span.begin(), it);

  // Subspans.
  EXPECT_THAT(span.subspan(1),
              ::testing::ElementsAreArray(materialized.begin() + 1,
                                          materialized.end()));
  EXPECT_THAT(span.subspan(1, 2),
              ::testing::ElementsAre(materialized[1], materialized[2]));
  EXPECT_TRUE(span.subspan(4).empty());
  EXPECT_ANY_THROW(span.subspan(5));
  EXPECT_ANY_THROW(span.subspan(2, 3));

  // A span of materialized values.
  SubBlockMetadataSpan fromVector{materialized};
  EXPECT_THAT(fromVector, ::testing::ElementsAreArray(materialized));
  EXPECT_THAT(fromVector.subspan(3), ::testing::ElementsAre(materialized[3]));
  // The values of a span of the `PermutationBlockMetadata` point to it.
  EXPECT_EQ(span.front().permutationMetadata_, &metadata);

  // A subrange and its conversion back to a span.
  SubBlockMetadataRange range{span.begin() + 1, span.end()};
  SubBlockMetadataSpan fromRange{range};
  EXPECT_THAT(fromRange, ::testing::ElementsAreArray(materialized.begin() + 1,
                                                     materialized.end()));
  // The default-constructed span is empty.
  EXPECT_TRUE(SubBlockMetadataSpan{}.empty());
}

// _____________________________________________________________________________
TEST(SubBlockMetadata, consistencyAndOutput) {
  SubBlockMetadata subBlock{
      {B(1), B(2), B(3), B(4)}, {B(1), B(2), B(5), B(4)}, 3, false, 0, 0};
  EXPECT_FALSE(subBlock.containsInconsistentTriples(2));
  EXPECT_TRUE(subBlock.containsInconsistentTriples(3));
  SubBlockMetadata next{
      {B(1), B(3), B(0), B(4)}, {B(1), B(3), B(5), B(4)}, 3, false, 0, 1};
  EXPECT_TRUE(subBlock.isConsistentWith(next, 1));
  EXPECT_FALSE(subBlock.isConsistentWith(next, 2));
  // Without `permutationMetadata_` there is no graph info.
  EXPECT_FALSE(subBlock.graphInfo().has_value());
  EXPECT_ANY_THROW(subBlock.completeBlock());
  std::ostringstream str;
  str << subBlock;
  EXPECT_THAT(str.str(), ::testing::HasSubstr("sub-block index: 0"));
}

// _____________________________________________________________________________
TEST(PermutationBlockMetadata, checkInvariants) {
  // Valid metadata with and without sub-blocks.
  auto permutation = makeTestPermutation();
  EXPECT_NO_THROW(PermutationBlockMetadata{permutation}.checkInvariants());
  EXPECT_NO_THROW(
      PermutationBlockMetadata{permutation.blocks_}.checkInvariants());

  // Create the metadata (which already checks the invariants if expensive
  // checks are enabled), and check its invariants.
  auto createAndCheck = [](const BlocksAndSubBlocks& permutation) {
    PermutationBlockMetadata{permutation}.checkInvariants();
  };

  // Two blocks in the wrong order.
  auto unsortedBlocks = permutation;
  std::swap(unsortedBlocks.blocks_[0], unsortedBlocks.blocks_[1]);
  unsortedBlocks.subBlocks_ = SubBlockTable{};
  EXPECT_ANY_THROW(createAndCheck(unsortedBlocks));

  // A block whose first triple is larger than its last triple.
  auto unsortedBounds = permutation;
  std::swap(unsortedBounds.blocks_[1].firstTriple_,
            unsortedBounds.blocks_[1].lastTriple_);
  EXPECT_ANY_THROW(createAndCheck(unsortedBounds));

  // Sub-blocks in the wrong order.
  auto unsortedSubBlocks = permutation;
  unsortedSubBlocks.subBlocks_ = makeSubBlockTable(
      std::vector{makeSubBlock({B(10), B(10), B(10)}, 2, {30, 20}),
                  makeSubBlock({B(9), B(0), B(7)}, 7, {70, 30})});
  EXPECT_ANY_THROW(createAndCheck(unsortedSubBlocks));
}
