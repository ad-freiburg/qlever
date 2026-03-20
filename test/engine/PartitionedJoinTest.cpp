// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "engine/PartitionBucket.h"
#include "engine/PartitionedJoinHelpers.h"
#include "global/Id.h"
#include "index/CompressedRelation.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::testing::makeAllocator;
using namespace qlever::partitionedJoin;

namespace {

// Helper to create a CompressedBlockMetadata with specific first/last triple
// col1 values and a given number of rows. This simulates a scan with one fixed
// column (col0), so the join column is col1.
CompressedBlockMetadata makeBlock(size_t blockIdx, Id firstCol1, Id lastCol1,
                                  size_t numRows) {
  CompressedBlockMetadata block;
  block.blockIndex_ = blockIdx;
  block.numRows_ = numRows;
  // Set col0 to a fixed value (simulating a scan with fixed first column).
  block.firstTriple_.col0Id_ = Id::makeFromInt(42);
  block.firstTriple_.col1Id_ = firstCol1;
  block.firstTriple_.col2Id_ = Id::makeFromInt(0);
  block.firstTriple_.graphId_ = Id::makeFromInt(0);
  block.lastTriple_.col0Id_ = Id::makeFromInt(42);
  block.lastTriple_.col1Id_ = lastCol1;
  block.lastTriple_.col2Id_ = Id::makeFromInt(999);
  block.lastTriple_.graphId_ = Id::makeFromInt(0);
  block.containsDuplicatesWithDifferentGraphs_ = false;
  // Required for the block to be valid.
  block.offsetsAndCompressedSize_ =
      std::vector<CompressedBlockMetadataNoBlockIndex::OffsetAndCompressedSize>{
          {0, 100}, {100, 100}};
  return block;
}

// Helper struct that keeps blocks alive alongside the metadata that
// references them via span-based iterators.
struct TestScanMetadata {
  std::vector<CompressedBlockMetadata> blocks_;
  // The `ScanSpecAndBlocksAndBounds` references `blocks_` via the span.
  // We construct it lazily to ensure `blocks_` is fully constructed first.
  std::unique_ptr<CompressedRelationReader::ScanSpecAndBlocksAndBounds>
      metadata_;

  explicit TestScanMetadata(std::vector<CompressedBlockMetadata> blocks)
      : blocks_(std::move(blocks)) {
    ScanSpecification scanSpec{Id::makeFromInt(42), std::nullopt, std::nullopt};
    BlockMetadataSpan span{blocks_.data(), blocks_.size()};
    BlockMetadataRanges ranges{BlockMetadataRange{span.begin(), span.end()}};

    CompressedRelationReader::ScanSpecAndBlocks base{scanSpec,
                                                     std::move(ranges)};

    CompressedRelationReader::ScanSpecAndBlocksAndBounds::FirstAndLastTriple
        firstAndLast{blocks_.front().firstTriple_, blocks_.back().lastTriple_};

    metadata_ =
        std::make_unique<CompressedRelationReader::ScanSpecAndBlocksAndBounds>(
            std::move(base), std::move(firstAndLast));
  }

  const CompressedRelationReader::ScanSpecAndBlocksAndBounds& get() const {
    return *metadata_;
  }
};

}  // namespace

// Test: Single partition when all blocks fit within RAM budget.
TEST(PartitionedJoinHelpers, SinglePartition) {
  // 4 blocks, each 100 rows, 2 columns. Total = 4 * 100 * 2 * 8 = 6400 bytes.
  TestScanMetadata meta({
      makeBlock(0, Id::makeFromInt(1), Id::makeFromInt(10), 100),
      makeBlock(1, Id::makeFromInt(11), Id::makeFromInt(20), 100),
      makeBlock(2, Id::makeFromInt(21), Id::makeFromInt(30), 100),
      makeBlock(3, Id::makeFromInt(31), Id::makeFromInt(40), 100),
  });

  auto partitions = computePartitionBoundaries(
      meta.get(), ad_utility::MemorySize::megabytes(1), 2);

  ASSERT_EQ(partitions.size(), 1u);
  EXPECT_EQ(partitions[0].beginBlockIdx, 0u);
  EXPECT_EQ(partitions[0].endBlockIdx, 4u);
  EXPECT_EQ(partitions[0].endBlockIdxWithOverlap, 4u);
}

// Test: Multiple partitions when blocks exceed RAM budget.
TEST(PartitionedJoinHelpers, MultiplePartitions) {
  // 4 blocks, each 1000 rows, 2 columns.
  // Each block = 1000 * 2 * 8 = 16000 bytes.
  TestScanMetadata meta({
      makeBlock(0, Id::makeFromInt(1), Id::makeFromInt(10), 1000),
      makeBlock(1, Id::makeFromInt(11), Id::makeFromInt(20), 1000),
      makeBlock(2, Id::makeFromInt(21), Id::makeFromInt(30), 1000),
      makeBlock(3, Id::makeFromInt(31), Id::makeFromInt(40), 1000),
  });

  // Budget: 20000 bytes. Block size = 1000 * 2 * 8 = 16000.
  // Block 0 alone: 16000 <= 20000 ok. Block 0+1: 32000 > 20000, finalize P0.
  // Each subsequent block also forms its own partition.
  auto partitions = computePartitionBoundaries(
      meta.get(), ad_utility::MemorySize::bytes(20000), 2);

  ASSERT_EQ(partitions.size(), 4u);
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(partitions[i].beginBlockIdx, i);
    EXPECT_EQ(partitions[i].endBlockIdx, i + 1);
  }
}

// Test: Single oversized block forms its own partition.
TEST(PartitionedJoinHelpers, OversizedBlock) {
  TestScanMetadata meta({
      makeBlock(0, Id::makeFromInt(1), Id::makeFromInt(10), 100),
      makeBlock(1, Id::makeFromInt(11), Id::makeFromInt(20), 100000),
      makeBlock(2, Id::makeFromInt(21), Id::makeFromInt(30), 100),
  });

  // Budget: 1000 bytes. Block 1 is enormous but still forms its own partition.
  auto partitions = computePartitionBoundaries(
      meta.get(), ad_utility::MemorySize::bytes(1000), 2);

  ASSERT_EQ(partitions.size(), 3u);
  EXPECT_EQ(partitions[1].beginBlockIdx, 1u);
  EXPECT_EQ(partitions[1].endBlockIdx, 2u);
}

// Test: Overlap blocks when boundary values match.
TEST(PartitionedJoinHelpers, OverlapBlocks) {
  // Block 0 ends with value 10, block 1 starts with value 10.
  // So P0 should have overlap extending into block 1.
  TestScanMetadata meta({
      makeBlock(0, Id::makeFromInt(1), Id::makeFromInt(10), 1000),
      makeBlock(1, Id::makeFromInt(10), Id::makeFromInt(20), 1000),
      makeBlock(2, Id::makeFromInt(21), Id::makeFromInt(30), 1000),
  });

  // Budget allows only 1 block per partition.
  auto partitions = computePartitionBoundaries(
      meta.get(), ad_utility::MemorySize::bytes(1), 2);

  ASSERT_GE(partitions.size(), 3u);
  // P0 should have overlap extending to include block 1.
  EXPECT_EQ(partitions[0].endBlockIdx, 1u);
  EXPECT_EQ(partitions[0].endBlockIdxWithOverlap, 2u);
}

// Test: findPartition binary search.
TEST(PartitionedJoinHelpers, FindPartition) {
  std::vector<PartitionBoundary> partitions = {
      {Id::makeFromInt(1), Id::makeFromInt(10), 0, 1, 1},
      {Id::makeFromInt(11), Id::makeFromInt(20), 1, 2, 2},
      {Id::makeFromInt(21), Id::makeFromInt(30), 2, 3, 3},
  };

  // Value in first partition.
  EXPECT_EQ(findPartition(Id::makeFromInt(5), partitions), 0u);
  // Value at boundary of first partition.
  EXPECT_EQ(findPartition(Id::makeFromInt(10), partitions), 0u);
  // Value in second partition.
  EXPECT_EQ(findPartition(Id::makeFromInt(15), partitions), 1u);
  // Value in third partition.
  EXPECT_EQ(findPartition(Id::makeFromInt(25), partitions), 2u);
  // Value below all partitions.
  EXPECT_EQ(findPartition(Id::makeFromInt(0), partitions), std::nullopt);
  // Value above all partitions.
  EXPECT_EQ(findPartition(Id::makeFromInt(31), partitions), std::nullopt);
}

// Test: Empty partitions vector.
TEST(PartitionedJoinHelpers, FindPartitionEmpty) {
  std::vector<PartitionBoundary> partitions;
  EXPECT_EQ(findPartition(Id::makeFromInt(5), partitions), std::nullopt);
}

// Test: PartitionBucket basic operations.
TEST(PartitionBucketTest, BasicOperations) {
  auto alloc = makeAllocator();
  PartitionBucket bucket(3, alloc, 10);  // 3 columns, page capacity 10.

  EXPECT_TRUE(bucket.empty());
  EXPECT_EQ(bucket.numRows(), 0u);

  // Create a row and append it.
  IdTable table(3, alloc);
  table.push_back({Id::makeFromInt(5), Id::makeFromInt(1), Id::makeFromInt(2)});
  table.push_back({Id::makeFromInt(3), Id::makeFromInt(4), Id::makeFromInt(5)});
  table.push_back({Id::makeFromInt(1), Id::makeFromInt(7), Id::makeFromInt(8)});

  for (size_t i = 0; i < table.size(); ++i) {
    bucket.append(table[i]);
  }

  EXPECT_FALSE(bucket.empty());
  EXPECT_EQ(bucket.numRows(), 3u);

  // Materialize and sort by column 0.
  IdTable result = bucket.materializeAndSort(0);
  ASSERT_EQ(result.size(), 3u);
  ASSERT_EQ(result.numColumns(), 3u);

  // Should be sorted by column 0.
  EXPECT_EQ(result(0, 0), Id::makeFromInt(1));
  EXPECT_EQ(result(1, 0), Id::makeFromInt(3));
  EXPECT_EQ(result(2, 0), Id::makeFromInt(5));
}

// Test: PartitionBucket with multiple pages.
TEST(PartitionBucketTest, MultiplePages) {
  auto alloc = makeAllocator();
  PartitionBucket bucket(2, alloc, 3);  // 2 columns, page capacity 3.

  // Insert 10 rows (should create ~3-4 pages).
  for (int i = 10; i >= 1; --i) {
    IdTable row(2, alloc);
    row.push_back({Id::makeFromInt(i), Id::makeFromInt(i * 10)});
    bucket.append(row[0]);
  }

  EXPECT_EQ(bucket.numRows(), 10u);

  IdTable result = bucket.materializeAndSort(0);
  ASSERT_EQ(result.size(), 10u);

  // Should be sorted ascending by column 0.
  for (size_t i = 0; i < result.size(); ++i) {
    EXPECT_EQ(result(i, 0), Id::makeFromInt(static_cast<int>(i + 1)));
  }
}
