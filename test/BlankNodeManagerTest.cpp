// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "gmock/gmock.h"
#include "util/BlankNodeManager.h"
#include "util/GTestHelpers.h"

namespace ad_utility {
// _____________________________________________________________________________
TEST(BlankNodeManager, blockAllocationAndFree) {
  BlankNodeManager bnm(0);
  EXPECT_TRUE(bnm.usedBlocksSet_.rlock()->empty());

  {
    // LocalBlankNodeManager allocates a new block
    BlankNodeManager::LocalBlankNodeManager lbnm(&bnm);
    [[maybe_unused]] uint64_t id = lbnm.getId();
    EXPECT_EQ(bnm.usedBlocksSet_.rlock()->size(), 1);
  }

  // Once the LocalBlankNodeManager is destroyed, all Blocks allocated through
  // it are freed/removed from the BlankNodeManager's set.
  EXPECT_TRUE(bnm.usedBlocksSet_.rlock()->empty());

  // Mock randomIntGenerator to let the block index generation collide.
  bnm.randBlockIndex_ = SlowRandomIntGenerator<uint64_t>(0, 1);
  [[maybe_unused]] auto _ = bnm.allocateBlock();
  for (int i = 0; i < 30; ++i) {
    auto block = bnm.allocateBlock();
    bnm.usedBlocksSet_.wlock()->erase(block.blockIdx_);
  }
}

// _____________________________________________________________________________
TEST(BlankNodeManager, LocalBlankNodeManagerGetID) {
  BlankNodeManager bnm(0);
  BlankNodeManager::LocalBlankNodeManager l(&bnm);

  // initially the LocalBlankNodeManager doesn't have any blocks
  EXPECT_EQ(l.blocks_.size(), 0);

  // A new Block is allocated, if
  // no blocks are allocated yet
  uint64_t id = l.getId();
  EXPECT_EQ(l.blocks_.size(), 1);

  // or the ids of the last block are all used
  l.blocks_.back().nextIdx_ = id + BlankNodeManager::blockSize_;
  id = l.getId();
  EXPECT_EQ(l.blocks_.size(), 2);
}

// _____________________________________________________________________________
TEST(BlankNodeManager, maxNumOfBlocks) {
  // Mock a high `minIndex_` to simulate reduced space in the `usedBlocksSet_`
  BlankNodeManager bnm(ValueId::maxIndex - 256 * BlankNodeManager::blockSize_ +
                       2);
  AD_EXPECT_THROW_WITH_MESSAGE(
      [[maybe_unused]] auto _ = bnm.allocateBlock(),
      ::testing::HasSubstr(
          "Critical high number of blank node blocks in use:"));
}

// _____________________________________________________________________________
TEST(BlankNodeManager, moveLocalBlankNodeManager) {
  // This ensures that the `blocks_` of the `LocalBlankNodeManager` are moved
  // correctly, such that they're freed/removed from the `BlankNodeManager`
  // set only once.
  BlankNodeManager bnm(0);
  EXPECT_NO_THROW({
    BlankNodeManager::LocalBlankNodeManager l1(&bnm);
    auto l2(std::move(l1));
    BlankNodeManager::LocalBlankNodeManager l3(&bnm);
    l3 = std::move(l2);
  });
  EXPECT_TRUE(bnm.usedBlocksSet_.rlock()->empty());
}

}  // namespace ad_utility
