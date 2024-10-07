// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/BlankNodeManager.h"
#include "util/Random.h"

namespace ad_utility {
TEST(BlankNodeManager, blockAllocation) {
  using Block = BlankNodeManager::Block;
  auto& bnm = globalBlankNodeManager;

  EXPECT_EQ(bnm.usedBlocksSet_.size(), 0);

  {
    Block b = bnm.allocateBlock();
    EXPECT_EQ(bnm.usedBlocksSet_.size(), 1);
  }

  // Blocks are removed from the globalBlankNodeManager once they are destroyed.
  EXPECT_EQ(bnm.usedBlocksSet_.size(), 0);
}

TEST(BlankNodeManager, LocalBlankNodeManagerGetID) {
  BlankNodeManager::LocalBlankNodeManager l;
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

}  // namespace ad_utility
