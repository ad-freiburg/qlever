// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "util/BlankNodeManager.h"
#include "util/Random.h"

using namespace ad_utility;

class BlankNodeManagerTest : public BlankNodeManager {
 public:
  BlankNodeManagerTest() : BlankNodeManager() {}
  BlankNodeManagerTest(SlowRandomIntGenerator<uint64_t> randomIntGenerator)
      : BlankNodeManager(randomIntGenerator) {}

  using BlankNodeManager::usedBlocksSet_;
};

class LocalBlankNodeManagerTest
    : public BlankNodeManager::LocalBlankNodeManager {
 public:
  using BlankNodeManager::LocalBlankNodeManager::blocks_;
};

TEST(blockAllocation, BlankNodeManagerTest) {
  using Block = BlankNodeManager::Block;
  BlankNodeManagerTest bnm;

  EXPECT_EQ(bnm.usedBlocksSet_.size(), 0);
  Block b = bnm.allocateBlock();
  EXPECT_EQ(bnm.usedBlocksSet_.size(), 1);
  bnm.freeBlock(b.blockIdx_);
  EXPECT_EQ(bnm.usedBlocksSet_.size(), 0);
  // double free
  EXPECT_NO_THROW(bnm.freeBlock(b.blockIdx_));
}

TEST(LocalBlankNodeManagerGetID, BlankNodeManagerTest) {
  using LocalBNM = LocalBlankNodeManagerTest;
  LocalBNM l;
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
