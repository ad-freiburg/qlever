// Copyright 2024 - 2025 The QLever Authors, in particular:
//
// 2024 Moritz Dom <domm@informatik.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "gmock/gmock.h"
#include "util/BlankNodeManager.h"
#include "util/GTestHelpers.h"
#include "util/SourceLocation.h"

namespace ad_utility {

// ____________________________________________________________________________
// Test fixture providing common infrastructure for BlankNodeManager tests.
class BlankNodeManagerTestFixture : public ::testing::Test {
 protected:
  // Helper to create a BlankNodeManager with default minIndex.
  static std::unique_ptr<BlankNodeManager> createManager(
      uint64_t minIndex = 0) {
    return std::make_unique<BlankNodeManager>(minIndex);
  }

  // Helper to create a LocalBlankNodeManager.
  static std::shared_ptr<BlankNodeManager::LocalBlankNodeManager>
  createLocalManager(BlankNodeManager* bnm) {
    return std::make_shared<BlankNodeManager::LocalBlankNodeManager>(bnm);
  }

  // Helper to get the primary blocks from a LocalBlankNodeManager.
  static auto& getPrimaryBlocks(BlankNodeManager::LocalBlankNodeManager& lbnm) {
    return lbnm.blocks_->blocks_;
  }

  // Helper to get the total number of blocks (primary + other).
  static size_t getTotalBlockCount(
      const BlankNodeManager::LocalBlankNodeManager& lbnm) {
    size_t count = lbnm.blocks_->blocks_.size();
    for (const auto& otherBlocks : lbnm.otherBlocks_) {
      count += otherBlocks->blocks_.size();
    }
    return count;
  }

  // Helper to get all block indices (from both primary and other blocks).
  static std::vector<uint64_t> getAllBlockIndices(
      const BlankNodeManager::LocalBlankNodeManager& lbnm) {
    std::vector<uint64_t> indices;
    for (const auto& block : lbnm.blocks_->blocks_) {
      indices.push_back(block.blockIdx_);
    }
    for (const auto& otherBlocks : lbnm.otherBlocks_) {
      for (const auto& block : otherBlocks->blocks_) {
        indices.push_back(block.blockIdx_);
      }
    }
    return indices;
  }

  // Helper to allocate N IDs from a LocalBlankNodeManager.
  static std::vector<uint64_t> allocateIds(
      BlankNodeManager::LocalBlankNodeManager& lbnm, size_t count) {
    std::vector<uint64_t> ids;
    ids.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      ids.push_back(lbnm.getId());
    }
    return ids;
  }

  // Helper to allocate IDs that span multiple blocks.
  static std::vector<uint64_t> allocateIdsAcrossBlocks(
      BlankNodeManager::LocalBlankNodeManager& lbnm, size_t numBlocks) {
    std::vector<uint64_t> ids;
    for (size_t i = 0; i < numBlocks; ++i) {
      // Allocate at least one ID per block.
      ids.push_back(lbnm.getId());
      // Fill the rest of the block to force allocation of next block.
      if (i < numBlocks - 1) {
        auto& blocks = getPrimaryBlocks(lbnm);
        if (!blocks.empty()) {
          blocks.back().nextIdx_ =
              blocks.back().startIdx_ + BlankNodeManager::blockSize_;
        }
      }
    }
    return ids;
  }

  // Helper to verify all IDs are contained in the LocalBlankNodeManager.
  static void verifyIdsContained(
      const BlankNodeManager::LocalBlankNodeManager& lbnm,
      const std::vector<uint64_t>& ids,
      ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(loc);
    for (uint64_t id : ids) {
      EXPECT_TRUE(lbnm.containsBlankNodeIndex(id))
          << "ID " << id << " should be contained";
    }
  }

  // Helper to verify IDs are NOT contained in the LocalBlankNodeManager.
  static void verifyIdsNotContained(
      const BlankNodeManager::LocalBlankNodeManager& lbnm,
      const std::vector<uint64_t>& ids,
      ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(loc);
    for (uint64_t id : ids) {
      EXPECT_FALSE(lbnm.containsBlankNodeIndex(id))
          << "ID " << id << " should not be contained";
    }
  }

  // Helper to get the number of used blocks.
  static size_t getUsedBlockCount(const BlankNodeManager& bnm) {
    return bnm.state_.rlock()->usedBlocksSet_.size();
  }

  // Helper to check if a specific block index is used.
  static bool isBlockUsed(const BlankNodeManager& bnm, uint64_t blockIdx) {
    return bnm.state_.rlock()->usedBlocksSet_.contains(blockIdx);
  }

  // Helper to get the number of managed UUIDs.
  static size_t getManagedUuidCount(const BlankNodeManager& bnm) {
    return bnm.state_.rlock()->managedBlockSets_.size();
  }

  // Helper to serialize a LocalBlankNodeManager.
  static std::vector<BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>
  serialize(const BlankNodeManager::LocalBlankNodeManager& lbnm) {
    return lbnm.getOwnedBlockIndices();
  }

  // Helper to deserialize into a new LocalBlankNodeManager.
  static std::shared_ptr<BlankNodeManager::LocalBlankNodeManager> deserialize(
      BlankNodeManager* bnm,
      const std::vector<
          BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>& entries) {
    auto lbnm = createLocalManager(bnm);
    lbnm->allocateBlocksFromExplicitIndices(entries);
    return lbnm;
  }

  // Helper to perform a round-trip serialization/deserialization.
  static std::shared_ptr<BlankNodeManager::LocalBlankNodeManager>
  roundTripSerialize(BlankNodeManager* bnm,
                     const BlankNodeManager::LocalBlankNodeManager& source) {
    auto entries = serialize(source);
    return deserialize(bnm, entries);
  }
};
// _____________________________________________________________________________
TEST(BlankNodeManager, blockAllocationAndFree) {
  BlankNodeManager bnm(0);
  EXPECT_TRUE(bnm.state_.rlock()->usedBlocksSet_.empty());

  {
    // LocalBlankNodeManager allocates a new block.
    BlankNodeManager::LocalBlankNodeManager lbnm(&bnm);
    [[maybe_unused]] uint64_t id = lbnm.getId();
    EXPECT_EQ(bnm.state_.rlock()->usedBlocksSet_.size(), 1);
  }

  // Once the LocalBlankNodeManager is destroyed, all Blocks allocated through.
  // it are freed/removed from the BlankNodeManager's set.
  EXPECT_TRUE(bnm.state_.rlock()->usedBlocksSet_.empty());

  // Mock randomIntGenerator to let the block index generation collide.
  bnm.state_.wlock()->randBlockIndex_ = SlowRandomIntGenerator<uint64_t>(0, 1);
  [[maybe_unused]] auto _ = bnm.allocateBlock();
  for (int i = 0; i < 30; ++i) {
    auto block = bnm.allocateBlock();
    bnm.state_.wlock()->usedBlocksSet_.erase(block.blockIdx_);
  }
}

// _____________________________________________________________________________
TEST(BlankNodeManager, LocalBlankNodeManagerGetID) {
  BlankNodeManager bnm(0);
  auto l = std::make_shared<BlankNodeManager::LocalBlankNodeManager>(&bnm);

  // Getter for the contained blocks.
  auto blocks = [&l]() -> auto& { return l->blocks_->blocks_; };
  // initially the LocalBlankNodeManager doesn't have any blocks.
  EXPECT_EQ(blocks().size(), 0);

  // A new Block is allocated, if
  // no blocks are allocated yet.
  uint64_t id = l->getId();
  EXPECT_EQ(blocks().size(), 1);
  EXPECT_TRUE(l->containsBlankNodeIndex(id));
  EXPECT_FALSE(l->containsBlankNodeIndex(id + 1));
  EXPECT_FALSE(l->containsBlankNodeIndex(id - 1));

  // or the ids of the last block are all used.
  blocks().back().nextIdx_ = id + BlankNodeManager::blockSize_;
  id = l->getId();
  EXPECT_TRUE(l->containsBlankNodeIndex(id));
  EXPECT_EQ(blocks().size(), 2);

  // The `LocalBlankNodeManager` still works when recursively merged.
  std::vector itSelf{l};
  l->mergeWith(itSelf);

  EXPECT_TRUE(l->containsBlankNodeIndex(id));
  EXPECT_TRUE(l->containsBlankNodeIndex(l->getId()));
  EXPECT_EQ(l->blocks_, l->otherBlocks_[0]);
}

// _____________________________________________________________________________
TEST(BlankNodeManager, maxNumOfBlocks) {
  // Mock a high `minIndex_` to simulate reduced space in the `usedBlocksSet_`.
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
  // correctly, such that they're freed/removed from the `BlankNodeManager`.
  // set only once.
  BlankNodeManager bnm(0);
  EXPECT_NO_THROW({
    BlankNodeManager::LocalBlankNodeManager l1(&bnm);
    auto l2(std::move(l1));
    BlankNodeManager::LocalBlankNodeManager l3(&bnm);
    l3 = std::move(l2);
  });
  EXPECT_TRUE(bnm.state_.rlock()->usedBlocksSet_.empty());
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, serializationRoundTrip) {
  auto bnm = createManager();
  auto lbnm = createLocalManager(bnm.get());

  // Allocate IDs across multiple blocks.
  auto originalIds = allocateIdsAcrossBlocks(*lbnm, 3);
  ASSERT_EQ(getPrimaryBlocks(*lbnm).size(), 3);

  auto entries = serialize(*lbnm);
  EXPECT_EQ(entries.size(), 1);  // Only primary blocks, no merged blocks.
  EXPECT_EQ(entries[0].blockIndices_.size(), 3);

  // Deserialize into a new LocalBlankNodeManager.
  auto lbnm2 = deserialize(bnm.get(), entries);

  // Verify all original IDs are contained.
  verifyIdsContained(*lbnm2, originalIds);

  // Verify block indices are preserved (now in otherBlocks_).
  EXPECT_EQ(getTotalBlockCount(*lbnm2), 3);
  auto originalIndices = getAllBlockIndices(*lbnm);
  auto restoredIndices = getAllBlockIndices(*lbnm2);
  EXPECT_EQ(originalIndices, restoredIndices);

  // Verify new IDs can still be allocated and don't conflict.
  auto newId = lbnm2->getId();
  EXPECT_TRUE(lbnm2->containsBlankNodeIndex(newId));
  // The new ID should be in a new block (primary blocks now has 1 block).
  EXPECT_EQ(getPrimaryBlocks(*lbnm2).size(), 1);
  EXPECT_EQ(getTotalBlockCount(*lbnm2), 4);
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, explicitBlockAllocation) {
  auto bnm = createManager();

  // Allocate specific block indices.
  auto block1 = bnm->allocateExplicitBlock(5);
  EXPECT_EQ(block1.blockIdx_, 5);
  EXPECT_EQ(block1.startIdx_, 5 * BlankNodeManager::blockSize_);
  EXPECT_EQ(block1.nextIdx_, 5 * BlankNodeManager::blockSize_);
  EXPECT_TRUE(isBlockUsed(*bnm, 5));

  auto block2 = bnm->allocateExplicitBlock(10);
  EXPECT_EQ(block2.blockIdx_, 10);
  EXPECT_TRUE(isBlockUsed(*bnm, 10));

  // Verify we can't allocate the same block twice.
  auto allocateExplicitly = [&]() {
    [[maybe_unused]] auto blocks = bnm->allocateExplicitBlock(5);
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      allocateExplicitly(),
      ::testing::HasSubstr("has previously already been allocated"));
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, uuidManagement) {
  auto bnm = createManager();

  // Create multiple LocalBlankNodeManagers.
  auto lbnm1 = createLocalManager(bnm.get());
  auto lbnm2 = createLocalManager(bnm.get());
  auto lbnm3 = createLocalManager(bnm.get());

  // Allocate some IDs to create blocks.
  allocateIds(*lbnm1, 5);
  allocateIds(*lbnm2, 3);
  allocateIds(*lbnm3, 7);

  // Get UUIDs.
  auto entries1 = serialize(*lbnm1);
  auto entries2 = serialize(*lbnm2);
  auto entries3 = serialize(*lbnm3);

  // Verify each has a unique UUID.
  EXPECT_NE(entries1[0].uuid_, entries2[0].uuid_);
  EXPECT_NE(entries1[0].uuid_, entries3[0].uuid_);
  EXPECT_NE(entries2[0].uuid_, entries3[0].uuid_);

  // All three UUIDs should be registered.
  EXPECT_EQ(getManagedUuidCount(*bnm), 3);

  // Destroy one LocalBlankNodeManager.
  lbnm1.reset();

  // UUID count should decrease (the destructor of the `Blocks` struct blocks.
  // until it has been deleted).
  EXPECT_EQ(getManagedUuidCount(*bnm), 2);
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, sharedBlockSetViaUuid) {
  auto bnm = createManager();
  auto lbnm1 = createLocalManager(bnm.get());

  // Allocate IDs across multiple blocks.
  allocateIdsAcrossBlocks(*lbnm1, 2);
  auto entries = serialize(*lbnm1);

  // Deserialize the same data into two different LocalBlankNodeManagers.
  auto lbnm2 = deserialize(bnm.get(), entries);
  auto lbnm3 = deserialize(bnm.get(), entries);

  // Both should reference the same underlying Blocks (same shared_ptr).
  // The deserialized blocks are in otherBlocks_, and they share the same UUID.
  auto entries2 = serialize(*lbnm2);
  auto entries3 = serialize(*lbnm3);
  // Serialize returns primary first (empty), then otherBlocks.
  // So the deserialized blocks are at index 1.
  ASSERT_EQ(entries2.size(), 2);  // Empty primary + 1 from otherBlocks.
  ASSERT_EQ(entries3.size(), 2);
  EXPECT_TRUE(entries2[0].blockIndices_.empty());  // Primary is empty.
  EXPECT_TRUE(entries3[0].blockIndices_.empty());
  // The UUIDs of the deserialized blocks (at index 1) should match the
  // original.
  EXPECT_EQ(entries2[1].uuid_, entries[0].uuid_);
  EXPECT_EQ(entries3[1].uuid_, entries[0].uuid_);

  // Block indices should only be allocated once in usedBlocksSet_.
  EXPECT_EQ(getUsedBlockCount(*bnm), 2);

  // Verify both can see the blocks (now in otherBlocks_).
  EXPECT_EQ(getTotalBlockCount(*lbnm2), 2);
  EXPECT_EQ(getTotalBlockCount(*lbnm3), 2);
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, deserializationWithMergedBlocks) {
  auto bnm = createManager();

  // Create LocalBlankNodeManager A with some blocks.
  auto lbnmA = createLocalManager(bnm.get());
  auto idsA = allocateIdsAcrossBlocks(*lbnmA, 2);

  // Create LocalBlankNodeManager B with other blocks.
  auto lbnmB = createLocalManager(bnm.get());
  auto idsB = allocateIdsAcrossBlocks(*lbnmB, 2);

  // Merge B into A.
  std::vector managers{lbnmB};
  lbnmA->mergeWith(managers);

  // Serialize A (should have multiple OwnedBlocksEntry elements).
  auto entries = serialize(*lbnmA);
  EXPECT_EQ(entries.size(), 2);  // Primary blocks + one merged set.
  EXPECT_EQ(entries[0].blockIndices_.size(), 2);  // A's blocks.
  EXPECT_EQ(entries[1].blockIndices_.size(), 2);  // B's blocks.

  // Deserialize into a new LocalBlankNodeManager C.
  auto lbnmC = deserialize(bnm.get(), entries);

  // Verify C has all blocks in otherBlocks (after deserialization).
  // We verify indirectly by serializing and checking the number of sets.
  auto entriesC = serialize(*lbnmC);
  // entriesC will have 3 entries: 1 empty primary + 2 from otherBlocks.
  EXPECT_EQ(entriesC.size(), 3);
  EXPECT_TRUE(entriesC[0].blockIndices_.empty());  // Primary is empty.
  EXPECT_EQ(entriesC[1].blockIndices_.size(), 2);  // First set.
  EXPECT_EQ(entriesC[2].blockIndices_.size(), 2);  // Second set.
  EXPECT_EQ(getPrimaryBlocks(*lbnmC).size(), 0);   // Primary blocks is empty.
  EXPECT_EQ(getTotalBlockCount(*lbnmC), 4);        // Total of 4 blocks.

  // Verify all IDs from both A and B are contained in C.
  verifyIdsContained(*lbnmC, idsA);
  verifyIdsContained(*lbnmC, idsB);
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, idAllocationAfterDeserialization) {
  auto bnm = createManager();
  auto lbnm1 = createLocalManager(bnm.get());

  // Allocate some IDs (but not filling the whole block).
  auto ids = allocateIds(*lbnm1, 5);
  auto entries = serialize(*lbnm1);

  auto lbnm2 = deserialize(bnm.get(), entries);

  // The next ID should come from a NEW block in primary blocks
  // (deserialized blocks are in otherBlocks_).
  EXPECT_EQ(getPrimaryBlocks(*lbnm2).size(),
            0);  // Primary blocks empty before getId.
  [[maybe_unused]] auto newId = lbnm2->getId();
  EXPECT_EQ(getPrimaryBlocks(*lbnm2).size(), 1);  // New block in primary.
  EXPECT_EQ(getTotalBlockCount(*lbnm2), 2);       // 1 deserialized + 1 new.

  // The containsBlankNodeIndex test doesn't apply anymore since we don't
  // have direct access to the partially filled blocks in otherBlocks_.
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, emptyLocalBlankNodeManagerPrecondition) {
  auto bnm = createManager();
  auto lbnm = createLocalManager(bnm.get());

  // Allocate some IDs to make it non-empty.
  allocateIds(*lbnm, 5);

  // Create some dummy entries to try to deserialize.
  BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry entry;
  entry.uuid_ = boost::uuids::random_generator()();
  entry.blockIndices_ = {1, 2, 3};
  std::vector<BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>
      entries{entry};

  // Attempt to call allocateBlocksFromExplicitIndices on non-empty manager.
  AD_EXPECT_THROW_WITH_MESSAGE(
      lbnm->allocateBlocksFromExplicitIndices(entries),
      ::testing::HasSubstr(
          "Explicit reserving of blank node blocks is only allowed "
          "for empty"));
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, blockSetCleanup) {
  auto bnm = createManager();

  boost::uuids::uuid savedUuid;
  std::vector<uint64_t> blockIndices;

  {
    auto lbnm = createLocalManager(bnm.get());
    auto ids = allocateIdsAcrossBlocks(*lbnm, 3);
    auto entries = serialize(*lbnm);

    savedUuid = entries[0].uuid_;
    blockIndices = entries[0].blockIndices_;

    // Verify UUID is registered and blocks are used.
    EXPECT_EQ(getManagedUuidCount(*bnm), 1);
    EXPECT_EQ(getUsedBlockCount(*bnm), 3);
    for (auto idx : blockIndices) {
      EXPECT_TRUE(isBlockUsed(*bnm, idx));
    }
  }  // lbnm is destroyed here.

  // After destruction, UUID should be cleaned up and blocks freed.
  EXPECT_EQ(getManagedUuidCount(*bnm), 0);
  EXPECT_EQ(getUsedBlockCount(*bnm), 0);
  for (auto idx : blockIndices) {
    EXPECT_FALSE(isBlockUsed(*bnm, idx));
  }
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, explicitAndRandomAllocationCoexistence) {
  auto bnm = createManager();

  // Allocate some blocks explicitly.
  auto block1 = bnm->allocateExplicitBlock(100);
  auto block2 = bnm->allocateExplicitBlock(200);
  EXPECT_EQ(getUsedBlockCount(*bnm), 2);

  // Allocate some blocks randomly.
  auto randomBlock1 = bnm->allocateBlock();
  auto randomBlock2 = bnm->allocateBlock();
  EXPECT_EQ(getUsedBlockCount(*bnm), 4);

  // Verify no conflicts (all block indices should be different).
  std::set<uint64_t> allBlocks{block1.blockIdx_, block2.blockIdx_,
                               randomBlock1.blockIdx_, randomBlock2.blockIdx_};
  EXPECT_EQ(allBlocks.size(), 4);

  // All should be marked as used.
  EXPECT_TRUE(isBlockUsed(*bnm, block1.blockIdx_));
  EXPECT_TRUE(isBlockUsed(*bnm, block2.blockIdx_));
  EXPECT_TRUE(isBlockUsed(*bnm, randomBlock1.blockIdx_));
  EXPECT_TRUE(isBlockUsed(*bnm, randomBlock2.blockIdx_));
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, uuidCollisionHandling) {
  auto bnm = createManager();

  // We can't easily mock the UUID generator without modifying the production
  // code, but we can at least verify that creating many LocalBlankNodeManagers
  // doesn't cause issues and all get unique UUIDs.
  std::vector<std::shared_ptr<BlankNodeManager::LocalBlankNodeManager>>
      managers;
  std::set<boost::uuids::uuid> uuids;

  for (int i = 0; i < 100; ++i) {
    auto lbnm = createLocalManager(bnm.get());
    allocateIds(*lbnm, 1);  // Allocate at least one ID.
    auto entries = serialize(*lbnm);
    uuids.insert(entries[0].uuid_);
    managers.push_back(std::move(lbnm));
  }

  // All UUIDs should be unique.
  EXPECT_EQ(uuids.size(), 100);
  EXPECT_EQ(getManagedUuidCount(*bnm), 100);
}

// _____________________________________________________________________________
TEST_F(BlankNodeManagerTestFixture, serializationPreservesBlockIndices) {
  auto bnm = createManager();

  // Manually manipulate to allocate specific blocks via explicit allocation.
  // Then use them in a LocalBlankNodeManager.
  BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry entry;
  entry.uuid_ = boost::uuids::random_generator()();
  entry.blockIndices_ = {5, 42, 100};

  std::vector entries{entry};
  auto lbnm2 = deserialize(bnm.get(), entries);

  // Verify the block indices match (now in otherBlocks, so second entry).
  auto serialized = serialize(*lbnm2);
  ASSERT_EQ(serialized.size(), 2);  // Empty primary + 1 in otherBlocks.
  EXPECT_TRUE(serialized[0].blockIndices_.empty());  // Primary is empty.
  EXPECT_EQ(serialized[1].blockIndices_, entry.blockIndices_);

  // Verify the blocks are actually registered.
  EXPECT_TRUE(isBlockUsed(*bnm, 5));
  EXPECT_TRUE(isBlockUsed(*bnm, 42));
  EXPECT_TRUE(isBlockUsed(*bnm, 100));
}

}  // namespace ad_utility
