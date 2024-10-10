// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#pragma once

#include <gtest/gtest_prod.h>

#include <vector>

#include "global/ValueId.h"
#include "util/HashSet.h"
#include "util/Random.h"
#include "util/Synchronized.h"

namespace ad_utility {
/*
 * Manager class for Blank node indices added after indexing time.
 */
class BlankNodeManager {
 public:
  // Minimum index.
  const uint64_t minIndex_;

  // Number of indices that make up a single block.
  static constexpr uint blockSize_ = 1000;

  // Number of blocks available.
  const uint64_t totalAvailableBlocks_ =
      (ValueId::maxIndex - minIndex_ + 1) / blockSize_;

 private:
  // Int Generator yielding random block indices.
  SlowRandomIntGenerator<uint64_t> randBlockIndex_;

  // Tracks blocks currently used by instances of `LocalBlankNodeManager`.
  Synchronized<HashSet<uint64_t>> usedBlocksSet_;

 public:
  // Constructor, where `minIndex` is the minimum index such that all managed
  // indices are in [`minIndex_`, `ValueId::maxIndex`]. Currently `minIndex_` is
  // determined by the number of BlankNodes in the current Index.
  explicit BlankNodeManager(uint64_t minIndex = 0);
  ~BlankNodeManager() = default;

  // A BlankNodeIndex Block of size `blockSize_`.
  class Block {
    // Intentional private constructor, allowing only the BlankNodeManager to
    // create Blocks (for a `LocalBlankNodeManager`).
    explicit Block(uint64_t blockIndex, uint64_t startIndex);
    friend class BlankNodeManager;

   public:
    ~Block() = default;
    // The index of this block.
    uint64_t blockIdx_;
    // The next free index within this block.
    uint64_t nextIdx_;
  };

  // Manages the BlankNodes used within a LocalVocab.
  class LocalBlankNodeManager {
   public:
    explicit LocalBlankNodeManager(BlankNodeManager* blankNodeManager);
    ~LocalBlankNodeManager();

    // Get a new id.
    [[nodiscard]] uint64_t getId();

   private:
    // Reserved blocks.
    std::vector<BlankNodeManager::Block> blocks_;

    // Reference of the BlankNodeManager, used to free the reserved blocks.
    BlankNodeManager* const blankNodeManager_;

    FRIEND_TEST(BlankNodeManager, LocalBlankNodeManagerGetID);
  };

  void setInitialIndex(uint64_t idx);

  // Allocate and retrieve a block of free ids.
  [[nodiscard]] Block allocateBlock();

  // Free a block of ids.
  void freeBlock(uint64_t blockIndex);

  FRIEND_TEST(BlankNodeManager, blockAllocationAndFree);
};

}  // namespace ad_utility
