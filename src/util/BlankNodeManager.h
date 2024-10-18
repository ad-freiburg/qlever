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
 * Manager class owned by an `Index` to manage currently available indices for
 * blank nodes to be added during runtime. The intention is to use the same
 * `BlankNodeIndex`-Datatype as for blank nodes given at indexing time, by
 * setting their count as the minimum index for the ones added at runtime.
 * A `LocalVocab` can register new blank nodes (e.g. resulting from a `Service`
 * operation) by obtaining a `Block` of currently unused indices using it's own
 * `LocalBlankNodeManager` from the `BlankNodeManager`.
 */
class BlankNodeManager {
 public:
  // Minimum blank node index.
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
  // indices are in [`minIndex_`, `ValueId::maxIndex`]. `minIndex_` is
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
    const uint64_t blockIdx_;
    // The next free index within this block.
    uint64_t nextIdx_;
  };

  // Manages the BlankNodes used within a LocalVocab.
  class LocalBlankNodeManager {
   public:
    explicit LocalBlankNodeManager(BlankNodeManager* blankNodeManager);
    ~LocalBlankNodeManager();

    // No copy, as the managed blocks shall not be duplicated.
    LocalBlankNodeManager(const LocalBlankNodeManager&) = delete;
    LocalBlankNodeManager& operator=(const LocalBlankNodeManager&) = delete;

    LocalBlankNodeManager(LocalBlankNodeManager&&) = default;
    LocalBlankNodeManager& operator=(LocalBlankNodeManager&&) = default;

    // Get a new id.
    [[nodiscard]] uint64_t getId();

   private:
    // Reserved blocks.
    std::vector<BlankNodeManager::Block> blocks_;

    // Reference of the BlankNodeManager, used to free the reserved blocks.
    BlankNodeManager* blankNodeManager_;

    // The first index after the current Block.
    uint64_t idxAfterCurrentBlock_{0};

    FRIEND_TEST(BlankNodeManager, LocalBlankNodeManagerGetID);
  };

  // Allocate and retrieve a block of free ids.
  [[nodiscard]] Block allocateBlock();

  FRIEND_TEST(BlankNodeManager, blockAllocationAndFree);
  FRIEND_TEST(BlankNodeManager, moveLocalBlankNodeManager);
};

}  // namespace ad_utility
