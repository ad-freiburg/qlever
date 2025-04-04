// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_BLANKNODEMANAGER_H
#define QLEVER_SRC_UTIL_BLANKNODEMANAGER_H

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

    // The first index within this block
    const uint64_t startIdx_;
    // The next free index within this block.
    uint64_t nextIdx_;
  };

  // Manages the blank nodes for a single local vocab.
  class LocalBlankNodeManager {
   public:
    explicit LocalBlankNodeManager(BlankNodeManager* blankNodeManager);
    ~LocalBlankNodeManager() = default;

    // No copy, as the managed blocks should not be duplicated.
    LocalBlankNodeManager(const LocalBlankNodeManager&) = delete;
    LocalBlankNodeManager& operator=(const LocalBlankNodeManager&) = delete;

    // Move is allowed.
    LocalBlankNodeManager(LocalBlankNodeManager&&) = default;
    LocalBlankNodeManager& operator=(LocalBlankNodeManager&&) = default;

    // Get a new blank node index.
    [[nodiscard]] uint64_t getId();

    // Return true iff the `index` was returned by a previous call to `getId()`.
    bool containsBlankNodeIndex(uint64_t index) const;

    // Merge passed `LocalBlankNodeManager`s to keep alive their reserved
    // BlankNodeIndex blocks.
    CPP_template(typename R)(requires ql::ranges::range<R>) void mergeWith(
        const R& localBlankNodeManagers) {
      auto inserter = std::back_inserter(otherBlocks_);
      for (const auto& l : localBlankNodeManagers) {
        if (l == nullptr) {
          continue;
        }
        ql::ranges::copy(l->otherBlocks_, inserter);
        *inserter = l->blocks_;
      }
    }

    // Getter for the `blankNodeManager_` pointer required in
    // `LocalVocab::mergeWith`.
    BlankNodeManager* blankNodeManager() const { return blankNodeManager_; }

   private:
    // Reference to the BlankNodeManager, used to free the reserved blocks.
    BlankNodeManager* blankNodeManager_;

    // Reserved blocks.
    using Blocks = std::vector<BlankNodeManager::Block>;
    std::shared_ptr<Blocks> blocks_{
        new Blocks(), [blankNodeManager = blankNodeManager()](auto blocksPtr) {
          auto ptr = blankNodeManager->usedBlocksSet_.wlock();
          for (const auto& block : *blocksPtr) {
            AD_CONTRACT_CHECK(ptr->contains(block.blockIdx_));
            ptr->erase(block.blockIdx_);
          }
          delete blocksPtr;
        }};

    // The first index after the current Block.
    uint64_t idxAfterCurrentBlock_{0};

    // Blocks merged from other `LocalBlankNodeManager`s.
    std::vector<std::shared_ptr<const Blocks>> otherBlocks_;

    FRIEND_TEST(BlankNodeManager, LocalBlankNodeManagerGetID);
  };

  // Allocate and retrieve a block of new blank node indexes.
  [[nodiscard]] Block allocateBlock();

  FRIEND_TEST(BlankNodeManager, blockAllocationAndFree);
  FRIEND_TEST(BlankNodeManager, moveLocalBlankNodeManager);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BLANKNODEMANAGER_H
