// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_BLANKNODEMANAGER_H
#define QLEVER_SRC_UTIL_BLANKNODEMANAGER_H

#include <gtest/gtest_prod.h>

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <vector>

#include "HashMap.h"
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
  class Blocks;
  struct UuidHash {
    std::size_t operator()(const boost::uuids::uuid& uuid) const {
      return boost::hash<boost::uuids::uuid>()(uuid);
    }
  };

  struct State {
    // Int Generator yielding random block indices.
    SlowRandomIntGenerator<uint64_t> randBlockIndex_;

    // A generator for UUIDS of the associated blank node managers
    boost::uuids::random_generator uuidGenerator_;

    // Tracks blocks currently used by instances of `LocalBlankNodeManager`.
    HashSet<uint64_t> usedBlocksSet_;

    std::unordered_map<boost::uuids::uuid, std::weak_ptr<Blocks>, UuidHash>
        managedBlockSets_;

    explicit State(SlowRandomIntGenerator<uint64_t> randBlockIndex)
        : randBlockIndex_{std::move(randBlockIndex)} {}
  };
  Synchronized<State> state_;
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
  struct Blocks {
    BlankNodeManager* manager_;
    boost::uuids::uuid uuid_{manager_->state_.wlock()->uuidGenerator_()};
    std::vector<Block> blocks_;

    explicit Blocks(BlankNodeManager* manager) : manager_(manager) {}
    ~Blocks() { manager_->freeBlockSet(*this); }
  };

  // TODO<joka921> make this `ad_utility::HashSet`.

 public:
  // Constructor, where `minIndex` is the minimum index such that all managed
  // indices are in [`minIndex_`, `ValueId::maxIndex`]. `minIndex_` is
  // determined by the number of BlankNodes in the current Index.
  explicit BlankNodeManager(uint64_t minIndex = 0);
  ~BlankNodeManager() = default;

  std::shared_ptr<Blocks> createBlockSet();
  // TODO<joka921> Implement this.
  std::shared_ptr<Blocks> registerBlocksWithExplicitUuid(
      boost::uuids::uuid uuid);
  void freeBlockSet(Blocks& blocks);

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

    struct OwnedBlocksEntry {
      boost::uuids::uuid uuid_;
      std::vector<uint64_t> blockIndices_;
    };

    // Return the indices of all the blank node blocks that are currently being
    // owned by this `LocalBlankNodeManager`. Can be used to persist
    // intermediate results or SPARQL UPDATEs that contain blank nodes on disk.
    std::vector<OwnedBlocksEntry> getOwnedBlockIndices() const;

    // Explicitly allocate the blank node blocks with the given `indices`.
    // Will throw an exception if any of the requested blocks is already
    // allocated. For usages see `LocalVocab.h` and `TripleSerializer.h`.
    void allocateBlocksFromExplicitIndices(
        const std::vector<OwnedBlocksEntry>& indices);

    // Getter for the `blankNodeManager_` pointer required in
    // `LocalVocab::mergeWith`.
    BlankNodeManager* blankNodeManager() const { return blankNodeManager_; }

   private:
    // Reference to the BlankNodeManager, used to free the reserved blocks.
    BlankNodeManager* blankNodeManager_;

    // Reserved blocks.
    std::shared_ptr<Blocks> blocks_ = blankNodeManager_->createBlockSet();

    // The first index after the current Block.
    uint64_t idxAfterCurrentBlock_{0};

    // Blocks merged from other `LocalBlankNodeManager`s.
    std::vector<std::shared_ptr<const Blocks>> otherBlocks_;

    FRIEND_TEST(BlankNodeManager, LocalBlankNodeManagerGetID);
  };

  // Allocate and retrieve a block of new blank node indexes.
  [[nodiscard]] Block allocateBlock();

  // Allocate and return the block with the given `blockIdx`. This function can
  // only be safely called when no calls to `allocatedBlock()` have been
  // performed. It can for example be used to restore blocks from previously
  // serialized cache results or updates when the engine is started, but before
  // any queries are performed.
  [[nodiscard]] Block allocateExplicitBlock(uint64_t blockIdx);

  // Get the number of currently used blocks
  size_t numBlocksUsed() const { return state_.rlock()->usedBlocksSet_.size(); }

  FRIEND_TEST(BlankNodeManager, blockAllocationAndFree);
  FRIEND_TEST(BlankNodeManager, moveLocalBlankNodeManager);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BLANKNODEMANAGER_H
