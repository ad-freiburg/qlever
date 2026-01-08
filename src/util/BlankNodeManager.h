// Copyright 2024 - 2025 The QLever Authors, in particular:
//
// 2024 Moritz Dom <domm@informatik.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLANKNODEMANAGER_H
#define QLEVER_SRC_UTIL_BLANKNODEMANAGER_H

#include <gtest/gtest_prod.h>

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <vector>

#include "global/ValueId.h"
#include "util/ExceptionHandling.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/Random.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
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
  // The minimal `BlankNodeIndex` that this manager can assign. All indices `<
  // minIndex_` are already contained in the original `Index` of QLever (without
  // considering UPDATEs or blank nodes from local query), These blank nodes are
  // not managed by this `BlankNodeManager`.
  const uint64_t minIndex_;

  // Number of indices that make up a single block.
  static constexpr uint blockSize_ = 1000;

  // Number of blocks available.
  const uint64_t totalAvailableBlocks_ =
      (ValueId::maxIndex - minIndex_ + 1) / blockSize_;

 private:
  // Forward declaration because of cyclic dependency.
  struct Blocks;

  // All the data members of this `BlankNodeManager`, wrapped into a struct,
  // s.t. we can synchronize the access and make the `BlankNodeManager`
  // threadsafe.
  struct State {
    // Random generator for block indices.
    SlowRandomIntGenerator<uint64_t> randBlockIndex_;

    // A random generator for UUIDs.
    boost::uuids::random_generator uuidGenerator_;

    // Hash set the stores the indices of all the blank node blocks that are
    // currently reserved by any of the `LocalBlankNodeManager` that are
    // currently alive.
    HashSet<uint64_t> usedBlocksSet_;

    // Each set of blocks that is currently managed by a `LocalBlankNodeManager`
    // is assigned a UUID. This map keeps track of the currently active sets,
    // but does not participate in their (shared) ownership, hence the
    // `weak_ptr`.
    ad_utility::HashMap<boost::uuids::uuid, std::weak_ptr<Blocks>,
                        boost::hash<boost::uuids::uuid>>
        managedBlockSets_;

    // Constructor, all members except for the block index generator can be
    // default-constructed.
    explicit State(SlowRandomIntGenerator<uint64_t> randBlockIndex)
        : randBlockIndex_{std::move(randBlockIndex)} {}
  };

  // The actual state variable, wrapped in a `Synchronized` to enforce
  // threadsafe access.
  Synchronized<State> state_;

  // A block of blank node indices.
  class Block {
    // Intentional private constructor, allowing only the BlankNodeManager to
    // create Blocks (which are then passed to a `LocalBlankNodeManager`).
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

  // A set of allocated blocks, that is associated with a UUID. On destruction,
  // all the blocks, as well as the UUID are deallocated in the
  // `BlankNodeManager` from which the `Blocks` were obtained.
  struct Blocks {
    BlankNodeManager* manager_;
    boost::uuids::uuid uuid_;
    std::vector<Block> blocks_;
    ad_utility::ThrowInDestructorIfSafe throwIfSafe_;

    explicit Blocks(BlankNodeManager* manager, boost::uuids::uuid uuid)
        : manager_(manager), uuid_(std::move(uuid)) {
      AD_CORRECTNESS_CHECK(manager_ != nullptr);
    }
    ~Blocks() noexcept(false) {
      throwIfSafe_(
          [this]() { manager_->freeBlockSet(*this); },
          std::string_view{"In `freeBlockSet` called from the destructor of a "
                           "`BlankNodeManager::Blocks` object"});
    }
    // We never want to copy or move `Blocks`, they are only ever to be managed
    // by `shared_ptr`s.
    Blocks(const Blocks&) = delete;
    Block& operator=(const Blocks&) = delete;
  };

 public:
  // Constructor, where `minIndex` is the minimum index such that all managed
  // indices are in [`minIndex_`, `ValueId::maxIndex`]. `minIndex_` is
  // determined by the number of BlankNodes in the current Index.
  explicit BlankNodeManager(uint64_t minIndex = 0);
  ~BlankNodeManager() = default;

  // Create a new set of blocks with a random UUID that initially contains no
  // blocks, but is registered (via the UUID) in the `BlankNodeManager`.
  std::shared_ptr<Blocks> createBlockSet();

  // Free all the blocks currently contained in the `blocks` and unregister the
  // associated UUID. This function is called by the `Blocks` destructor.
  void freeBlockSet(const Blocks& blocks);

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

    // The information required to serialize and deserialize a
    // `LocalBlankNodeManager`, containing of the `uuid_` and the
    // `blockIndices_`. Note: We do not store the status of the blocks (how many
    // of the indices in the block were already assigned), but on
    // deserialization always behave as if they are completely filled. This
    // wastes at most one block per `LocalBlankNodeManager` and therefore should
    // definitely be affordable.
    struct OwnedBlocksEntry {
      boost::uuids::uuid uuid_;
      std::vector<uint64_t> blockIndices_;
      AD_SERIALIZE_FRIEND_FUNCTION(OwnedBlocksEntry) {
        triviallySerialize(serializer, arg.uuid_);
        serializer | arg.blockIndices_;
      }
    };

    // Return the indices of all the blank node blocks that are currently being
    // owned by this `LocalBlankNodeManager`. Can be used to persist
    // intermediate results or SPARQL UPDATEs that contain blank nodes on disk.
    std::vector<OwnedBlocksEntry> getOwnedBlockIndices() const;

    // Reinstate the blank node block sets by allocating and registering all the
    // UUIDs and block indices contained in `indices`. This has to be called on
    // an empty `LocalBlankNodeManager` with the result of
    // `getOwnedBlockIndices()`.
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
    friend class BlankNodeManagerTestFixture;
  };

  // Allocate and retrieve a block of new blank node indexes.
  [[nodiscard]] Block allocateBlock();

  // Allocate and return the block with the given `blockIdx`. This function can
  // only be safely called when no calls to `allocatedBlock()` have been
  // performed. It can for example be used to restore blocks from previously
  // serialized cache results or updates when the engine is started, but before
  // any queries are performed.
  [[nodiscard]] Block allocateExplicitBlock(uint64_t blockIdx);

  // If the `uuid` of the `entry` is not yet registered with this
  // `BlankNodeManager`, register and return a new `Blocks` struct with the
  // explicit UUID of the `entry`, and explicitly allocate all blocks
  // represented by the `entry` and store them in the result. If the `uuid` is
  // already registered, then return a `shared_ptr` to the `Blocks` associated
  // with this `uuid`. This functionality is used to reinstate sets of
  // registered blocks when loading SPARQL UPDATEs or serialized cache results
  // when QLever is started.
  std::shared_ptr<Blocks> registerAndAllocateBlockSet(
      const LocalBlankNodeManager::OwnedBlocksEntry& entry);

  // Get the number of currently used blocks
  size_t numBlocksUsed() const { return state_.rlock()->usedBlocksSet_.size(); }

  FRIEND_TEST(BlankNodeManager, blockAllocationAndFree);
  FRIEND_TEST(BlankNodeManager, moveLocalBlankNodeManager);
  friend class BlankNodeManagerTestFixture;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_BLANKNODEMANAGER_H
