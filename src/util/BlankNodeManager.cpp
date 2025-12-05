// Copyright 2024 - 2025 The QLever Authors, in particular:
//
// 2024 Moritz Dom <domm@informatik.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
#include "util/BlankNodeManager.h"

#include "util/Exception.h"

namespace ad_utility {

// _____________________________________________________________________________
BlankNodeManager::BlankNodeManager(uint64_t minIndex)
    : minIndex_(minIndex),
      state_{SlowRandomIntGenerator<uint64_t>(0, totalAvailableBlocks_ - 1)} {}

// _____________________________________________________________________________
BlankNodeManager::Block BlankNodeManager::allocateBlock() {
  // The Random-Generation Algorithm's performance is reduced once the number of
  // used blocks exceeds a limit.
  auto stateLock = state_.wlock();
  auto numBlocks = stateLock->usedBlocksSet_.size();
  AD_CORRECTNESS_CHECK(
      numBlocks < totalAvailableBlocks_ / 256,
      absl::StrCat("Critical high number of blank node blocks in use: ",
                   numBlocks, " blocks"));

  auto& usedBlocksSetPtr = stateLock->usedBlocksSet_;
  while (true) {
    auto blockIdx = stateLock->randBlockIndex_();
    if (!usedBlocksSetPtr.contains(blockIdx)) {
      usedBlocksSetPtr.insert(blockIdx);
      return Block(blockIdx, minIndex_ + blockIdx * blockSize_);
    }
  }
}

// ______________________________________________________________________________
[[nodiscard]] auto BlankNodeManager::allocateExplicitBlock(uint64_t blockIdx)
    -> Block {
  auto lock = state_.wlock();
  auto& usedBlocksSet = lock->usedBlocksSet_;
  AD_CONTRACT_CHECK(!usedBlocksSet.contains(blockIdx),
                    "Trying to explicitly allocate a block of blank nodes that "
                    "has previously already been allocated.");
  usedBlocksSet.insert(blockIdx);
  return Block(blockIdx, minIndex_ + blockIdx * blockSize_);
}

// _____________________________________________________________________________
BlankNodeManager::Block::Block(uint64_t blockIndex, uint64_t startIndex)
    : blockIdx_(blockIndex), startIdx_(startIndex), nextIdx_(startIndex) {}

// _____________________________________________________________________________
BlankNodeManager::LocalBlankNodeManager::LocalBlankNodeManager(
    BlankNodeManager* blankNodeManager)
    : blankNodeManager_(blankNodeManager) {}

// _____________________________________________________________________________
uint64_t BlankNodeManager::LocalBlankNodeManager::getId() {
  auto& blocks = blocks_->blocks_;
  if (blocks.empty() || blocks.back().nextIdx_ == idxAfterCurrentBlock_) {
    blocks.emplace_back(blankNodeManager_->allocateBlock());
    idxAfterCurrentBlock_ = blocks.back().nextIdx_ + blockSize_;
  }
  return blocks.back().nextIdx_++;
}

// _____________________________________________________________________________
bool BlankNodeManager::LocalBlankNodeManager::containsBlankNodeIndex(
    uint64_t index) const {
  auto containsIndex = [index](const Block& block) {
    return index >= block.startIdx_ && index < block.nextIdx_;
  };

  return ql::ranges::any_of(blocks_->blocks_, containsIndex) ||
         ql::ranges::any_of(
             otherBlocks_, [&](const std::shared_ptr<const Blocks>& blocks) {
               return ql::ranges::any_of(blocks->blocks_, containsIndex);
             });
}

// _____________________________________________________________________________
auto BlankNodeManager::LocalBlankNodeManager::getOwnedBlockIndices() const
    -> std::vector<OwnedBlocksEntry> {
  std::vector<OwnedBlocksEntry> indices;
  // Lambda that turns a single `Blocks` object into an `OwnedBlocksEntry`.
  auto resultFromSingleSet = [](const auto& set) {
    OwnedBlocksEntry res;
    res.uuid_ = set->uuid_;
    // TODO<joka921> use ::ranges::transform/to_vector.
    for (const auto& block : set->blocks_) {
      res.blockIndices_.push_back(block.blockIdx_);
    }
    return res;
  };

  // First serialize the primary blocks set, and then the other block sets.
  indices.reserve(blocks_->blocks_.size() + otherBlocks_.size());
  indices.push_back(resultFromSingleSet(blocks_));
  for (const auto& set : otherBlocks_) {
    indices.push_back(resultFromSingleSet(set));
  }
  return indices;
}

// _____________________________________________________________________________
void BlankNodeManager::LocalBlankNodeManager::allocateBlocksFromExplicitIndices(
    const std::vector<OwnedBlocksEntry>& indices) {
  AD_CONTRACT_CHECK(blocks_->blocks_.empty() && otherBlocks_.empty(),
                    "Explicit reserving of blank node blocks is only allowed "
                    "for empty `LocalBlankNodeManager`s");

  // Lambda that does the registering for a single `Blocks` object.
  auto allocateSingleSet = [this](const OwnedBlocksEntry& entry) {
    auto [blocks, isNew] =
        blankNodeManager_->registerBlocksWithExplicitUuid(entry.uuid_);
    // If the block is not new, then the `Blocks` with the given UUID have
    // already been reserved by another `LocalBlankNodeManager` that owns the
    // same set. If it is new, we are the first, and have to also allocate the
    // blocks.
    // TODO<joka921> It is ugly to not have this under a single lock and sublte,
    // maybe this complete lambda should be a member function of the
    // `BlankNodeManager` under a single lock.
    if (isNew) {
      for (const auto& idx : entry.blockIndices_) {
        blocks->blocks_.emplace_back(
            blankNodeManager_->allocateExplicitBlock(idx));
      }
    }
    return blocks;
  };

  // The semantics of the argument is (as enforced by the `getOwnedBlockIndices`
  // function): The first element is the `blocks_` primarily owned by this
  // `LocalBlankNodeManager`, The remaining elements are the `otherBlocks_`.
  AD_CONTRACT_CHECK(!indices.empty());
  blocks_ = allocateSingleSet(indices.at(0));
  otherBlocks_.reserve(indices.size() - 1);
  for (const auto& entry : indices | ql::views::drop(1)) {
    otherBlocks_.push_back(allocateSingleSet(entry));
  }

  // The following code ensures that the next call to `getId` allocates a new
  // block. This is necessary, because we don't have access to the information
  // which IDs in the allocated blocks actually are in use.
  if (!blocks_->blocks_.empty()) {
    idxAfterCurrentBlock_ = blocks_->blocks_.back().nextIdx_;
  }
}

// _____________________________________________________________________________
auto BlankNodeManager::createBlockSet() -> std::shared_ptr<Blocks> {
  auto res = std::make_shared<Blocks>(this);
  // Guard against the (very very unlikely) case of UUID collision
  auto lock = state_.wlock();
  while (true) {
    auto [it, isNew] = lock->managedBlockSets_.try_emplace(res->uuid_, res);
    if (isNew) {
      return res;
    }
    // Note: the only realistic way to cover this is by tampering with the UUID
    // generator.
    res->uuid_ = lock->uuidGenerator_();
  }
}

// _____________________________________________________________________________
void BlankNodeManager::freeBlockSet(Blocks& blocks) {
  // We keep the lock the whole time to not make any inconsistent state visible
  // to the outside world.
  auto lock = state_.wlock();
  // First unregister the UUID.
  auto it = lock->managedBlockSets_.find(blocks.uuid_);
  AD_CORRECTNESS_CHECK(it != lock->managedBlockSets_.end());
  // This `if` check guards against a very rare condition where timings AND
  // UUIDs have to collide. In particular, we expect the value to be expired,
  // because this function is only called in the destructor of the object that
  // the `weak_ptr` points to, so after there are no more `shared_ptr`s to this
  // object.
  if (it->second.expired()) {
    lock->managedBlockSets_.erase(it);
  }
  auto& usedBlockSet = lock->usedBlocksSet_;
  for (const auto& block : blocks.blocks_) {
    AD_CONTRACT_CHECK(usedBlockSet.contains(block.blockIdx_));
    usedBlockSet.erase(block.blockIdx_);
  }
}

// _____________________________________________________________________________
std::pair<std::shared_ptr<BlankNodeManager::Blocks>, bool>
BlankNodeManager::registerBlocksWithExplicitUuid(boost::uuids::uuid uuid) {
  auto lock = state_.wlock();
  // Try to insert a new `nullptr` at the given UUID. If  the insertion
  // succeeds, we will later emplace a useful value.
  auto [it, isNew] = lock->managedBlockSets_.try_emplace(
      uuid, std::shared_ptr<Blocks>(nullptr));

  // The `expired()` might happen in a very rare condition, where deleting of a
  // `UUID` and its reinsertion race against each other. (I doubt that it can be
  // observed in correct code outside of unit tests).
  if (isNew || it->second.expired()) {
    auto blocks = std::make_shared<Blocks>(this);
    it->second = blocks;
    return std::make_pair(std::move(blocks), true);
  } else {
    // We have found a preexisting, nonexpired `Blocks` object with the
    // requested UUID, just return a shared_ptr to the stored `Blocks` object.
    return std::make_pair(it->second.lock(), true);
  }
}

}  // namespace ad_utility
