// Copyright 2024 - 2025 The QLever Authors, in particular:
//
// 2024 Moritz Dom <domm@informatik.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
#include "util/BlankNodeManager.h"

#include <absl/cleanup/cleanup.h>

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
  stateLock->randomBlockWasRequested_ = true;
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
[[nodiscard]] auto BlankNodeManager::allocateExplicitBlock(
    uint64_t blockIdx, boost::optional<WriteLock&> lockOpt) -> Block {
  auto localLock =
      lockOpt.has_value() ? std::nullopt : std::optional{state_.wlock()};
  auto& lock = lockOpt.has_value() ? lockOpt.value() : localLock.value();
  auto& usedBlocksSet = lock->usedBlocksSet_;
  AD_CONTRACT_CHECK(!lock->randomBlockWasRequested_,
                    "The explicit allocation of blank node blocks (e.g. from "
                    "serialized updates or cached results) has to happen "
                    "before any additional random blank nodes are requested");
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
             otherBlocks_,
             [containsIndex](const std::shared_ptr<const Blocks>& blocks) {
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
    res.blockIndices_ = ::ranges::to<std::vector>(
        set->blocks_ | ql::views::transform(&Block::blockIdx_));
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

  // We read all the previously allocated blocks into the `otherBlocks_`, s.t.
  // the primary `blocks_` vector statys empty. That way, we are completely
  // decoupled from other `LocalBlankNodeManager`s which might reuse the same
  // blocks as `*this`.
  otherBlocks_.reserve(indices.size());
  for (const auto& entry : indices) {
    otherBlocks_.push_back(
        blankNodeManager_->registerAndAllocateBlockSet(entry));
  }
}

// _____________________________________________________________________________
auto BlankNodeManager::createBlockSet() -> std::shared_ptr<Blocks> {
  // Guard against the (very very unlikely) case of UUID collision.
  auto lockOpt = std::optional{state_.wlock()};
  auto& lock = lockOpt.value();
  auto uuid = lock->uuidGenerator_();
  auto [it, isNew] =
      lock->managedBlockSets_.try_emplace(uuid, std::shared_ptr<Blocks>());
  // Note: the (very unlikely) exception thrown by the following check is safe,
  // as all the destructors of the variables above are trivial, and we haven't
  // actually modified the `managedBlockSets_` in the case `isNew` is false.
  AD_CORRECTNESS_CHECK(isNew,
                       "You encountered a UUID collision inside "
                       "`BlankNodeManager::createBlockSet()`. Consider "
                       "yourself to be very (un)lucky!");
  auto res = std::make_shared<Blocks>(this, uuid);
  it->second = res;
  return res;
}

// _____________________________________________________________________________
void BlankNodeManager::freeBlockSet(const Blocks& blocks) {
  // We keep the lock the whole time because we have to perform a consistent,
  // transactional operation on the `state_`, which itself is not threadsafe.
  state_.withWriteLock([&blocks](auto& state) {
    // First unregister the UUID.
    auto it = state.managedBlockSets_.find(blocks.uuid_);
    if (it == state.managedBlockSets_.end()) {
      // Note: it is very hard to manually trigger this condition in unit tests,
      // because it depends on very subtle race conditions, and the reusing of
      // explicit `UUUID`s which in my understanding can currently never happen.
      // We nevertheless still return silently here to make the code more
      // robust.
      return;
    }
    // This `if` check guards against a very rare condition where timings AND
    // UUIDs have to collide. In particular, we expect the value to be expired,
    // because this function is only called in the destructor of the object that
    // the `weak_ptr` points to, so after there are no more `shared_ptr`s to
    // this object.
    if (it->second.expired()) {
      state.managedBlockSets_.erase(it);
    }
    auto& usedBlockSet = state.usedBlocksSet_;
    for (const auto& block : blocks.blocks_) {
      AD_CONTRACT_CHECK(usedBlockSet.contains(block.blockIdx_));
      usedBlockSet.erase(block.blockIdx_);
    }
  });
}

// _____________________________________________________________________________
std::shared_ptr<BlankNodeManager::Blocks>
BlankNodeManager::registerAndAllocateBlockSet(
    const LocalBlankNodeManager::OwnedBlocksEntry& entry) {
  // We keep the lock the whole time to avoid race conditions between
  // registering the UUID and allocating the blocks.
  auto lockOpt = std::optional{state_.wlock()};
  auto& lock = lockOpt.value();

  // Try to insert a new `nullptr` at the given UUID. If the insertion
  // succeeds, we will later emplace a useful value.
  auto [it, isNew] = lock->managedBlockSets_.try_emplace(
      entry.uuid_, std::shared_ptr<Blocks>(nullptr));

  // Note: the following `nullptr` check might become true for two reasons:
  // 1. We have newly inserted the UUID (likely), or 2. We have found an expired
  // `weak_ptr` from a previous usage of the same UUID, where we are currently
  // racing against its deletion (very unlikely). Note, that 2., even if it
  // happens never causes a problem, as the `freeBlockSet` function also
  // gracefully handles this case.
  if (auto ptr = it->second.lock(); ptr == nullptr) {
    auto blocks = std::make_shared<Blocks>(this, entry.uuid_);
    // At the end of this scope is a return. But in the case of an exception
    // (e.g. if the `AD_CONTRACT_CHECK` below fires, we have to unlock, before
    // the destructor of the `blocks` runs, otherwise we are in a deadlock.
    auto cleanup = absl::Cleanup{[&lockOpt]() { lockOpt.reset(); }};
    it->second = blocks;
    // If the block is new, we need to allocate all the specified block indices.
    for (const auto& idx : entry.blockIndices_) {
      blocks->blocks_.push_back(allocateExplicitBlock(idx, lock));
    }
    return blocks;
  } else {
    // We have found a preexisting, nonexpired `Blocks` object with the
    // requested UUID, just return a shared_ptr to the stored `Blocks` object.
    AD_CORRECTNESS_CHECK(ptr != nullptr);
    AD_CORRECTNESS_CHECK(ql::ranges::equal(
        entry.blockIndices_,
        ptr->blocks_ | ql::views::transform(&Block::blockIdx_)));
    return ptr;
  }
}

}  // namespace ad_utility
