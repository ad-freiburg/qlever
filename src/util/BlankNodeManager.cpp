// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

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
  auto resultFromSingleSet = [](const auto& set) {
    OwnedBlocksEntry res;
    res.uuid_ = set->uuid_;
    for (const auto& block : set->blocks_) {
      res.blockIndices_.push_back(block.blockIdx_);
    }
    return res;
  };
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

  auto allocateSingleSet = [this](const OwnedBlocksEntry& entry) {
    auto [blocks, isNew] =
        blankNodeManager_->registerBlocksWithExplicitUuid(entry.uuid_);
    if (isNew) {
      for (const auto& idx : entry.blockIndices_) {
        blocks->blocks_.emplace_back(
            blankNodeManager_->allocateExplicitBlock(idx));
      }
    }
    return blocks;
  };
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

auto BlankNodeManager::createBlockSet() -> std::shared_ptr<Blocks> {
  auto res = std::make_shared<Blocks>(this);
  // TODO<joka921> Check that uuid is not present?
  state_.wlock()->managedBlockSets_[res->uuid_] = res;
  return res;
}

void BlankNodeManager::freeBlockSet(Blocks& blocks) {
  // TODO<joka921> we have to put EVERYTHING inside the `BlankNodeManager`
  // into a `synchronized.
  auto lock = state_.wlock();
  auto it = lock->managedBlockSets_.find(blocks.uuid_);
  AD_CORRECTNESS_CHECK(it != lock->managedBlockSets_.end());
  auto& usedBlockSet = lock->usedBlocksSet_;
  for (const auto& block : blocks.blocks_) {
    AD_CONTRACT_CHECK(usedBlockSet.contains(block.blockIdx_));
    usedBlockSet.erase(block.blockIdx_);
  }
}

}  // namespace ad_utility
