// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include "util/BlankNodeManager.h"

#include "util/Exception.h"

namespace ad_utility {

// _____________________________________________________________________________
BlankNodeManager::BlankNodeManager(uint64_t minIndex)
    : minIndex_(minIndex),
      randBlockIndex_(
          SlowRandomIntGenerator<uint64_t>(0, totalAvailableBlocks_ - 1)) {}

// _____________________________________________________________________________
BlankNodeManager::Block BlankNodeManager::allocateBlock() {
  // The Random-Generation Algorithm's performance is reduced once the number of
  // used blocks exceeds a limit.
  auto numBlocks = usedBlocksSet_.rlock()->size();
  AD_CORRECTNESS_CHECK(
      numBlocks < totalAvailableBlocks_ / 256,
      absl::StrCat("Critical high number of blank node blocks in use: ",
                   numBlocks, " blocks"));

  auto usedBlocksSetPtr = usedBlocksSet_.wlock();
  while (true) {
    auto blockIdx = randBlockIndex_();
    if (!usedBlocksSetPtr->contains(blockIdx)) {
      usedBlocksSetPtr->insert(blockIdx);
      return Block(blockIdx, minIndex_ + blockIdx * blockSize_);
    }
  }
}

// ______________________________________________________________________________
[[nodiscard]] auto BlankNodeManager::reserveExplicitBlock(uint64_t blockIdx)
    -> Block {
  auto usedBlocksSetPtr = usedBlocksSet_.wlock();
  AD_CONTRACT_CHECK(!usedBlocksSetPtr->contains(blockIdx),
                    "Trying to explicitly allocate a block of blank nodes that "
                    "has previously already been allocated.");
  usedBlocksSetPtr->insert(blockIdx);
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
  if (blocks_->empty() || blocks_->back().nextIdx_ == idxAfterCurrentBlock_) {
    blocks_->emplace_back(blankNodeManager_->allocateBlock());
    idxAfterCurrentBlock_ = blocks_->back().nextIdx_ + blockSize_;
  }
  return blocks_->back().nextIdx_++;
}

// _____________________________________________________________________________
bool BlankNodeManager::LocalBlankNodeManager::containsBlankNodeIndex(
    uint64_t index) const {
  auto containsIndex = [index](const Block& block) {
    return index >= block.startIdx_ && index < block.nextIdx_;
  };

  return ql::ranges::any_of(*blocks_, containsIndex) ||
         ql::ranges::any_of(
             otherBlocks_,
             [&](const std::shared_ptr<const std::vector<Block>>& blocks) {
               return ql::ranges::any_of(*blocks, containsIndex);
             });
}

// _____________________________________________________________________________
std::vector<uint64_t>
BlankNodeManager::LocalBlankNodeManager::getReservedBlockIndices() const {
  std::vector<uint64_t> indices;
  // TODO<joka921> Use nicer algorithms for this.
  for (const auto& block : *blocks_) {
    indices.push_back(block.blockIdx_);
  }
  for (const auto& set : otherBlocks_) {
    for (const auto& block : *set) {
      indices.push_back(block.blockIdx_);
    }
  }
  return indices;
}

// _____________________________________________________________________________
void BlankNodeManager::LocalBlankNodeManager::reserveBlocksFromExplicitIndices(
    const std::vector<uint64_t>& indices) {
  AD_CONTRACT_CHECK(blocks_->empty() && otherBlocks_.empty(),
                    "Explicit reserving of blank node blocks is only allowed "
                    "for empty `LocalBlankNodeManager`s");
  for (const auto& idx : indices) {
    blocks_->emplace_back(blankNodeManager_->reserveExplicitBlock(idx));
  }

  // The following code ensures that the next call to `getId` allocates a new
  // block. This is necessary, because we don't have access to the information
  // which IDs in the allocated blocks actually are in use.
  if (!blocks_->empty()) {
    idxAfterCurrentBlock_ = blocks_->back().nextIdx_;
  }
}

}  // namespace ad_utility
