// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include "util/BlankNodeManager.h"

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

// _____________________________________________________________________________
BlankNodeManager::Block::Block(uint64_t blockIndex, uint64_t startIndex)
    : blockIdx_(blockIndex), nextIdx_(startIndex) {}

// _____________________________________________________________________________
BlankNodeManager::LocalBlankNodeManager::LocalBlankNodeManager(
    BlankNodeManager* blankNodeManager)
    : blankNodeManager_(blankNodeManager) {}

// _____________________________________________________________________________
BlankNodeManager::LocalBlankNodeManager::~LocalBlankNodeManager() {
  auto ptr = blankNodeManager_->usedBlocksSet_.wlock();
  for (auto block : blocks_) {
    AD_CONTRACT_CHECK(ptr->contains(block.blockIdx_));
    ptr->erase(block.blockIdx_);
  }
}

// _____________________________________________________________________________
uint64_t BlankNodeManager::LocalBlankNodeManager::getId() {
  if (blocks_.empty() || blocks_.back().nextIdx_ == idxAfterCurrentBlock_) {
    blocks_.emplace_back(blankNodeManager_->allocateBlock());
    idxAfterCurrentBlock_ = blocks_.back().nextIdx_ + blockSize_;
  }
  return blocks_.back().nextIdx_++;
}

}  // namespace ad_utility
