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
  AD_CORRECTNESS_CHECK(usedBlocksSet_.rlock()->size() <
                       totalAvailableBlocks_ / 256);

  uint64_t newBlockIndex = randBlockIndex_();
  {
    auto usedBlocksSetPtr_ = usedBlocksSet_.wlock();
    while (usedBlocksSetPtr_->contains(newBlockIndex)) {
      newBlockIndex = randBlockIndex_();
    }
    usedBlocksSetPtr_->insert(newBlockIndex);
  }
  return Block(newBlockIndex, minIndex_ + newBlockIndex * blockSize_);
}

// _____________________________________________________________________________
void BlankNodeManager::freeBlock(uint64_t blockIndex) {
  usedBlocksSet_.wlock()->erase(blockIndex);
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
  for (auto block : blocks_) {
    blankNodeManager_->freeBlock(block.blockIdx_);
  }
}

// _____________________________________________________________________________
uint64_t BlankNodeManager::LocalBlankNodeManager::getId() {
  if (blocks_.empty() ||
      blocks_.back().nextIdx_ ==
          (blankNodeManager_->minIndex_ + blocks_.back().blockIdx_ + 1) *
              blockSize_) {
    blocks_.emplace_back(blankNodeManager_->allocateBlock());
  }
  return blocks_.back().nextIdx_++;
}

}  // namespace ad_utility
