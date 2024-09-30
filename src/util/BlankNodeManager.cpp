// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include "util/BlankNodeManager.h"

namespace ad_utility {

// _____________________________________________________________________________
BlankNodeManager::BlankNodeManager(
    SlowRandomIntGenerator<uint64_t> randomIntGenerator)
    : randBlockIndex_(randomIntGenerator) {}

// _____________________________________________________________________________
BlankNodeManager::Block BlankNodeManager::allocateBlock() {
  // The Random-Generation Algorithm's performance is reduced once the number of
  // used blocks exceeds a limit.
  AD_CORRECTNESS_CHECK(usedBlocksSet_.size() < totalAvailableBlocks_ / 256);

  // Lock this part, as it might be accessed concurrently by
  // `LocalBlankNodeManager` instances.
  mtx_.lock();
  uint64_t newBlockIndex = randBlockIndex_();
  while (usedBlocksSet_.contains(newBlockIndex)) {
    newBlockIndex = randBlockIndex_();
  }
  usedBlocksSet_.insert(newBlockIndex);
  mtx_.unlock();

  return Block(newBlockIndex);
}

// _____________________________________________________________________________
void BlankNodeManager::freeBlock(uint64_t blockIndex) {
  usedBlocksSet_.erase(blockIndex);
}

// _____________________________________________________________________________
BlankNodeManager::Block::Block(uint64_t blockIndex)
    : blockIdx_(blockIndex), nextIdx_(blockIndex * blockSize_) {}

// _____________________________________________________________________________
BlankNodeManager::Block::~Block() {
  globalBlankNodeManager.freeBlock(blockIdx_);
}

// _____________________________________________________________________________
uint64_t BlankNodeManager::LocalBlankNodeManager::getId() {
  if (blocks_.empty() ||
      blocks_.back().nextIdx_ == (blocks_.back().blockIdx_ + 1) * blockSize_) {
    blocks_.emplace_back(globalBlankNodeManager.allocateBlock());
  }
  return blocks_.back().nextIdx_++;
}

}  // namespace ad_utility
