// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#pragma once

#include <vector>

#include "global/ValueId.h"
#include "util/HashSet.h"
#include "util/Random.h"

namespace ad_utility {
/*
 * Manager class for LocalBlankNodeIndex.
 */
class BlankNodeManager {
 public:
  static const uint blockSize_ = 1000;
  static constexpr uint64_t totalAvailableBlocks_ =
      (ValueId::maxIndex + 1) / blockSize_;

 private:
  // Int Generator yielding random block indices.
  SlowRandomIntGenerator<uint64_t> randBlockIndex_;
  // Mutex for Block allocation.
  std::mutex mtx_;

 protected:
  // Tracks blocks currently hold by instances of `LocalBlankNodeManager`.
  HashSet<uint64_t> usedBlocksSet_;

 public:
  explicit BlankNodeManager(
      SlowRandomIntGenerator<uint64_t> randomIntGenerator =
          SlowRandomIntGenerator<uint64_t>{0, totalAvailableBlocks_ - 1});
  ~BlankNodeManager() = default;

  // A Local BlankNode Ids Block of size `blockSize_`.
  struct Block {
    explicit Block(uint64_t blockIndex);
    ~Block();
    uint64_t blockIdx_;
    uint64_t nextIdx_;
  };

  // Manages the LocalBlankNodes used within a LocalVocab.
  class LocalBlankNodeManager {
   public:
    LocalBlankNodeManager() = default;
    ~LocalBlankNodeManager() = default;

    // Get a new id.
    [[nodiscard]] uint64_t getId();

   protected:
    // Reserved blocks.
    std::vector<BlankNodeManager::Block> blocks_;
  };

  // Allocate and retrieve a block of free ids.
  [[nodiscard]] Block allocateBlock();

  // Free a block of ids.
  void freeBlock(uint64_t blockIndex);
};

}  // namespace ad_utility

inline ad_utility::BlankNodeManager globalBlankNodeManager;
