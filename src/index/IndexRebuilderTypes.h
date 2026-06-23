//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_INDEX_INDEXREBUILDERTYPES_H
#define QLEVER_SRC_INDEX_INDEXREBUILDERTYPES_H

#include <vector>

#include "global/Id.h"
#include "global/IndexTypes.h"
#include "util/BPlusTree.h"
#include "util/BlankNodeManager.h"
#include "util/HashMap.h"

namespace qlever::indexRebuilder {
using OwnedBlocksEntry =
    ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry;
using OwnedBlocks = std::vector<OwnedBlocksEntry>;
using InsertionPositions = std::vector<VocabIndex>;
using LocalVocabMapping = ad_utility::HashMap<Id::T, Id>;
using BlankNodeBlocks = std::vector<uint64_t>;

// Helper struct that groups together the data required to remap IDs from the
// old index to the new index after a rebuild.
struct IndexRebuildMapping {
  InsertionPositions insertionPositions_;
  LocalVocabMapping localVocabMapping_;
  BlankNodeBlocks blankNodeBlocks_;
  uint64_t minBlankNodeIndex_;
  // B+ tree built from `insertionPositions_` for fast SIMD-accelerated
  // `upper_bound` lookups in `remapVocabId`.
  ad_utility::BPlusTree<uint64_t> insertionPositionsTree_;
};
}  // namespace qlever::indexRebuilder

#endif  // QLEVER_SRC_INDEX_INDEXREBUILDERTYPES_H
