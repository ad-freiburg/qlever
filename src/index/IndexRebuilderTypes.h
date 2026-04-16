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
#include "util/BlankNodeManager.h"
#include "util/HashMap.h"

namespace qlever::indexRebuilder {
using OwnedBlocksEntry =
    ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry;
using OwnedBlocks = std::vector<OwnedBlocksEntry>;
using InsertionPositions = std::vector<VocabIndex>;
using LocalVocabMapping = ad_utility::HashMap<Id::T, Id>;
using BlankNodeBlocks = std::vector<uint64_t>;

struct MappingInformation {
  InsertionPositions insertionPositions_;
  LocalVocabMapping localVocabMapping_;
  BlankNodeBlocks blankNodeBlocks_;
};
}  // namespace qlever::indexRebuilder

#endif  // QLEVER_SRC_INDEX_INDEXREBUILDERTYPES_H
