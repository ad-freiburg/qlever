//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_INDEX_INDEXREBUILDER_H
#define QLEVER_SRC_INDEX_INDEXREBUILDER_H

#include <string>
#include <vector>

#include "global/IndexTypes.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "util/BlankNodeManager.h"
#include "util/CancellationHandle.h"

namespace qlever {

// Build a new index based on this data.
void materializeToIndex(
    const IndexImpl& index, const std::string& newIndexName,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const std::vector<LocalVocabIndex>& entries,
    const std::vector<
        ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>&
        ownedBlocks,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    const std::string& logFileName);

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INDEXREBUILDER_H
