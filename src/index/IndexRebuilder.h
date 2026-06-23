//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXREBUILDER_H
#define QLEVER_SRC_INDEX_INDEXREBUILDER_H

#include <optional>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "global/Id.h"
#include "global/IndexTypes.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "index/IndexRebuilderTypes.h"
#include "util/CancellationHandle.h"
#include "util/CompilerExtensions.h"
#include "util/Exception.h"

namespace qlever {

namespace indexRebuilder {

// Map old vocab `Id`s to new vocab `Id`s according to the given
// `insertionPositions`. This is the  most performance critical code of the
// rebuild.
// Defined in the header (and force-inlined) on purpose: it is called from
// several translation units, and force-inlining a function defined in a `.cpp`
// is not possible (the body has to be visible at every call site).
AD_ALWAYS_INLINE Id remapVocabId(Id original,
                                 const InsertionPositions& insertionPositions) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::VocabIndex,
      "Only ids resembling a vocab index can be remapped with this function.");
  size_t offset = ql::ranges::distance(
      insertionPositions.begin(),
      ql::ranges::upper_bound(insertionPositions, original.getVocabIndex(),
                              std::less{}));
  return Id::makeFromVocabIndex(
      VocabIndex::make(original.getVocabIndex().get() + offset));
}

// Remaps a blank node `Id` to another blank node `Id` to reduce the gaps in the
// id space left by random allocation of blank node ids. Return an empty
// optional if the blank node cannot be remapped given the provided mapping.
std::optional<Id> tryRemapBlankNodeId(Id original,
                                      const BlankNodeBlocks& blankNodeBlocks,
                                      uint64_t minBlankNodeIndex);

// Remaps a blank node `Id` to another blank node `Id` to reduce the gaps in the
// id space left by random allocation of blank node ids.
Id remapBlankNodeId(Id original, const BlankNodeBlocks& blankNodeBlocks,
                    uint64_t minBlankNodeIndex);
}  // namespace indexRebuilder

// Build a new index based on the existing state of the engine.
// The new index will be written at the path specified by `newIndexName`.
// The progress of this operation will be logged to the file specified by
// `logFileName` (even though some progress will be visible in non-deterministic
// order in the main log due to concurrency).
// `locatedTriplesSharedState`, `entries`, and `ownedBlocks` are the state of
// the engine that is relevant for the rebuild and that is needed to build the
// new index.
// `cancellationHandle` can be used to cancel the rebuild. In this case, the new
// index will be left in an incomplete state and should be deleted by the
// caller.
// Return the datastructures used for mapping to be used in further
// post-processing.
indexRebuilder::IndexRebuildMapping materializeToIndex(
    const IndexImpl& index, const std::string& newIndexName,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const std::vector<LocalVocabIndex>& entries,
    const indexRebuilder::OwnedBlocks& ownedBlocks,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    const std::string& logFileName);

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INDEXREBUILDER_H
