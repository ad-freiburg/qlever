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

#include "global/IndexTypes.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "index/IndexRebuilderTypes.h"
#include "util/CancellationHandle.h"

namespace qlever {

namespace indexRebuilder {

// Map old vocab `Id`s to new vocab `Id`s according to the given
// `insertionPositions`. This is the most performance critical code of the
// rebuild.
//
// There are two overloads:
//
// (1) Stateless - performs a fresh `std::upper_bound` on every call. Use this
//     when consecutive queries have no predictable relationship (e.g. when
//     the input id stream is in arbitrary order).
//
// (2) Hinted - takes an in/out `hint` that the caller carries across calls.
//     The hint stores the offset returned by the previous call. On each
//     call we first check whether the hint is still the right answer and
//     whether `hint + 1` is, before falling back to a full `upper_bound`
//     (which then refreshes the hint). This gives a ~10-15x speedup when
//     consecutive queries are monotone-non-decreasing or repeat the same
//     value, and is essentially as fast as the stateless variant on
//     fully random input. Initialize `hint` to `0` for the first call;
//     do NOT reset it between calls (it self-corrects).
Id remapVocabId(Id original, const InsertionPositions& insertionPositions);
Id remapVocabId(Id original, const InsertionPositions& insertionPositions,
                size_t& hint);

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
