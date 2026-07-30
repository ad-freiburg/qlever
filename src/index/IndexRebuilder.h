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

namespace detail {
// Compute by what offset `value` needs to be increased to fit in the new index.
AD_ALWAYS_INLINE size_t computeIndexOffset(
    VocabIndex value, const InsertionPositions& insertionPositions) {
  return ql::ranges::distance(
      insertionPositions.begin(),
      ql::ranges::upper_bound(insertionPositions, value, std::less{}));
}

// Apply `offset` to `value` and return the new `Id` resulting from this.
AD_ALWAYS_INLINE Id applyOffset(VocabIndex value, size_t offset) {
  return Id::makeFromVocabIndex(VocabIndex::make(value.get() + offset));
}
}  // namespace detail

// Map old vocab `Id`s to new vocab `Id`s according to the given
// `insertionPositions`. This is the most performance critical code of the
// rebuild; the definitions live in this header so that the forced inlining
// also works for callers in other translation units.
// If the extra `hint` argument is provided, first optimistically check whether
// `hint` (confirmed against `hint - 1`) or `hint + 1` is already the right
// position before doing binary search. This can significantly speed up
// remapping when there are long sequences of ids that are mostly monotonically
// increasing. Once the call returns, `hint` will be updated to the position of
// the remapped id, so that the next call can use it as a hint.
AD_ALWAYS_INLINE Id remapVocabId(Id original,
                                 const InsertionPositions& insertionPositions) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::VocabIndex,
      "Only ids resembling a vocab index can be remapped with this function.");
  auto value = original.getVocabIndex();
  return detail::applyOffset(
      value, detail::computeIndexOffset(value, insertionPositions));
}

AD_ALWAYS_INLINE Id remapVocabId(Id original,
                                 const InsertionPositions& insertionPositions,
                                 size_t& hint) {
  AD_EXPENSIVE_CHECK(
      original.getDatatype() == Datatype::VocabIndex,
      "Only ids resembling a vocab index can be remapped with this function.");
  AD_EXPENSIVE_CHECK(hint <= insertionPositions.size(),
                     "Hint must be a valid index into the insertion positions "
                     "or equal to its size.");
  auto value = original.getVocabIndex();
  auto isUpperBound = [value, &insertionPositions](size_t candidate) {
    return candidate == insertionPositions.size() ||
           insertionPositions[candidate] > value;
  };

  // Update `hint` to the correct upper bound for `value`. Avoid writing `hint`
  // in cases where that's not necessary.
  [&hint, &isUpperBound, &value, &insertionPositions]() {
    // Check if the cached hint is still the upper bound for `value`.
    if (isUpperBound(hint)) [[likely]] {
      // `hint` is an upper bound, so check if `hint - 1` is not an upper bound.
      if (hint == 0 || !isUpperBound(hint - 1)) [[likely]] {
        // `hint` still is the correct upper bound, so there is nothing to do.
        return;
      }
    } else {
      // Check if `hint + 1` is an upper bound. This is the case when we just
      // move the hint forward by one position.
      size_t next = hint + 1;
      if (isUpperBound(next)) [[likely]] {
        hint = next;
        return;
      }
    }

    // Fallback and write the hint for the next iteration.
    hint = detail::computeIndexOffset(value, insertionPositions);
  }();

  return detail::applyOffset(value, hint);
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
