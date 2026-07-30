// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_AUXINDEXBUILDER_H
#define QLEVER_SRC_INDEX_AUXINDEXBUILDER_H

#include <string>
#include <vector>

#include "global/Id.h"
#include "index/AuxIndex.h"
#include "util/CancellationHandle.h"
#include "util/HashMap.h"
#include "util/MemorySize/MemorySize.h"

class IndexImpl;
struct LocatedTriplesState;

namespace qlever {

// The mapping that translates the `Id`s that the delta triples used *before* a
// build of the auxiliary index into the `Id`s of the newly built generation. It
// is required to carry over the updates that arrived while the build was
// running (see `DeltaTriples::addFromSnapshotDiff`), because building an
// auxiliary index moves words out of the local vocabulary and out of the
// previous generation of the auxiliary vocabulary into the new one.
class AuxIndexIdMapping {
 private:
  // For each offset (see `AuxVocabulary`) of the *previous* generation of the
  // auxiliary vocabulary, the `Id` that its word has in the new generation.
  std::vector<Id> newIdForOldAuxOffset_;
  // For each local vocab entry that was used by the delta triples, the `Id`
  // that its word has now. This is an `Id` of the new auxiliary vocabulary, or
  // an `Id` of the main vocabulary resp. an encoded `Id` if the word turned out
  // to be representable there.
  ad_utility::HashMap<LocalVocabIndex, Id> newIdForLocalVocabEntry_;
  // The previous generation of the auxiliary index, or `nullptr` if there was
  // none. Required to turn an `Id` of that generation into an offset.
  const AuxVocabulary* previousAuxVocab_ = nullptr;

 public:
  AuxIndexIdMapping() = default;
  AuxIndexIdMapping(std::vector<Id> newIdForOldAuxOffset,
                    ad_utility::HashMap<LocalVocabIndex, Id> newIdForLocalVocab,
                    const AuxVocabulary* previousAuxVocab)
      : newIdForOldAuxOffset_{std::move(newIdForOldAuxOffset)},
        newIdForLocalVocabEntry_{std::move(newIdForLocalVocab)},
        previousAuxVocab_{previousAuxVocab} {}

  // Map `id` into the `Id` space of the new generation. `Id`s that are not
  // affected (everything but the `Id`s of the previous auxiliary vocabulary and
  // the local vocab entries) are returned unchanged. If a local vocab entry is
  // not part of this mapping (because it was added after the mapping was
  // created), `std::nullopt` is returned: such an `Id` has to be re-created in
  // the local vocabulary of the new index instead.
  std::optional<Id> map(Id id) const;

  // The number of local vocab entries that this mapping covers.
  size_t numLocalVocabEntries() const {
    return newIdForLocalVocabEntry_.size();
  }
};

// The result of building a new generation of the auxiliary index.
struct AuxIndexBuildResult {
  // The base name of the files of the new generation.
  std::string basename_;
  // The metadata of the new generation.
  AuxIndexMetadata metadata_;
  // The mapping into the `Id` space of the new generation, see above.
  AuxIndexIdMapping idMapping_;
};

// Build a new generation of the auxiliary index of `index` from the delta
// triples in `locatedTriplesState` merged with the current generation of the
// auxiliary index of `index` (if it has one). The delta triples take
// precedence: if both of them have an entry for the same triple, the one from
// the delta triples wins, because it is the more recent one.
//
// Insertions of triples that the main index already contains and deletions of
// triples that it does not contain are *not* removed, they are simply carried
// over. (Removing them requires a lookup in the main index for each triple;
// this is what `?cmd=vacuum-delta-triples` does and it is deliberately kept
// separate.)
//
// The caller has to keep the words of the local vocabulary that
// `locatedTriplesState` refers to alive for the duration of the call, see
// `DeltaTriplesManager::getCurrentLocatedTriplesSharedStateWithVocab`.
//
// The new generation is only usable once its metadata file exists, which is
// written last, so an interrupted or failed build leaves behind files that are
// ignored (and that the caller should delete via `AuxIndex::deleteFromDisk`).
AuxIndexBuildResult buildAuxIndex(
    const IndexImpl& index, const LocatedTriplesState& locatedTriplesState,
    ad_utility::MemorySize memoryLimit,
    const ad_utility::SharedCancellationHandle& cancellationHandle);

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_AUXINDEXBUILDER_H
