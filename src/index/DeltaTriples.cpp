// Copyright 2023 - 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "index/DeltaTriples.h"

#include "absl/strings/str_cat.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"

// ____________________________________________________________________________
LocatedTriples::iterator& DeltaTriples::LocatedTripleHandles::forPermutation(
    Permutation::Enum permutation) {
  return handles_[static_cast<size_t>(permutation)];
}

// ____________________________________________________________________________
void DeltaTriples::clear() {
  triplesInserted_.clear();
  triplesDeleted_.clear();
  std::ranges::for_each(locatedTriplesPerBlock_,
                        &LocatedTriplesPerBlock::clear);
}

// ____________________________________________________________________________
std::vector<DeltaTriples::LocatedTripleHandles>
DeltaTriples::locateAndAddTriples(CancellationHandle cancellationHandle,
                                  std::span<const IdTriple<0>> idTriples,
                                  bool shouldExist) {
  std::array<std::vector<LocatedTriples::iterator>, Permutation::ALL.size()>
      intermediateHandles;
  for (auto permutation : Permutation::ALL) {
    auto& perm = index_.getImpl().getPermutation(permutation);
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        // TODO<qup42>: replace with `getAugmentedMetadata` once integration
        //  is done
        idTriples, perm.metaData().blockData(), perm.keyOrder(), shouldExist,
        cancellationHandle);
    cancellationHandle->throwIfCancelled();
    intermediateHandles[static_cast<size_t>(permutation)] =
        locatedTriplesPerBlock_[static_cast<size_t>(permutation)].add(
            locatedTriples);
    cancellationHandle->throwIfCancelled();
  }
  std::vector<DeltaTriples::LocatedTripleHandles> handles{idTriples.size()};
  for (auto permutation : Permutation::ALL) {
    for (size_t i = 0; i < idTriples.size(); i++) {
      handles[i].forPermutation(permutation) =
          intermediateHandles[static_cast<size_t>(permutation)][i];
    }
  }
  return handles;
}

// ____________________________________________________________________________
void DeltaTriples::eraseTripleInAllPermutations(LocatedTripleHandles& handles) {
  // Erase for all permutations.
  for (auto permutation : Permutation::ALL) {
    auto ltIter = handles.forPermutation(permutation);
    locatedTriplesPerBlock_[static_cast<int>(permutation)].erase(
        ltIter->blockIndex_, ltIter);
  }
}

// ____________________________________________________________________________
void DeltaTriples::insertTriples(CancellationHandle cancellationHandle,
                                 Triples triples) {
  LOG(DEBUG) << "Inserting"
             << " " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), true,
                    triplesInserted_, triplesDeleted_);
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriples(CancellationHandle cancellationHandle,
                                 Triples triples) {
  LOG(DEBUG) << "Deleting"
             << " " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), false,
                    triplesDeleted_, triplesInserted_);
}

// ____________________________________________________________________________
void DeltaTriples::addTriplesToLocalVocab(Triples& triples) {
  ad_utility::HashMap<Id, Id> blankNodeMap;
  // For the `id` (which has to be a `BlankNodeIndex`) return a local
  // blank node index that is managed by the `localVocab_`. If the `id` appeared
  // previously during the same `insert/delete` request, the same value is
  // returned as for the previous calls.
  auto getLocalBlankNode = [this, &blankNodeMap](Id id) {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::BlankNodeIndex);
    // If the same blank node appears multiple times in the same update
    // request, then we have to also consistently rewrite it.
    auto [it, newElement] = blankNodeMap.try_emplace(id, Id::makeUndefined());
    if (newElement) {
      it->second = Id::makeFromBlankNodeIndex(
          localVocab_.getBlankNodeIndex(index_.getBlankNodeManager()));
    }
    return it->second;
  };

  // Return true iff `idx` is a global blank node index that was part of the
  // initial index.
  auto isGlobalBlankNode =
      [minLocalBlankNode = index_.getBlankNodeManager()->minIndex_](
          BlankNodeIndex idx) { return idx.get() < minLocalBlankNode; };

  // TODO<joka921> Comment.
  auto convertLocalVocab = [this, isGlobalBlankNode,
                            &getLocalBlankNode](Id& id) {
    if (id.getDatatype() == Datatype::LocalVocabIndex) {
      id = Id::makeFromLocalVocabIndex(
          localVocab_.getIndexAndAddIfNotContained(*id.getLocalVocabIndex()));
    } else if (id.getDatatype() == Datatype::BlankNodeIndex) {
      auto idx = id.getBlankNodeIndex();
      if (isGlobalBlankNode(idx) ||
          localVocab_.isBlankNodeIndexContained(idx)) {
        // The ID is from the global index or already part of the local vocab.
        return;
      }
      id = getLocalBlankNode(id);
    }
  };
  std::ranges::for_each(triples, [&convertLocalVocab](IdTriple<0>& triple) {
    std::ranges::for_each(triple.ids_, convertLocalVocab);
    std::ranges::for_each(triple.payload_, convertLocalVocab);
  });
}

// ____________________________________________________________________________
void DeltaTriples::modifyTriplesImpl(CancellationHandle cancellationHandle,
                                     Triples triples, bool shouldExist,
                                     TriplesToHandlesMap& targetMap,
                                     TriplesToHandlesMap& inverseMap) {
  addTriplesToLocalVocab(triples);
  std::ranges::sort(triples);
  auto [first, last] = std::ranges::unique(triples);
  triples.erase(first, last);
  std::erase_if(triples, [&targetMap](const IdTriple<0>& triple) {
    return targetMap.contains(triple);
  });
  std::ranges::for_each(triples,
                        [this, &inverseMap](const IdTriple<0>& triple) {
                          auto handle = inverseMap.find(triple);
                          if (handle != inverseMap.end()) {
                            eraseTripleInAllPermutations(handle->second);
                            inverseMap.erase(triple);
                          }
                        });

  std::vector<LocatedTripleHandles> handles =
      locateAndAddTriples(std::move(cancellationHandle), triples, shouldExist);

  AD_CORRECTNESS_CHECK(triples.size() == handles.size());
  // TODO<qup42>: replace with std::views::zip in C++23
  for (size_t i = 0; i < triples.size(); i++) {
    targetMap.insert({triples[i], handles[i]});
  }
}

// ____________________________________________________________________________
const LocatedTriplesPerBlock& DeltaTriples::getLocatedTriplesPerBlock(
    Permutation::Enum permutation) const {
  return locatedTriplesPerBlock_[static_cast<int>(permutation)];
}
