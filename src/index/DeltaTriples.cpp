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
  return handles_[static_cast<int>(permutation)];
}

// ____________________________________________________________________________
void DeltaTriples::clear() {
  triplesInserted_.clear();
  triplesDeleted_.clear();
  for (auto& perm : Permutation::ALL) {
    getLocatedTriplesPerBlock(perm).clear();
  }
}

// ____________________________________________________________________________
std::vector<DeltaTriples::LocatedTripleHandles>
DeltaTriples::locateAndAddTriples(
    ad_utility::SharedCancellationHandle cancellationHandle,
    std::span<const IdTriple<0>> idTriples, bool shouldExist) {
  ad_utility::HashMap<Permutation::Enum, std::vector<LocatedTriples::iterator>>
      intermediateHandles;
  for (auto permutation : Permutation::ALL) {
    auto& perm = index_.getImpl().getPermutation(permutation);
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        idTriples, perm.augmentedBlockData(), perm.keyOrder(), shouldExist,
        cancellationHandle);
    cancellationHandle->throwIfCancelled();
    intermediateHandles[permutation] =
        getLocatedTriplesPerBlock(permutation).add(locatedTriples);
    cancellationHandle->throwIfCancelled();
  }
  std::vector<DeltaTriples::LocatedTripleHandles> handles{idTriples.size()};
  for (auto permutation : Permutation::ALL) {
    for (size_t i = 0; i < idTriples.size(); i++) {
      handles[i].forPermutation(permutation) =
          intermediateHandles[permutation][i];
    }
  }
  return handles;
}

// ____________________________________________________________________________
void DeltaTriples::eraseTripleInAllPermutations(
    DeltaTriples::LocatedTripleHandles& handles) {
  // Erase for all permutations.
  for (auto permutation : Permutation::ALL) {
    auto ltIter = handles.forPermutation(permutation);
    getLocatedTriplesPerBlock(permutation).erase(ltIter->blockIndex_, ltIter);
  }
}

// ____________________________________________________________________________
void DeltaTriples::insertTriples(
    ad_utility::SharedCancellationHandle cancellationHandle,
    std::vector<IdTriple<0>> triples) {
  LOG(DEBUG) << "Inserting"
             << " " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), true,
                    triplesInserted_, triplesDeleted_);
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriples(
    ad_utility::SharedCancellationHandle cancellationHandle,
    std::vector<IdTriple<0>> triples) {
  LOG(DEBUG) << "Deleting"
             << " " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  modifyTriplesImpl(std::move(cancellationHandle), std::move(triples), false,
                    triplesDeleted_, triplesInserted_);
}

// ____________________________________________________________________________
void DeltaTriples::modifyTriplesImpl(
    ad_utility::SharedCancellationHandle cancellationHandle,
    std::vector<IdTriple<0>> triples, bool shouldExist,
    ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles>& targetMap,
    ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles>& inverseMap) {
  std::ranges::sort(triples);
  auto [first, last] = std::ranges::unique(triples);
  triples.erase(first, last);
  std::erase_if(triples, [this, &targetMap](const IdTriple<0>& triple) {
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

// ____________________________________________________________________________
LocatedTriplesPerBlock& DeltaTriples::getLocatedTriplesPerBlock(
    Permutation::Enum permutation) {
  return locatedTriplesPerBlock_[static_cast<int>(permutation)];
}
