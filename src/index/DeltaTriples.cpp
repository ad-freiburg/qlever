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
#include "parser/TurtleParser.h"

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
        // TODO<qup42>: replace with the method for update block metadata once
        // integration is done
        idTriples, perm.metaData().blockData(), perm.keyOrder(), shouldExist,
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
  LOG(DEBUG) << "Inserting " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  std::ranges::sort(triples);
  // Unique moves all duplicate items to the end and returns iterators for that
  // subrange.
  auto [first, last] = std::ranges::unique(triples);
  triples.erase(first, last);
  std::erase_if(triples, [this](const IdTriple<0>& triple) {
    return triplesInserted_.contains(triple);
  });
  std::ranges::for_each(triples, [this](const IdTriple<0>& triple) {
    auto handle = triplesDeleted_.find(triple);
    if (handle != triplesDeleted_.end()) {
      eraseTripleInAllPermutations(handle->second);
      triplesDeleted_.erase(triple);
    }
  });

  std::vector<LocatedTripleHandles> handles =
      locateAndAddTriples(std::move(cancellationHandle), triples, true);

  AD_CORRECTNESS_CHECK(triples.size() == handles.size());
  // TODO<qup42>: replace with std::views::zip in C++23
  for (size_t i = 0; i < triples.size(); i++) {
    triplesInserted_.insert({triples[i], handles[i]});
  }
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriples(
    ad_utility::SharedCancellationHandle cancellationHandle,
    std::vector<IdTriple<0>> triples) {
  LOG(DEBUG) << "Deleting " << triples.size()
             << " triples (including idempotent triples)." << std::endl;
  std::ranges::sort(triples);
  auto [first, last] = std::ranges::unique(triples);
  triples.erase(first, last);
  std::erase_if(triples, [this](const IdTriple<0>& triple) {
    return triplesDeleted_.contains(triple);
  });
  std::ranges::for_each(triples, [this](const IdTriple<0>& triple) {
    auto handle = triplesInserted_.find(triple);
    if (handle != triplesInserted_.end()) {
      eraseTripleInAllPermutations(handle->second);
      triplesInserted_.erase(triple);
    }
  });

  std::vector<LocatedTripleHandles> handles =
      locateAndAddTriples(std::move(cancellationHandle), triples, false);

  AD_CORRECTNESS_CHECK(triples.size() == handles.size());
  // TODO<qup42>: replace with std::views::zip in C++23
  for (size_t i = 0; i < triples.size(); i++) {
    triplesDeleted_.insert({triples[i], handles[i]});
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
