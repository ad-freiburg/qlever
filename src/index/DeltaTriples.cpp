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
  switch (permutation) {
    case Permutation::PSO:
      return forPSO_;
    case Permutation::POS:
      return forPOS_;
    case Permutation::SPO:
      return forSPO_;
    case Permutation::SOP:
      return forSOP_;
    case Permutation::OSP:
      return forOSP_;
    case Permutation::OPS:
      return forOPS_;
    default:
      AD_FAIL();
  }
}

// ____________________________________________________________________________
void DeltaTriples::clear() {
  triplesInserted_.clear();
  triplesDeleted_.clear();
  locatedTriplesPerBlockInPSO_.clear();
  locatedTriplesPerBlockInPOS_.clear();
  locatedTriplesPerBlockInSPO_.clear();
  locatedTriplesPerBlockInSOP_.clear();
  locatedTriplesPerBlockInOSP_.clear();
  locatedTriplesPerBlockInOPS_.clear();
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
};

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
  // TODO: This `switch` would no longer be needed if the six
  // locatedTriplesPerBlockIn... were a map with the permutation as key.
  switch (permutation) {
    case Permutation::PSO:
      return locatedTriplesPerBlockInPSO_;
    case Permutation::POS:
      return locatedTriplesPerBlockInPOS_;
    case Permutation::SPO:
      return locatedTriplesPerBlockInSPO_;
    case Permutation::SOP:
      return locatedTriplesPerBlockInSOP_;
    case Permutation::OSP:
      return locatedTriplesPerBlockInOSP_;
    case Permutation::OPS:
      return locatedTriplesPerBlockInOPS_;
    default:
      AD_FAIL();
  }
}
LocatedTriplesPerBlock& DeltaTriples::getLocatedTriplesPerBlock(
    Permutation::Enum permutation) {
  // TODO: This `switch` would no longer be needed if the six
  // locatedTriplesPerBlockIn... were a map with the permutation as key.
  switch (permutation) {
    case Permutation::PSO:
      return locatedTriplesPerBlockInPSO_;
    case Permutation::POS:
      return locatedTriplesPerBlockInPOS_;
    case Permutation::SPO:
      return locatedTriplesPerBlockInSPO_;
    case Permutation::SOP:
      return locatedTriplesPerBlockInSOP_;
    case Permutation::OSP:
      return locatedTriplesPerBlockInOSP_;
    case Permutation::OPS:
      return locatedTriplesPerBlockInOPS_;
    default:
      AD_FAIL();
  }
}
