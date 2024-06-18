// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

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
      return forPSO;
    case Permutation::POS:
      return forPOS;
    case Permutation::SPO:
      return forSPO;
    case Permutation::SOP:
      return forSOP;
    case Permutation::OSP:
      return forOSP;
    case Permutation::OPS:
      return forOPS;
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
DeltaTriples::locateAndAddTriples(const std::vector<IdTriple>& idTriples,
                                  bool shouldExist) {
  ad_utility::HashMap<Permutation::Enum, std::vector<LocatedTriples::iterator>>
      intermediateHandles;
  for (auto permutation : Permutation::ALL) {
    auto locatedTriples = LocatedTriple::locateTriplesInPermutation(
        idTriples, index_.getImpl().getPermutation(permutation), shouldExist);
    intermediateHandles[permutation] =
        getLocatedTriplesPerBlock(permutation).add(locatedTriples);
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
    auto lt_iter = handles.forPermutation(permutation);
    getLocatedTriplesPerBlock(permutation).erase(lt_iter->blockIndex_, lt_iter);
  }
};

// ____________________________________________________________________________
void DeltaTriples::insertTriples(
    ad_utility::SharedCancellationHandle cancellationHandle,
    std::vector<IdTriple> triples) {
  // TODO<qup42> use canellationHandle
  (void)cancellationHandle;
  // TODO<qup42> add elimination of duplicate triples?
  LOG(INFO) << "Inserting " << triples.size() << " triples." << std::endl;
  std::erase_if(triples, [this](const IdTriple& triple) {
    return triplesInserted_.contains(triple);
  });
  std::erase_if(triples, [this](const IdTriple triple) {
    bool isPresent = triplesDeleted_.contains(triple);
    if (isPresent) {
      eraseTripleInAllPermutations(triplesDeleted_.at(triple));
      triplesDeleted_.erase(triple);
    }
    return isPresent;
  });

  LOG(INFO) << "Inserting " << triples.size() << " triples." << std::endl;

  std::vector<LocatedTripleHandles> handles =
      locateAndAddTriples(triples, true);

  AD_CORRECTNESS_CHECK(triples.size() == handles.size());
  // TODO<qup42>: replace with std::views::zip in C++23
  for (size_t i = 0; i < triples.size(); i++) {
    triplesInserted_.insert({triples[i], handles[i]});
  }
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriples(
    ad_utility::SharedCancellationHandle cancellationHandle,
    std::vector<IdTriple> triples) {
  // TODO<qup42> use canellationHandle
  (void)cancellationHandle;
  // TODO<qup42> add elimination of duplicate triples?
  LOG(INFO) << "Deleting " << triples.size() << " triples." << std::endl;
  std::erase_if(triples, [this](const IdTriple& triple) {
    return triplesDeleted_.contains(triple);
  });
  std::erase_if(triples, [this](const IdTriple triple) {
    bool isPresent = triplesInserted_.contains(triple);
    if (isPresent) {
      eraseTripleInAllPermutations(triplesInserted_.at(triple));
      triplesInserted_.erase(triple);
    }
    return isPresent;
  });

  LOG(INFO) << "Deleting " << triples.size() << " triples." << std::endl;

  std::vector<LocatedTripleHandles> handles =
      locateAndAddTriples(triples, false);

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
