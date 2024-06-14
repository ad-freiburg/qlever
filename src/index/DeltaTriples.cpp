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
DeltaTriples::locateTriplesInAllPermutations(
    const std::vector<IdTriple>& idTriples, bool shouldExist) {
  std::vector<DeltaTriples::LocatedTripleHandles> handles{idTriples.size()};
  auto handlespso = locatedTriplesPerBlockInPSO_.add(
      LocatedTriple::locateTriplesInPermutation(
          idTriples, index_.getImpl().PSO(), shouldExist));
  auto handlespos = locatedTriplesPerBlockInPOS_.add(
      LocatedTriple::locateTriplesInPermutation(
          idTriples, index_.getImpl().POS(), shouldExist));
  auto handlesspo = locatedTriplesPerBlockInSPO_.add(
      LocatedTriple::locateTriplesInPermutation(
          idTriples, index_.getImpl().SPO(), shouldExist));
  auto handlessop = locatedTriplesPerBlockInSOP_.add(
      LocatedTriple::locateTriplesInPermutation(
          idTriples, index_.getImpl().SOP(), shouldExist));
  auto handlesosp = locatedTriplesPerBlockInOSP_.add(
      LocatedTriple::locateTriplesInPermutation(
          idTriples, index_.getImpl().OSP(), shouldExist));
  auto handlesops = locatedTriplesPerBlockInOPS_.add(
      LocatedTriple::locateTriplesInPermutation(
          idTriples, index_.getImpl().OPS(), shouldExist));
  // TODO<qup42>: assert that all handles have the same length
  for (size_t i = 0; i < idTriples.size(); i++) {
    handles[i].forPSO = handlespso[i];
    handles[i].forPOS = handlespos[i];
    handles[i].forSPO = handlesspo[i];
    handles[i].forSOP = handlessop[i];
    handles[i].forOSP = handlesosp[i];
    handles[i].forOPS = handlesops[i];
  }
  return handles;
}

// ____________________________________________________________________________
void DeltaTriples::eraseTripleInAllPermutations(
    DeltaTriples::LocatedTripleHandles& handles) {
  // Helper lambda for erasing for one particular permutation.
  auto erase = [](LocatedTriples::iterator locatedTriple,
                  LocatedTriplesPerBlock& locatedTriplesPerBlock) {
    locatedTriplesPerBlock.erase(locatedTriple->blockIndex, locatedTriple);
  };
  // Now erase for all permutations.
  erase(handles.forPSO, locatedTriplesPerBlockInPSO_);
  erase(handles.forPOS, locatedTriplesPerBlockInPOS_);
  erase(handles.forSPO, locatedTriplesPerBlockInSPO_);
  erase(handles.forSOP, locatedTriplesPerBlockInSOP_);
  erase(handles.forOSP, locatedTriplesPerBlockInOSP_);
  erase(handles.forOPS, locatedTriplesPerBlockInOPS_);
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
      locateTriplesInAllPermutations(triples, true);

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
      locateTriplesInAllPermutations(triples, false);

  AD_CORRECTNESS_CHECK(triples.size() == handles.size());
  // TODO<qup42>: replace with std::views::zip in C++23
  for (size_t i = 0; i < triples.size(); i++) {
    triplesDeleted_.insert({triples[i], handles[i]});
  }
}

// ____________________________________________________________________________
const LocatedTriplesPerBlock& DeltaTriples::getTriplesWithPositionsPerBlock(
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
