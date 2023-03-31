// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "index/DeltaTriples.h"

#include "absl/strings/str_cat.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"
#include "parser/TurtleParser.h"
#include "util/Timer.h"

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
DeltaTriples::LocatedTripleHandles DeltaTriples::locateTripleInAllPermutations(
    const IdTriple& idTriple) {
  auto [s, p, o] = idTriple;
  LocatedTripleHandles handles;
  handles.forPSO =
      locatedTriplesPerBlockInPSO_.add(LocatedTriple::locateTripleInPermutation(
          p, s, o, index_.getImpl().PSO()));
  handles.forPOS =
      locatedTriplesPerBlockInPOS_.add(LocatedTriple::locateTripleInPermutation(
          p, o, s, index_.getImpl().POS()));
  handles.forSPO =
      locatedTriplesPerBlockInSPO_.add(LocatedTriple::locateTripleInPermutation(
          s, p, o, index_.getImpl().SPO()));
  handles.forSOP =
      locatedTriplesPerBlockInSOP_.add(LocatedTriple::locateTripleInPermutation(
          s, o, p, index_.getImpl().SOP()));
  handles.forOSP =
      locatedTriplesPerBlockInOSP_.add(LocatedTriple::locateTripleInPermutation(
          o, s, p, index_.getImpl().OSP()));
  handles.forOPS =
      locatedTriplesPerBlockInOPS_.add(LocatedTriple::locateTripleInPermutation(
          o, p, s, index_.getImpl().OPS()));
  return handles;
}

// ____________________________________________________________________________
void DeltaTriples::eraseTripleInAllPermutations(
    DeltaTriples::LocatedTripleHandles& handles) {
  // Helper lambda for erasing for one particular permutation.
  auto erase = [](LocatedTriples::iterator locatedTriple,
                  LocatedTriplesPerBlock& locatedTriplesPerBlock) {
    size_t blockIndex = locatedTriple->blockIndex;
    locatedTriplesPerBlock.map_[blockIndex].erase(locatedTriple);
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
void DeltaTriples::insertTriple(TurtleTriple turtleTriple) {
  IdTriple idTriple = getIdTriple(turtleTriple);
  // Inserting a triple (that does not exist in the original index) a second
  // time has no effect.
  //
  // TODO: Test this behavior.
  if (triplesInserted_.contains(idTriple)) {
    throw std::runtime_error(absl::StrCat(
        "Triple \"", turtleTriple.toString(), "\" was already inserted before",
        ", this insertion therefore has no effect"));
  }
  // When re-inserting a previously deleted triple, we need to remove the triple
  // from `triplesDeleted_` AND remove it from all
  // `locatedTriplesPerBlock` (one per permutation) as well.
  if (triplesDeleted_.contains(idTriple)) {
    eraseTripleInAllPermutations(triplesDeleted_.at(idTriple));
    triplesDeleted_.erase(idTriple);
    return;
  }
  // Locate the triple in one of the permutations (it does not matter which one)
  // to check if it already exists in the index. If it already exists, the
  // insertion is invalid, otherwise insert it.
  LocatedTriple locatedTriple = LocatedTriple::locateTripleInPermutation(
      idTriple[1], idTriple[0], idTriple[2], index_.getImpl().PSO());
  if (locatedTriple.existsInIndex) {
    throw std::runtime_error(
        absl::StrCat("Triple \"", turtleTriple.toString(),
                     "\" already exists in the original index",
                     ", this insertion therefore has no effect"));
  }
  auto iterators = locateTripleInAllPermutations(idTriple);
  triplesInserted_.insert({idTriple, iterators});
}

// ____________________________________________________________________________
void DeltaTriples::deleteTriple(TurtleTriple turtleTriple) {
  IdTriple idTriple = getIdTriple(turtleTriple);
  // Deleting a triple (that does exist in the original index) a second time has
  // no effect.
  //
  // TODO: Test this behavior.
  if (triplesDeleted_.contains(idTriple)) {
    throw std::runtime_error(absl::StrCat(
        "Triple \"", turtleTriple.toString(), "\" was already deleted before",
        ", this deletion therefore has no effect"));
  }
  // When deleting a previously inserted triple (that did not exist in the index
  // before), we need to remove the triple from `triplesInserted_` AND remove it
  // from all `locatedTriplesPerBlock` (one per permutation) as well.
  if (triplesInserted_.contains(idTriple)) {
    eraseTripleInAllPermutations(triplesInserted_.at(idTriple));
    triplesInserted_.erase(idTriple);
    return;
  }
  // Locate the triple in one of the permutations (it does not matter which one)
  // to check if it actually exists in the index. If it does not exist, the
  // deletion is invalid, otherwise add as deleted triple.
  LocatedTriple locatedTriple = LocatedTriple::locateTripleInPermutation(
      idTriple[1], idTriple[0], idTriple[2], index_.getImpl().PSO());
  if (!locatedTriple.existsInIndex) {
    throw std::runtime_error(
        absl::StrCat("Triple \"", turtleTriple.toString(),
                     "\" does not exist in the original index",
                     ", the deletion has no effect"));
  }
  auto iterators = locateTripleInAllPermutations(idTriple);
  triplesDeleted_.insert({idTriple, iterators});
}

// ____________________________________________________________________________
const LocatedTriplesPerBlock& DeltaTriples::getTriplesWithPositionsPerBlock(
    Index::Permutation permutation) const {
  switch (permutation) {
    case Index::Permutation::PSO:
      return locatedTriplesPerBlockInPSO_;
    case Index::Permutation::POS:
      return locatedTriplesPerBlockInPOS_;
    case Index::Permutation::SPO:
      return locatedTriplesPerBlockInSPO_;
    case Index::Permutation::SOP:
      return locatedTriplesPerBlockInSOP_;
    case Index::Permutation::OSP:
      return locatedTriplesPerBlockInOSP_;
    case Index::Permutation::OPS:
      return locatedTriplesPerBlockInOPS_;
    default:
      AD_FAIL();
  }
}

// ____________________________________________________________________________
IdTriple DeltaTriples::getIdTriple(const TurtleTriple& turtleTriple) {
  TripleComponent subject = std::move(turtleTriple._subject);
  TripleComponent predicate = std::move(turtleTriple._predicate);
  TripleComponent object = std::move(turtleTriple._object);
  Id subjectId = std::move(subject).toValueId(index_.getVocab(), localVocab_);
  Id predId = std::move(predicate).toValueId(index_.getVocab(), localVocab_);
  Id objectId = std::move(object).toValueId(index_.getVocab(), localVocab_);
  return IdTriple{subjectId, predId, objectId};
}
