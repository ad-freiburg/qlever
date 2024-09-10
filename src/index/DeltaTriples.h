// Copyright 2023 - 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "Permutation.h"
#include "engine/LocalVocab.h"
#include "global/IdTriple.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "index/LocatedTriples.h"
#include "parser/TurtleParser.h"
#include "util/HashSet.h"

// A class for maintaining triples that are inserted or deleted after index
// building, we call these delta triples. How it works in principle:
//
// 1. For each delta triple, find the block index in each permutation (see
// `LocatedTriple` in `index/LocatedTriples.h`).
//
// 2. For each permutation and each block, store a sorted list of the positions
// of the delta triples within that block (see `LocatedTriplesPerBlock` in
// `index/LocatedTriples.h`).
//
// 3. In the call of `PermutationImpl::scan`, use the respective lists to merge
// the relevant delta triples into the index scan result.
//
// NOTE: The delta triples currently do not go well together with CACHING. See
// the discussion at the end of this file.
class DeltaTriples {
 private:
  // The index to which these triples are added.
  const Index& index_;

  // The local vocabulary of the delta triples (they may have components,
  // which are not contained in the vocabulary of the original index).
  LocalVocab localVocab_;

  // The positions of the delta triples in each of the six permutations.
  std::array<LocatedTriplesPerBlock, Permutation::ALL.size()>
      locatedTriplesPerBlock_;

  FRIEND_TEST(DeltaTriplesTest, insertTriplesAndDeleteTriples);

  // Each delta triple needs to know where it is stored in each of the six
  // `LocatedTriplesPerBlock` above.
  struct LocatedTripleHandles {
    using It = LocatedTriples::iterator;
    static_assert(static_cast<int>(Permutation::Enum::PSO) == 0);
    static_assert(static_cast<int>(Permutation::Enum::POS) == 1);
    static_assert(static_cast<int>(Permutation::Enum::SPO) == 2);
    static_assert(static_cast<int>(Permutation::Enum::SOP) == 3);
    static_assert(static_cast<int>(Permutation::Enum::OPS) == 4);
    static_assert(static_cast<int>(Permutation::Enum::OSP) == 5);
    static_assert(Permutation::ALL.size() == 6);
    std::array<It, Permutation::ALL.size()> handles_;

    LocatedTriples::iterator& forPermutation(Permutation::Enum permutation);
  };

  // The sets of triples added to and subtracted from the original index. In
  // particular, no triple can be in both of these sets.
  ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles> triplesInserted_;
  ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles> triplesDeleted_;

 public:
  // Construct for given index.
  explicit DeltaTriples(const Index& index) : index_(index) {}

  // Get the common `LocalVocab` of the delta triples.
  LocalVocab& localVocab() { return localVocab_; }
  const LocalVocab& localVocab() const { return localVocab_; }

  // Clear `triplesAdded_` and `triplesSubtracted_` and all associated data
  // structures.
  void clear();

  // The number of delta triples added and subtracted.
  size_t numInserted() const { return triplesInserted_.size(); }
  size_t numDeleted() const { return triplesDeleted_.size(); }

  // Insert triples.
  void insertTriples(ad_utility::SharedCancellationHandle cancellationHandle,
                     std::vector<IdTriple<0>> triples);

  // Delete triples.
  void deleteTriples(ad_utility::SharedCancellationHandle cancellationHandle,
                     std::vector<IdTriple<0>> triples);

  // Get `TripleWithPosition` objects for given permutation.
  LocatedTriplesPerBlock& getLocatedTriplesPerBlock(
      Permutation::Enum permutation);
  const LocatedTriplesPerBlock& getLocatedTriplesPerBlock(
      Permutation::Enum permutation) const;

 private:
  // Find the position of the given triple in the given permutation and add it
  // to each of the six `LocatedTriplesPerBlock` maps (one per permutation).
  // Return the iterators of where it was added (so that we can easily delete it
  // again from these maps later).
  std::vector<LocatedTripleHandles> locateAndAddTriples(
      ad_utility::SharedCancellationHandle cancellationHandle,
      std::span<const IdTriple<0>> idTriples, bool shouldExist);

  // Implementation to actually insert triples. `shouldExist` specifies the
  // action: insert or delete. `targetMap` contains triples for the current
  // action. `inverseMap` contains triples for the inverse action. These are
  // then used to resolve idempotent actions and update the corresponding maps.
  void DeltaTriples::modifyTriplesImpl(
      ad_utility::SharedCancellationHandle cancellationHandle,
      std::vector<IdTriple<0>> triples, bool shouldExist,
      ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles>& targetMap,
      ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles>& inverseMap);

  // Erase `LocatedTriple` object from each `LocatedTriplesPerBlock` list. The
  // argument are iterators for each list, as returned by the method
  // `locateTripleInAllPermutations` above.
  //
  // NOTE: The iterators are invalid afterward. That is OK, as long as we also
  // delete the respective entry in `triplesInserted_` or `triplesDeleted_`,
  // which stores these iterators.
  void eraseTripleInAllPermutations(LocatedTripleHandles& handles);
};

// DELTA TRIPLES AND THE CACHE
//
// For now, our approach only works when the results of index scans are not
// cached (unless there are no relevant delta triples for a particular scan).
// There are two ways how this can play out in the future:
//
// Either we generally do not cache the results of index scans anymore. This
// would have various advantages, in particular, joining with something like
// `rdf:type` would then be possible without storing the whole relation in
// RAM. However, we need a faster decompression then and maybe a smaller block
// size (currently 8 MB).
//
// Or we add the delta triples when iterating over the cached (uncompressed)
// result from the index scan. In that case, we would need to (in Step 1 above)
// store and maintain the positions in those uncompressed index scans. However,
// this would only work for the results of index scans. For the results of more
// complex subqueries, it's hard to figure out which delta triples are relevant.
