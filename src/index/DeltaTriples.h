// Copyright 2023 - 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "engine/LocalVocab.h"
#include "global/IdTriple.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"

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
  FRIEND_TEST(DeltaTriplesTest, insertTriplesAndDeleteTriples);
  FRIEND_TEST(DeltaTriplesTest, clear);
  FRIEND_TEST(DeltaTriplesTest, addTriplesToLocalVocab);

 private:
  // The index to which these triples are added.
  const Index& index_;

  // The local vocabulary of the delta triples (they may have components,
  // which are not contained in the vocabulary of the original index).
  LocalVocab localVocab_;

  // Assert that the Permutation Enum values have the expected int values.
  // This is used to store and lookup items that exist for permutation in an
  // array.
  static_assert(static_cast<int>(Permutation::Enum::PSO) == 0);
  static_assert(static_cast<int>(Permutation::Enum::POS) == 1);
  static_assert(static_cast<int>(Permutation::Enum::SPO) == 2);
  static_assert(static_cast<int>(Permutation::Enum::SOP) == 3);
  static_assert(static_cast<int>(Permutation::Enum::OPS) == 4);
  static_assert(static_cast<int>(Permutation::Enum::OSP) == 5);
  static_assert(Permutation::ALL.size() == 6);

  // The positions of the delta triples in each of the six permutations.
  std::array<LocatedTriplesPerBlock, Permutation::ALL.size()>
      locatedTriplesPerBlock_;

  // Each delta triple needs to know where it is stored in each of the six
  // `LocatedTriplesPerBlock` above.
  struct LocatedTripleHandles {
    using It = LocatedTriples::iterator;
    std::array<It, Permutation::ALL.size()> handles_;

    LocatedTriples::iterator& forPermutation(Permutation::Enum permutation);
  };
  using TriplesToHandlesMap =
      ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles>;
  using Triples = std::vector<IdTriple<0>>;
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  // The sets of triples added to and subtracted from the original index. Any
  // triple can be at most in one of the sets. The information whether a triple
  // is in the index is missing. This means that a triple that is in the index
  // may still be in the inserted set and vice versa.
  TriplesToHandlesMap triplesInserted_;
  TriplesToHandlesMap triplesDeleted_;

 public:
  // Construct for given index.
  explicit DeltaTriples(const Index& index) : index_(index) {}

  // Get the common `LocalVocab` of the delta triples.
 private:
  LocalVocab& localVocab() { return localVocab_; }

 public:
  const LocalVocab& localVocab() const { return localVocab_; }

  // Clear `triplesAdded_` and `triplesSubtracted_` and all associated data
  // structures.
  void clear();

  // The number of delta triples added and subtracted.
  size_t numInserted() const { return triplesInserted_.size(); }
  size_t numDeleted() const { return triplesDeleted_.size(); }

  // Insert triples.
  void insertTriples(CancellationHandle cancellationHandle, Triples triples);

  // Delete triples.
  void deleteTriples(CancellationHandle cancellationHandle, Triples triples);

  // Get `TripleWithPosition` objects for given permutation.
  const LocatedTriplesPerBlock& getLocatedTriplesPerBlock(
      Permutation::Enum permutation) const;

 private:
  // Find the position of the given triple in the given permutation and add it
  // to each of the six `LocatedTriplesPerBlock` maps (one per permutation).
  // `shouldExist` specifies the action: insert or delete. Return the iterators
  // of where it was added (so that we can easily delete it again from these
  // maps later).
  std::vector<LocatedTripleHandles> locateAndAddTriples(
      CancellationHandle cancellationHandle,
      std::span<const IdTriple<0>> idTriples, bool shouldExist);

  // Common implementation for `insertTriples` and `deleteTriples`.
  // `shouldExist` specifies the action: insert or delete. `targetMap` contains
  // triples for the current action. `inverseMap` contains triples for the
  // inverse action. These are then used to resolve idempotent actions and
  // update the corresponding maps.
  void modifyTriplesImpl(CancellationHandle cancellationHandle, Triples triples,
                         bool shouldExist, TriplesToHandlesMap& targetMap,
                         TriplesToHandlesMap& inverseMap);

  // Rewrite each triple in `triples` such that all local vocab entries and all
  // local blank nodes are managed by the `localVocab_` of this class.
  //
  // NOTE: This is important for two reasons: (1) It avoids duplicates for
  // successive insertions referring to the same local vocab entries; (2) It
  // avoids storing local vocab entries or blank nodes that were created only
  // temporarily when evaluating the WHERE clause of an update query.
  void rewriteLocalVocabEntriesAndBlankNodes(Triples& triples);
  FRIEND_TEST(DeltaTriplesTest, rewriteLocalVocabEntriesAndBlankNodes);

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
// Changes to the DeltaTriples invalidate all cache results that have an index
// scan in their subtree, which is almost all entries in practice.
