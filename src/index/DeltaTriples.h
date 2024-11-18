// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/LocalVocab.h"
#include "global/IdTriple.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"
#include "util/Synchronized.h"

// Typedef for one `LocatedTriplesPerBlock` object for each of the six
// permutations.
using LocatedTriplesPerBlockAllPermutations =
    std::array<LocatedTriplesPerBlock, Permutation::ALL.size()>;

// The locations of a set of delta triples (triples that were inserted or
// deleted since the index was built) in each of the six permutations, and a
// local vocab. This is all the information that is required to perform a query
// that correctly respects these delta triples, hence the name.
struct LocatedTriplesSnapshot {
  LocatedTriplesPerBlockAllPermutations locatedTriplesPerBlock_;
  LocalVocab localVocab_;
  // Get `TripleWithPosition` objects for given permutation.
  const LocatedTriplesPerBlock& getLocatedTriplesForPermutation(
      Permutation::Enum permutation) const;
};

// A shared pointer to a constant `LocatedTriplesSnapshot`, but as an explicit
// class, such that it can be forward-declared.
class SharedLocatedTriplesSnapshot
    : public std::shared_ptr<const LocatedTriplesSnapshot> {};

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

 public:
  using Triples = std::vector<IdTriple<0>>;
  using CancellationHandle = ad_utility::SharedCancellationHandle;

 private:
  // The index to which these triples are added.
  const IndexImpl& index_;

  // The located triples for all the 6 permutations.
  LocatedTriplesPerBlockAllPermutations locatedTriples_;

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

  // Each delta triple needs to know where it is stored in each of the six
  // `LocatedTriplesPerBlock` above.
  struct LocatedTripleHandles {
    using It = LocatedTriples::iterator;
    std::array<It, Permutation::ALL.size()> handles_;

    LocatedTriples::iterator& forPermutation(Permutation::Enum permutation);
  };
  using TriplesToHandlesMap =
      ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles>;

  // The sets of triples added to and subtracted from the original index. Any
  // triple can be at most in one of the sets. The information whether a triple
  // is in the index is missing. This means that a triple that is in the index
  // may still be in the inserted set and vice versa.
  TriplesToHandlesMap triplesInserted_;
  TriplesToHandlesMap triplesDeleted_;

 public:
  // Construct for given index.
  explicit DeltaTriples(const Index& index);
  explicit DeltaTriples(const IndexImpl& index) : index_{index} {};

  // Disable accidental copying.
  DeltaTriples(const DeltaTriples&) = delete;
  DeltaTriples& operator=(const DeltaTriples&) = delete;

  // Get the common `LocalVocab` of the delta triples.
 private:
  LocalVocab& localVocab() { return localVocab_; }
  auto& locatedTriples() { return locatedTriples_; }
  const auto& locatedTriples() const { return locatedTriples_; }

 public:
  const LocalVocab& localVocab() const { return localVocab_; }

  const LocatedTriplesPerBlock& getLocatedTriplesForPermutation(
      Permutation::Enum permutation) const {
    return locatedTriples_.at(static_cast<size_t>(permutation));
  }

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

  // Return a deep copy of the `LocatedTriples` and the corresponding
  // `LocalVocab` which form a snapshot of the current status of this
  // `DeltaTriples` object.
  SharedLocatedTriplesSnapshot getSnapshot() const;

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

// This class synchronizes the access to a `DeltaTriples` object, thus avoiding
// race conditions between concurrent updates and queries.
class DeltaTriplesManager {
  ad_utility::Synchronized<DeltaTriples> deltaTriples_;
  ad_utility::Synchronized<SharedLocatedTriplesSnapshot, std::shared_mutex>
      currentLocatedTriplesSnapshot_;

 public:
  using CancellationHandle = DeltaTriples::CancellationHandle;
  using Triples = DeltaTriples::Triples;

  explicit DeltaTriplesManager(const IndexImpl& index);
  FRIEND_TEST(DeltaTriplesTest, DeltaTriplesManager);

  // Modify the underlying `DeltaTriples` by applying `function` and then update
  // the current snapshot. Concurrent calls to `modify` and `clear` will be
  // serialized, and each call to `getCurrentSnapshot` will either return the
  // snapshot before or after a modification, but never one of an ongoing
  // modification.
  void modify(const std::function<void(DeltaTriples&)>& function);

  // Reset the updates represented by the underlying `DeltaTriples` and then
  // update the current snapshot.
  void clear();

  // Return a shared pointer to a deep copy of the current snapshot. This can
  // be safely used to execute a query without interfering with future updates.
  SharedLocatedTriplesSnapshot getCurrentSnapshot() const;
};
