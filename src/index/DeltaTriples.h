// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
// 2024 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_DELTATRIPLES_H
#define QLEVER_SRC_INDEX_DELTATRIPLES_H

#include "backports/three_way_comparison.h"
#include "engine/LocalVocab.h"
#include "global/IdTriple.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"
#include "util/Synchronized.h"
#include "util/TimeTracer.h"

// Typedef for one `LocatedTriplesPerBlock` object for each of the six
// permutations.
template <bool isInternal>
using LocatedTriplesPerBlockAllPermutations =
    std::array<LocatedTriplesPerBlock, Permutation::all<isInternal>().size()>;

// The locations of a set of delta triples (triples that were inserted or
// deleted since the index was built) in each of the six permutations, and a
// local vocab. This is all the information that is required to perform a query
// that correctly respects these delta triples, hence the name.
struct LocatedTriplesSnapshot {
  LocatedTriplesPerBlockAllPermutations<false> locatedTriplesPerBlock_;
  LocatedTriplesPerBlockAllPermutations<true> internalLocatedTriplesPerBlock_;
  // Make sure to keep the local vocab alive as long as the snapshot is alive.
  // The `DeltaTriples` class may concurrently add new entries under the hood,
  // but this is safe because the `LifetimeExtender` prevents access entirely.
  LocalVocab::LifetimeExtender localVocabLifetimeExtender_;
  // A unique index for this snapshot that is used in the query cache.
  size_t index_;
  // Get `TripleWithPosition` objects for given permutation.
  const LocatedTriplesPerBlock& getLocatedTriplesForPermutation(
      Permutation::Enum permutation) const;
  // Get `TripleWithPosition` objects for given internal permutation.
  const LocatedTriplesPerBlock& getInternalLocatedTriplesForPermutation(
      Permutation::Enum permutation) const;
};

// A shared pointer to a constant `LocatedTriplesSnapshot`, but as an explicit
// class, such that it can be forward-declared.
class SharedLocatedTriplesSnapshot
    : public std::shared_ptr<const LocatedTriplesSnapshot> {};

// A class for keeping track of the number of triples of the `DeltaTriples`.
struct DeltaTriplesCount {
  int64_t triplesInserted_;
  int64_t triplesDeleted_;

  /// Output as json. The signature of this function is mandated by the json
  /// library to allow for implicit conversion.
  friend void to_json(nlohmann::json& j, const DeltaTriplesCount& count);

  friend DeltaTriplesCount operator-(const DeltaTriplesCount& lhs,
                                     const DeltaTriplesCount& rhs);

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(DeltaTriplesCount,
                                              triplesInserted_, triplesDeleted_)
};

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
  FRIEND_TEST(DeltaTriplesTest, storeAndRestoreData);

 public:
  using Triples = std::vector<IdTriple<0>>;
  using CancellationHandle = ad_utility::SharedCancellationHandle;

 private:
  // The index to which these triples are added.
  const IndexImpl& index_;
  size_t nextSnapshotIndex_ = 0;

  // The local vocabulary of the delta triples (they may have components,
  // which are not contained in the vocabulary of the original index).
  LocalVocab localVocab_;

  // See the documentation of `setPersist()` below.
  std::optional<std::string> filenameForPersisting_;

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

  // Generic state wrapper to avoid code duplication for internal and regular
  // triples.
  template <bool isInternal>
  struct TriplesToHandles {
    // Each delta triple needs to know where it is stored in each of the six
    // `LocatedTriplesPerBlock` above.
    struct LocatedTripleHandles {
      using It = LocatedTriples::iterator;
      std::array<It, Permutation::all<isInternal>().size()> handles_;

      LocatedTriples::iterator& forPermutation(Permutation::Enum permutation);
    };
    using TriplesToHandlesMap =
        ad_utility::HashMap<IdTriple<0>, LocatedTripleHandles>;
    // The sets of triples added to and subtracted from the original index. Any
    // triple can be at most in one of the sets. The information whether a
    // triple is in the index is missing. This means that a triple that is in
    // the index may still be in the inserted set and vice versa.
    TriplesToHandlesMap triplesInserted_;
    TriplesToHandlesMap triplesDeleted_;
  };

  TriplesToHandles<false> triplesToHandlesNormal_;
  TriplesToHandles<true> triplesToHandlesInternal_;
  // The located triples for all the permutations.
  LocatedTriplesPerBlockAllPermutations<false> locatedTriplesNormal_;
  LocatedTriplesPerBlockAllPermutations<true> locatedTriplesInternal_;

 public:
  // Construct for given index.
  explicit DeltaTriples(const Index& index);
  explicit DeltaTriples(const IndexImpl& index) : index_{index} {}

  // Disable accidental copying.
  DeltaTriples(const DeltaTriples&) = delete;
  DeltaTriples& operator=(const DeltaTriples&) = delete;

  // Get the common `LocalVocab` of the delta triples.
 private:
  LocalVocab& localVocab() { return localVocab_; }

 public:
  const LocalVocab& localVocab() const { return localVocab_; }

  const LocatedTriplesPerBlock& getLocatedTriplesForPermutation(
      Permutation::Enum permutation) const {
    return locatedTriplesNormal_.at(static_cast<size_t>(permutation));
  }

  // Clear `triplesAdded_` and `triplesSubtracted_` and all associated data
  // structures.
  void clear();

  // The number of delta triples added and subtracted.
  int64_t numInserted() const {
    return static_cast<int64_t>(
        triplesToHandlesNormal_.triplesInserted_.size());
  }
  int64_t numDeleted() const {
    return static_cast<int64_t>(triplesToHandlesNormal_.triplesDeleted_.size());
  }
  DeltaTriplesCount getCounts() const;

  // The number of internal delta triples added and subtracted.
  int64_t numInternalInserted() const {
    return static_cast<int64_t>(
        triplesToHandlesInternal_.triplesInserted_.size());
  }
  int64_t numInternalDeleted() const {
    return static_cast<int64_t>(
        triplesToHandlesInternal_.triplesDeleted_.size());
  }

  // Insert triples.
  void insertTriples(CancellationHandle cancellationHandle, Triples triples,
                     ad_utility::timer::TimeTracer& tracer =
                         ad_utility::timer::DEFAULT_TIME_TRACER);

  // Delete triples.
  void deleteTriples(CancellationHandle cancellationHandle, Triples triples,
                     ad_utility::timer::TimeTracer& tracer =
                         ad_utility::timer::DEFAULT_TIME_TRACER);

  // Insert internal delta triples for efficient language filters and patterns.
  // Currently only used by test code.
  void insertInternalTriples(CancellationHandle cancellationHandle,
                             Triples triples,
                             ad_utility::timer::TimeTracer& tracer =
                                 ad_utility::timer::DEFAULT_TIME_TRACER);

  // Delete internal delta triples for efficient language filters and patterns.
  // Currently only used by test code.
  void deleteInternalTriples(CancellationHandle cancellationHandle,
                             Triples triples,
                             ad_utility::timer::TimeTracer& tracer =
                                 ad_utility::timer::DEFAULT_TIME_TRACER);

  // If the `filename` is set, then `writeToDisk()` will write these
  // `DeltaTriples` to `filename.value()`. If `filename` is `nullopt`, then
  // `writeToDisk` will be a nullop.
  void setPersists(std::optional<std::string> filename);

  // Write the delta triples to disk to persist them between restarts.
  void writeToDisk() const;

  // Read the delta triples from disk to restore them after a restart.
  void readFromDisk();

  // Return a deep copy of the `LocatedTriples` and the corresponding
  // `LocalVocab` which form a snapshot of the current status of this
  // `DeltaTriples` object.
  SharedLocatedTriplesSnapshot getSnapshot();

  // Register the original `metadata` for the given `permutation`. This has to
  // be called before any updates are processed. If `setInternalMetadata` is
  // true, this will set the metadata to the internal permutations instead.
  void setOriginalMetadata(
      Permutation::Enum permutation,
      std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata,
      bool setInternalMetadata);

  // Update the block metadata.
  void updateAugmentedMetadata();

 private:
  // The the proper state according to the template parameter. This will either
  // return a reference to `triplesToHandlesInternal_` or
  // `triplesToHandlesNormal_`.
  template <bool isInternal>
  TriplesToHandles<isInternal>& getState();

  // Helper function to get the correct located triple (either internal or
  // external), depending on the `internal` template parameter.
  template <bool isInternal>
  auto& getLocatedTriple();

  // Find the position of the given triple in the given permutation and add it
  // to each of the six `LocatedTriplesPerBlock` maps (one per permutation).
  // When `insertOrDelete` is `true`, the triples are inserted, otherwise
  // deleted. Return the iterators of where it was added (so that we can easily
  // delete it again from these maps later).
  template <bool isInternal>
  std::vector<typename TriplesToHandles<isInternal>::LocatedTripleHandles>
  locateAndAddTriples(CancellationHandle cancellationHandle,
                      ql::span<const IdTriple<0>> triples, bool insertOrDelete,
                      ad_utility::timer::TimeTracer& tracer =
                          ad_utility::timer::DEFAULT_TIME_TRACER);

  // Common implementation for `insertTriples` and `deleteTriples`. When
  // `insertOrDelete` is `true`, the triples are inserted, `targetMap` contains
  // the already inserted triples, and `inverseMap` contains the already deleted
  // triples. When `insertOrDelete` is `false`, the triples are deleted, and it
  // is the other way around:. This is used to resolve insertions or deletions
  // that are idempotent or cancel each other out.
  template <bool isInternal, bool insertOrDelete>
  void modifyTriplesImpl(CancellationHandle cancellationHandle, Triples triples,
                         ad_utility::timer::TimeTracer& tracer =
                             ad_utility::timer::DEFAULT_TIME_TRACER);

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
  template <bool isInternal>
  void eraseTripleInAllPermutations(
      typename TriplesToHandles<isInternal>::LocatedTripleHandles& handles);

  friend class DeltaTriplesManager;
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
  template <typename ReturnType>
  ReturnType modify(const std::function<ReturnType(DeltaTriples&)>& function,
                    bool writeToDiskAfterRequest = true,
                    bool updateMetadataAfterRequest = true,
                    ad_utility::timer::TimeTracer& tracer =
                        ad_utility::timer::DEFAULT_TIME_TRACER);

  void setFilenameForPersistentUpdatesAndReadFromDisk(std::string filename);

  // Reset the updates represented by the underlying `DeltaTriples` and then
  // update the current snapshot.
  void clear();

  // Return a shared pointer to a deep copy of the current snapshot. This can
  // be safely used to execute a query without interfering with future updates.
  SharedLocatedTriplesSnapshot getCurrentSnapshot() const;
};

#endif  // QLEVER_SRC_INDEX_DELTATRIPLES_H
