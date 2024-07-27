// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors:
//    2023 Hannah Bast <bast@cs.uni-freiburg.de>
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include "engine/idTable/IdTable.h"
#include "global/IdTriple.h"
#include "index/CompressedRelation.h"
#include "util/HashMap.h"

class Permutation;

struct NumAddedAndDeleted {
  size_t numAdded_;
  size_t numDeleted_;

  bool operator<=>(const NumAddedAndDeleted&) const = default;
};

// A triple and its block in a particular permutation.
// For a detailed definition of all border cases, see the definition at
// the end of this file.
struct LocatedTriple {
  // The index of the block, according to the definition above.
  size_t blockIndex_;
  // The `Id`s of the triple in the order of the permutation. For example,
  // for an object pertaining to the OPS permutation: `id1` is the object,
  // `id2` is the predicate, and `id3` is the subject.
  IdTriple<0> triple_;

  // Flag that is true if the given triple is inserted and false if it
  // is deleted.
  bool shouldTripleExist_;

  // Locate the given triples in the given permutation.
  static std::vector<LocatedTriple> locateTriplesInPermutation(
      std::span<const IdTriple<0>> triples,
      std::span<const CompressedBlockMetadata> blockMetadata,
      const std::array<size_t, 3>& keyOrder, bool shouldExist,
      ad_utility::SharedCancellationHandle cancellationHandle);
  bool operator==(const LocatedTriple&) const = default;

  // This operator is only for debugging and testing. It returns a
  // human-readable representation.
  friend std::ostream& operator<<(std::ostream& os, const LocatedTriple& lt) {
    os << "LT(" << lt.blockIndex_ << " " << lt.triple_ << " "
       << lt.shouldTripleExist_ << ")";
    return os;
  }
};

// A sorted set of located triples. In `LocatedTriplesPerBlock` below, we use
// this to store all located triples with the same `blockIndex_`.
//
// NOTE: We could also overload `std::less` here, but the explicit specification
// of the order makes it clearer.
struct LocatedTripleCompare {
  bool operator()(const LocatedTriple& x, const LocatedTriple& y) const {
    return x.triple_ < y.triple_;
  }
};
using LocatedTriples = std::set<LocatedTriple, LocatedTripleCompare>;

// This operator is only for debugging and testing. It returns a
// human-readable representation.
std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts);

// Sorted sets of located triples, grouped by block. We use this to store all
// located triples for a permutation.
class LocatedTriplesPerBlock {
 private:
  // The total number of `LocatedTriple` objects stored (for all blocks).
  size_t numTriples_ = 0;

  // For each block with a non-empty set of located triples, the located triples
  // in that block.
  ad_utility::HashMap<size_t, LocatedTriples> map_;

  FRIEND_TEST(LocatedTriplesTest, numTriplesInBlock);

  // Impl function to `mergeTriples`.
  template <size_t numIndexColumns>
  IdTable mergeTriplesImpl(size_t blockIndex, const IdTable& block) const;

  // Stores the block metadata where the block borders have been adjusted for
  // the updated triples.
  std::optional<std::vector<CompressedBlockMetadata>> augmentedMetadata_;
  std::vector<CompressedBlockMetadata> originalMetadata_;
  void updateAugmentedMetadata();

 public:
  // Get upper limits for the number of located triples for the given block. The
  // return value is a pair of numbers: first, the number of existing triples
  // ("to be deleted") and second, the number of new triples ("to be inserted").
  // The numbers are only upper limits because there may be triples that have no
  // effect (like adding an already existing triple and deleting a non-existent
  // triple).
  NumAddedAndDeleted numTriples(size_t blockIndex) const;

  // Returns whether there are updates triples for the block with the index
  // `blockIndex`.
  bool hasUpdates(size_t blockIndex) const;

  // Merge located triples for `blockIndex_` with the given index `block` and
  // write to `result`, starting from position `offsetInResult`. Return the
  // number of rows written to `result`.
  //
  // PRECONDITIONS:
  //
  // 1. `mergeTriples` must always be called with all the index columns in the
  // input. So the column indices must be `{0, 1, 2, ...}`.
  //
  // 2. It is the responsibility of the caller that there is enough space for
  // the result of the merge in `result` starting from `offsetInResult`.
  //
  // 3. The set of located triples for `blockIndex_` must be non-empty.
  // Otherwise, there is no need for merging and this method shouldn't be
  // called for efficiency reasons.
  IdTable mergeTriples(size_t blockIndex, const IdTable& block,
                       size_t numIndexColumns) const;

  // Add `locatedTriples` to the `LocatedTriplesPerBlock`.
  // Return handles to where they were added (`LocatedTriples` is a sorted set,
  // see above). We need the handles so that we can easily remove the
  // `locatedTriples` from the set again in case we need to.
  //
  // PRECONDITIONS:
  //
  // 1. The `locatedTriples` must not already exist in
  // `LocatedTriplesPerBlock`.
  std::vector<LocatedTriples::iterator> add(
      std::span<const LocatedTriple> locatedTriples);

  void erase(size_t blockIndex, LocatedTriples::iterator iter);

  // Get the total number of `LocatedTriple`s (for all blocks).
  size_t numTriples() const { return numTriples_; }

  // Get the number of blocks with a non-empty set of located triples.
  size_t numBlocks() const { return map_.size(); }

  // Must be called initially before using the `LocatedTriplesPerBlock` to
  // initialize the original block metadata that is augmented for updated
  // triples. This is currently done in `Permutation::loadFromDisk`.
  void setOriginalMetadata(std::vector<CompressedBlockMetadata> metadata);

  // Returns the block metadata where the block borders have been updated to
  // account for the update triples. All triples (both insert and delete) will
  // enlarge the block borders.
  const std::vector<CompressedBlockMetadata>& getAugmentedMetadata() const {
    AD_CONTRACT_CHECK(augmentedMetadata_.has_value());
    return augmentedMetadata_.value();
  };

  // Remove all located triples.
  void clear() {
    map_.clear();
    numTriples_ = 0;
    augmentedMetadata_ = originalMetadata_;
  }

  // This operator is only for debugging and testing. It returns a
  // human-readable representation.
  friend std::ostream& operator<<(std::ostream& os,
                                  const LocatedTriplesPerBlock& ltpb) {
    // Get the block indices in sorted order.
    std::vector<size_t> blockIndices;
    std::ranges::copy(ltpb.map_ | std::views::keys,
                      std::back_inserter(blockIndices));
    std::ranges::sort(blockIndices);
    for (auto blockIndex : blockIndices) {
      os << "LTs in Block #" << blockIndex << ": " << ltpb.map_.at(blockIndex)
         << std::endl;
    }
    return os;
  };
};

// Human-readable representation , which are very useful for debugging.
std::ostream& operator<<(std::ostream& os, const std::vector<IdTriple<0>>& v);

// DEFINITION OF THE POSITION OF A LOCATED TRIPLE IN A PERMUTATION
//
// 1. The position is defined by the index of a block.
//
// 2. A triple belongs to the first block (with the smallest index) that also
// contains at least one triple that is larger than or equal to the input
// triple.
//
// 2.1. In particular, if the triple falls "between two blocks" (the last triple
// of the previous block is smaller and the first triple of the next block is
// larger), then the block is the next block.
//
// 2.2. In particular, if the triple is smaller than all triples in the
// permutation, the position is the first position of the first block.
//
// 3. If the triple is larger than all triples in the permutation, the block
// index is one after the largest block index.
