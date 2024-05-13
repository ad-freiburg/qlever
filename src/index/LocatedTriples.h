// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include "engine/idTable/IdTable.h"
#include "global/IdTriple.h"
#include "util/HashMap.h"
#include "index/CompressedRelation.h"

class Permutation;

// A triple and its location in a particular permutation.
//
// If a triple is not contained in the permutation, the location is the location
// of the next larger triple (which may be in the next block or beyond the last
// block). For a detailed definition of all border cases, see the definition at
// the end of this file.
//
// NOTE: Technically, `blockIndex` and the `existsInIndex` are redundant in this
// record because they can be derived when the class is used. However, they are
// useful for testing, and for a small nuber of delta triples (think millions),
// space efficiency is not a significant issue for this class.
struct LocatedTriple {
  // The index of the block and the location within that block, according to the
  // definition above.
  size_t blockIndex;
  size_t rowIndexInBlock;
  // The `Id`s of the triple in the order of the permutation. For example,
  // for an object pertaining to the SPO permutation: `id1` is the subject,
  // `id2` is the predicate, and `id3` is the object.
  Id id1;
  Id id2;
  Id id3;
  // Flag that is true if and only if the triple exists in the permutation. It
  // is then equal to the triple at the position given by `blockIndex` and
  // `rowIndexInBlock`.
  bool existsInIndex;

  // Locate the given triple in the given permutation.
  static LocatedTriple locateTripleInPermutation(
      Id id1, Id id2, Id id3, const Permutation& permutation);

  // Special row index for triples that belong to the previous block (see the
  // definition for the location of a triple at the end of this file).
  //
  // NOTE: It is important that `NO_ROW_INDEX + 1 > NO_ROW_INDEX`, hence it is
  // defined as `max() - 1` and not as the seemingly more natural `max()`.
  static const size_t NO_ROW_INDEX = std::numeric_limits<size_t>::max() - 1;

  auto operator<=>(const LocatedTriple&) const = default;
};

// A sorted set of located triples. In `LocatedTriplesPerBlock` below, we use
// this to store all located triples with the same `blockIndex`.
//
// NOTE: We could also overload `std::less` here, but the explicit specification
// of the order makes it clearer.
struct LocatedTripleCompare {
  bool operator()(const LocatedTriple& x, const LocatedTriple& y) const {
    return IdTriple{x.id1, x.id2, x.id3} < IdTriple{y.id1, y.id2, y.id3};
  }
};
using LocatedTriples = std::set<LocatedTriple, LocatedTripleCompare>;

// Sorted sets of located triples, grouped by block. We use this to store all
// located triples for a permutation.
class LocatedTriplesPerBlock {
 private:
  // The total number of `LocatedTriple` objects stored (for all blocks).
  size_t numTriples_ = 0;

 public:
  // For each block with a non-empty set of located triples, the located triples
  // in that block.
  //
  // NOTE: This is currently not private because we want access to
  // `map_.size()`, `map_.clear()`, `map_.contains(...)`, and `map_.at(...)`.
  // We could also make `LocatedTriplesPerBlock` a subclass of `HashMap<size_t,
  // LocatedTriples>`, but not sure whether that is good style.
  ad_utility::HashMap<size_t, LocatedTriples> map_;

 public:
  using ScanSpecification = CompressedRelationReader::ScanSpecification;
  // Get the number of located triples for the given block that match `id1` (if
  // provided) and `id2` (if provided). The return value is a pair of numbers:
  // first, the number of existing triples ("to be deleted") and second, the
  // number of new triples ("to be inserted").
  std::pair<size_t, size_t> numTriples(size_t blockIndex, ScanSpecification scanSpec) const;

  // Merge located triples for `blockIndex` with the given index `block` and
  // write to `result`, starting from position `offsetInResult`. Consider only
  // located triples in the range specified by `rowIndexInBlockBegin` and
  // `rowIndexInBlockEnd`. Consider only triples that match `id1` (if provided)
  // and `id2` (if provided). Return the number of rows written to `result`.
  //
  // PRECONDITIONS:
  //
  // 1. The set of located triples for `blockIndex` must be non-empty.
  // Otherwise, there is no need for merging and this method shouldn't be
  // called for efficiency reasons.
  //
  // 2. It is the resposibility of the caller that there is enough space for the
  // result of the merge in `result` starting from `offsetInResult`.
  //
  // 3. If `block == std::nullopt`, we are adding to `result` the located
  // triples for block `blockIndex` where the `rowIndexInBlock` is
  // `NO_ROW_INDEX`. These actually belong to the previous block, but were
  // larger than all triples there. This requires that `id1` or both `id1` and
  // `id2` are specified.
  //
  size_t mergeTriples(size_t blockIndex, std::optional<IdTable> block,
                      IdTable& result, size_t offsetInResult,
                      ScanSpecification scanSpec,
                      size_t rowIndexInBlockBegin = 0,
                      size_t rowIndexInBlockEnd = LocatedTriple::NO_ROW_INDEX) const;

  // Add the given `locatedTriple` to the given `LocatedTriplesPerBlock`.
  // Return a handle to where it was added (`LocatedTriples` is a sorted set,
  // see above). We need this handle so that we can easily remove the
  // `locatedTriple` from the set again in case we need to.
  //
  // Precondition: The `locatedTriple` must not already exist in
  // `LocatedTriplesPerBlock`.
  LocatedTriples::iterator add(const LocatedTriple& locatedTriple) {
    LocatedTriples& locatedTriples = map_[locatedTriple.blockIndex];
    auto [handle, wasInserted] = locatedTriples.emplace(locatedTriple);
    AD_CORRECTNESS_CHECK(wasInserted == true);
    AD_CORRECTNESS_CHECK(handle != locatedTriples.end());
    ++numTriples_;
    return handle;
  };

  // Get the total number of `LocatedTriple`s (for all blocks).
  size_t numTriples() const { return numTriples_; }

  // Get the number of blocks with a non-empty set of located triples.
  size_t numBlocks() const { return map_.size(); }

  // Remove all located triples.
  void clear() {
    map_.clear();
    numTriples_ = 0;
  }
};

// Human-readable representation of `LocatedTriple`, `LocatedTriples`, and
// `LocatedTriplesPerBlock`, which are very useful for debugging.
std::ostream& operator<<(std::ostream& os, const LocatedTriple& lt);
std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts);
std::ostream& operator<<(std::ostream& os, const LocatedTriplesPerBlock& ltpb);
std::ostream& operator<<(std::ostream& os,
                         const columnBasedIdTable::Row<Id>& idTableRow);
std::ostream& operator<<(std::ostream& os, const IdTable& idTable);

// DEFINITION OF THE POSITION OF A LOCATED TRIPLE IN A PERMUTATION
//
// 1. The position is defined by the index of a block in the permutation and the
// index of a row within that block.
//
// 2. If the triple is contained in the permutation, it is contained exactly
// once and so there is a well defined block and position in that block.
//
// 2. If there is a block, where the first triple is smaller and the last triple
// is larger, then that is the block and the position in that block is that of
// the first triple that is (not smaller and hence) larger.
//
// 3. If the triple falls "between two blocks" (the last triple of the previous
// block is smaller and the first triple of the next block is larger), then the
// position is the first position in that next block.
//
// 4. As a special case of 3, if the triple is smaller than all triples in the
// permutation, the position is the first position of the first block.
//
// 5. If the triple is larger than all triples in the permutation, the block
// index is one after the largest block index and the position within that
// non-existing block is arbitrary.
