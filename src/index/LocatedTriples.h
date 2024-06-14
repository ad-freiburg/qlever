// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include "engine/idTable/IdTable.h"
#include "global/IdTriple.h"
#include "index/CompressedRelation.h"
#include "util/HashMap.h"

class Permutation;

// A triple and its block in a particular permutation.
// For a detailed definition of all border cases, see the definition at
// the end of this file.
struct LocatedTriple {
  // The index of the block, according to the definition above.
  size_t blockIndex;
  // The `Id`s of the triple in the order of the permutation. For example,
  // for an object pertaining to the OPS permutation: `id1` is the object,
  // `id2` is the predicate, and `id3` is the subject.
  IdTriple triple;

  // Flag that is true if the given triple is inserted and false if it
  // is deleted.
  bool shouldTripleExist;

  // Locate the given triples in the given permutation.
  static std::vector<LocatedTriple> locateTriplesInPermutation(
      const std::vector<IdTriple>& triples, const Permutation& permutation,
      bool shouldExist);

  bool operator==(const LocatedTriple&) const = default;

  friend std::ostream& operator<<(std::ostream& os, const LocatedTriple& lt);
};

// A sorted set of located triples. In `LocatedTriplesPerBlock` below, we use
// this to store all located triples with the same `blockIndex`.
//
// NOTE: We could also overload `std::less` here, but the explicit specification
// of the order makes it clearer.
struct LocatedTripleCompare {
  bool operator()(const LocatedTriple& x, const LocatedTriple& y) const {
    return x.triple < y.triple;
  }
};
using LocatedTriples = std::set<LocatedTriple, LocatedTripleCompare>;

std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts);

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
  // Get upper limits for the number of located triples for the given block. The
  // return value is a pair of numbers: first, the number of existing triples
  // ("to be deleted") and second, the number of new triples ("to be inserted").
  // The numbers are only upper limits because there may be triples that have no
  // effect (like adding an already existing triple and deleting a non-existent
  // triple).
  std::pair<size_t, size_t> numTriples(size_t blockIndex) const;

  // Merge located triples for `blockIndex` with the given index `block` and
  // write to `result`, starting from position `offsetInResult`. Return the
  // number of rows written to `result`.
  //
  // PRECONDITIONS:
  //
  // 1. The set of located triples for `blockIndex` must be non-empty.
  // Otherwise, there is no need for merging and this method shouldn't be
  // called for efficiency reasons.
  //
  // 2. It is the responsibility of the caller that there is enough space for
  // the result of the merge in `result` starting from `offsetInResult`.
  size_t mergeTriples(size_t blockIndex, IdTable block, IdTable& result,
                      size_t offsetInResult) const;

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
      const std::vector<LocatedTriple>& locatedTriples);

  void erase(size_t blockIndex, LocatedTriples::iterator iter);

  // Get the total number of `LocatedTriple`s (for all blocks).
  size_t numTriples() const { return numTriples_; }

  // Get the number of blocks with a non-empty set of located triples.
  size_t numBlocks() const { return map_.size(); }

  // Remove all located triples.
  void clear() {
    map_.clear();
    numTriples_ = 0;
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const LocatedTriplesPerBlock& ltpb);
};

// Human-readable representation , which are very useful for debugging.
// TODO<qup42>: find a better place for these definitions
std::ostream& operator<<(std::ostream& os,
                         const columnBasedIdTable::Row<Id>& idTableRow);
std::ostream& operator<<(std::ostream& os, const IdTable& idTable);
template <typename T, std::size_t N>
std::ostream& operator<<(std::ostream& os, const std::array<T, N>& v);
template <typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& v);

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
