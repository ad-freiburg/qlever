// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/idTable/IdTable.h"
#include "global/IdTriple.h"
#include "util/HashMap.h"

#pragma once

// A triple and its location in a particular permutation.
//
// NOTE: Technically, `blockIndex` and the `existsInIndex` are redundant in this
// record because they can be derived when the clas is used. However, both are
// useful for testing and for a small nuber of delta triples (think millions),
// the space efficiency of this class is not a significant issue.
struct LocatedTriple {
  // The index of the block and the position within that block, where the
  // triple "fits".
  size_t blockIndex;
  size_t rowIndexInBlock;
  // The `Id`s of the triple in the order of the permutation. For example,
  // for an object pertaining to the SPO permutation: `id1` is the subject,
  // `id2` is the predicate, and `id3` is the object.
  Id id1;
  Id id2;
  Id id3;
  // True iff the triple exists in the permutation (then it is equal to the
  // triple at the position given by `blockIndex` and `rowIndexInBlock`).
  bool existsInIndex;

  // Locate the given triple in the given permutation.
  template <typename Permutation>
  static LocatedTriple locateTripleInPermutation(
      Id id1, Id id2, Id id3, const Permutation& permutation);
};

// A sorted set of triples located at the same position in a particular
// permutation. Note that we could also overload `std::less` here.
struct LocatedTripleCompare {
  bool operator()(const LocatedTriple& x, const LocatedTriple& y) const {
    return IdTriple{x.id1, x.id2, x.id3} < IdTriple{y.id1, y.id2, y.id3};
  }
};
using LocatedTriples = std::set<LocatedTriple, LocatedTripleCompare>;

// A sorted set of triples located in particular permutation, grouped by block.
class LocatedTriplesPerBlock {
 private:
  // The total number of `LocatedTriple` objects stored (for all blocks).
  size_t numTriples_ = 0;

 public:
  // Map with the list of triples per block.
  //
  // TODO: Should be private. Should we make `LocatedTriplesPerBlock` a subclass
  // of `HashMap<size_t, LocatedTriples>` or is that bad style?
  ad_utility::HashMap<size_t, LocatedTriples> map_;

 public:
  // Get the number of to-be-inserted (first) and to-be-deleted (second) triples
  // for the given block and that match the `id1` (if provided) and `id2` (if
  // provided).
  std::pair<size_t, size_t> numTriples(size_t blockIndex) const;
  std::pair<size_t, size_t> numTriples(size_t blockIndex, Id id1) const;
  std::pair<size_t, size_t> numTriples(size_t blockIndex, Id id1, Id id2) const;

  // Merge the located triples for `blockIndex` into the given `block` (which
  // might be the whole block with that index or just a part of it) and write
  // the result to `result`, starting from position `offsetInResult`.
  //
  // It is the resposibility of the caller that there is enough space or the
  // result starting from that offset. Like for `numTriplesInBlock` above,
  // consider only triples that match `id1` (if provided) and `id2` (if
  // provided). If `block` is just a part of an index block, the first triple of
  // block has row index `rowIndexOffset` in the original block.
  //
  // TODO: Beware of triples inserted at the end of the block, they are found in
  // the `LocatedTriples` for `blockIndex + 1`. It's up to `CompressedRelation`
  // to handle that correctly.
  void mergeTriples(size_t blockIndex, const IdTable& block, IdTable& result,
                    size_t offsetInResult) const;
  void mergeTriples(size_t blockIndex, const IdTable& block, IdTable& result,
                    size_t offsetInResult, size_t rowIndexOffset, Id id1) const;
  void mergeTriples(size_t blockIndex, const IdTable& block, IdTable& result,
                    size_t offsetInResult, size_t rowIndexOffset, Id id1,
                    Id id2) const;

  // Add the given `locatedTriple` to the given `LocatedTriplesPerBlock`.
  // Returns a handle to where it was added (via which we can easily remove it
  // again if we need to).
  LocatedTriples::iterator add(const LocatedTriple& locatedTriple) {
    LocatedTriples& locatedTriples = map_[locatedTriple.blockIndex];
    auto [handle, wasInserted] = locatedTriples.emplace(locatedTriple);
    AD_CORRECTNESS_CHECK(wasInserted == true);
    AD_CORRECTNESS_CHECK(handle != locatedTriples.end());
    ++numTriples_;
    return handle;
  };

  // Get the total number of `LocatedTriple` objects (for all blocks).
  size_t numTriples() const { return numTriples_; }

  // Get the number of blocks containing `LocatedTriple` objects.
  size_t numBlocks() const { return map_.size(); }

  // Empty the data structure.
  void clear() {
    map_.clear();
    numTriples_ = 0;
  }

 private:
  // Match modes for `numTriplesInBlockImpl` and `mergeTriplesIntoBlockImpl`.
  enum struct MatchMode { MatchAll, MatchId1, MatchId1AndId2 };

  // The Implementation behind the public method `numTriplesInBlock` above.
  template <MatchMode matchMode>
  std::pair<size_t, size_t> numTriplesImpl(size_t blockIndex,
                                           Id id1 = Id::makeUndefined(),
                                           Id id2 = Id::makeUndefined()) const;

  // The Implementation behind the public method `mergeTriplesIntoBlock` above.
  // The only reason that the arguments `id1` and `id2` come at the end here is
  // so that we can give them default values.
  template <MatchMode matchMode>
  void mergeTriples(size_t blockIndex, const IdTable& block, IdTable& result,
                    size_t offsetInResult, size_t rowIndexOffset = 0,
                    Id id1 = Id::makeUndefined(),
                    Id id2 = Id::makeUndefined()) const;
};
