// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#include "global/IdTriple.h"
#include "util/HashMap.h"

#pragma once

// Result record returned by `locateTripleInPermutation`.
//
// NOTE: This is currently more information then we need. In particular, the
// `blockIndex` is already implicit in `LocatedTriplesPerBlock` and the
// bit `existsInOriginalIndex_` can be derived using the information stored in
// a block and our metadata. However, both are useful for testing and for a
// small nuber of delta triples (think millions), the space efficiency of this
// class is not a significant issue.
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
  // Whether the triple exists in the original index or is new.
  bool existsInIndex;
};

// All delta triples located at the same position in the original index.
//
// NOTE: A lambda does not work here because it has to be `static constexpr`
// and then I get a strange warning about "a field ... whose type uses the
// anonymous namespace". I also tried overloading `std::less`, but the
// required `namespace std { ... }` does not work at this point in the code,
// and I didn't want to have it somewhere else than here.
struct LocatedTripleCompare {
  bool operator()(const LocatedTriple& x, const LocatedTriple& y) const {
    return IdTriple{x.id1, x.id2, x.id3} < IdTriple{y.id1, y.id2, y.id3};
  }
};
using LocatedTriples = std::set<LocatedTriple, LocatedTripleCompare>;

// Data structures with positions for a particular permutation.
class LocatedTriplesPerBlock {
 private:
  // The number of `LocatedTriple` objects stored.
  size_t size_ = 0;

 public:
  // Map from block index to position list.
  //
  // TODO: Keep the position list for each block index sorted (primary key:
  // row index in block, secondary key: triple order).
  //
  // TODO: Should be private, but we want to iterate over it for testing.
  ad_utility::HashMap<size_t, LocatedTriples> map_;

 public:
  // Get the positions for a given block index. Returns an empty list if there
  // are no positions for that block index.
  //
  // TODO: Check if that is the behavior we want when actually using class
  // `DeltaTriples` to augment the result of an index scan.
  LocatedTriples getLocatedTriplesForBlock(size_t blockIndex) {
    auto it = map_.find(blockIndex);
    if (it != map_.end()) {
      return it->second;
    } else {
      return {};
    }
  }

  // Add the given `locatedTriple` to the given `LocatedTriplesPerBlock`.
  // Returns a handle to where it was added (via which we can easily remove it
  // again if we need to).
  LocatedTriples::iterator add(const LocatedTriple& locatedTriple) {
    LocatedTriples& locatedTriples = map_[locatedTriple.blockIndex];
    auto [handle, wasInserted] = locatedTriples.emplace(locatedTriple);
    AD_CORRECTNESS_CHECK(wasInserted == true);
    AD_CORRECTNESS_CHECK(handle != locatedTriples.end());
    ++size_;
    return handle;
  };

  // Get the total number of `LocatedTriple` objects (for all blocks).
  size_t size() const { return size_; }

  // Empty the data structure.
  void clear() {
    map_.clear();
    size_ = 0;
  }
};
