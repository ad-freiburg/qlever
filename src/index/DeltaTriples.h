// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include "engine/LocalVocab.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "parser/TurtleParser.h"
#include "util/HashSet.h"

// The `DeltaTriples` class needs to know the `Index` to which it belongs (for
// translating the components of a Turtle triple to `Id`s and for locating `Id`s
// in the permutations). However, the `Index` class also needs to know the
// `DeltaTriples` (in order to consider these triples when scanning a
// permutation). To avoid a circular include, and since we only need an `Index&`
// here, we can use a forward declaration.
// class Index;

// A class for maintaining triples that were inserted or deleted after index
// building.
//
// HOW IT WORKS:
//
// 1. For each "delta triple", find the "matching position" (block index and
// index within that block, see below for a precise definition) for each index
// permutation.
//
// 2. For each permutation and each block, store a sorted list of the positions
// of the delta triples within that block.
//
// 3. In the call of `CompressedRelation::scan`, for each block use the
// information from 2. to check whether there are delta triples for that block
// and if yes, merge them with the triples from the block (given that the
// positions are sorted, this can be done with negligible overhead).
//
// NOTE: For now, this only works when the results of index scans are not
// cached (at least not when there are relevant delta triples for that scan).
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
// store and maintain the positions in those uncompressed index scans.
//
// POSITION WHERE A TRIPLE "FITS" IN A PERMUTATION:
//
// 1. If the triple in contained in the permutation, it is contained exactly
// once and so there is a well defined block and position in that block.
//
// 2. If there is a block, where the first triple is smaller and the last triple
// is larger, then that is the block and the position in that block is that of
// the first triple that is (not smaller and hence) larger.
//
// 3. If the triple falls "between two blocks" (the last triple of the previous
// block is smaller and the first triple of the next block is larger), then take
// the first position in that next block.
//
// 4. Two possibilities remain:
// 4a. The triple is smaller than the first triple of the first block: then take
// that block and the first position in that block.
// 4b. The triple is larger than the last triple of the last block: then take
// that block and the last position in that block.
//
// NOTE: For now, this is a proof-of-concept implementation and the class is
// simplistic in many ways (TODO: make a list, in which ways exactly).
class DeltaTriples {
 public:
  // Inside this class, we are working with triples of IDs.
  using IdTriple = std::array<Id, 3>;

  // Hash value for such triple.
  template <typename H>
  friend H AbslHashValue(H h, const IdTriple& triple) {
    return H::combine(std::move(h), triple[0], triple[1], triple[2]);
  }

  // Result record returned by `locateTripleInPermutation`.
  //
  // NOTE: This is currently more information then we need. In particular, the
  // `blockIndex` is already implicit in `TriplesWithPositionsPerBlock` and the
  // bit `existsInOriginalIndex_` can be derived using the information stored in
  // a block and our metadata. However, both are useful for testing and for a
  // small nuber of delta triples (think millions), the space efficiency of this
  // class is not a significant issue.
  struct TripleWithPosition {
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
  struct TriplesWithPositionsCompare {
    bool operator()(const TripleWithPosition& x,
                    const TripleWithPosition& y) const {
      return IdTriple{x.id1, x.id2, x.id3} < IdTriple{y.id1, y.id2, y.id3};
    }
  };
  using TriplesWithPositions =
      std::set<TripleWithPosition, TriplesWithPositionsCompare>;
  using TriplesWithPositionsIterator = TriplesWithPositions::iterator;

  // Data structures with positions for a particular permutation.
  class TriplesWithPositionsPerBlock {
   private:
    // The number of `TripleWithPosition` objects stored.
    size_t size_ = 0;
    friend DeltaTriples;

   public:
    // Map from block index to position list.
    //
    // TODO: Keep the position list for each block index sorted (primary key:
    // row index in block, secondary key: triple order).
    //
    // TODO: Should be private, but we want to iterate over it for testing.
    ad_utility::HashMap<size_t, TriplesWithPositions> positionMap_;

   public:
    // Get the positions for a given block index. Returns an empty list if there
    // are no positions for that block index.
    //
    // TODO: Check if that is the behavior we want when actually using class
    // `DeltaTriples` to augment the result of an index scan.
    TriplesWithPositions getTriplesWithPositionsForBlock(size_t blockIndex) {
      auto it = positionMap_.find(blockIndex);
      if (it != positionMap_.end()) {
        return it->second;
      } else {
        return {};
      }
    }

    // Get the number of `TripleWithPosition` objects.
    size_t size() const { return size_; }

    // Empty the data structure.
    void clear() {
      positionMap_.clear();
      size_ = 0;
    }
  };

 private:
  // The index to which these triples are added.
  const Index& index_;

  // The positions of the delta triples in each of the six permutations.
  TriplesWithPositionsPerBlock triplesWithPositionsPerBlockInPSO_;
  TriplesWithPositionsPerBlock triplesWithPositionsPerBlockInPOS_;
  TriplesWithPositionsPerBlock triplesWithPositionsPerBlockInSPO_;
  TriplesWithPositionsPerBlock triplesWithPositionsPerBlockInSOP_;
  TriplesWithPositionsPerBlock triplesWithPositionsPerBlockInOSP_;
  TriplesWithPositionsPerBlock triplesWithPositionsPerBlockInOPS_;

  // Each inserted or deleted triple needs to know where it is stored in each of
  // the six `TriplesWithPositionsPerBlock` above.
  struct TriplesWithPositionsIterators {
    TriplesWithPositions::iterator iteratorPSO;
    TriplesWithPositions::iterator iteratorPOS;
    TriplesWithPositions::iterator iteratorSPO;
    TriplesWithPositions::iterator iteratorSOP;
    TriplesWithPositions::iterator iteratorOSP;
    TriplesWithPositions::iterator iteratorOPS;
  };

  // The sets of triples added to and subtracted from the original index
  //
  // NOTE: The methods `insertTriple` and `deleteTriple` make sure that only
  // triples are added that are not already contained in the original index and
  // that only triples are subtracted that are contained in the original index.
  // In particular, no triple can be in both of these sets.
  ad_utility::HashMap<IdTriple, TriplesWithPositionsIterators> triplesInserted_;
  ad_utility::HashMap<IdTriple, TriplesWithPositionsIterators> triplesDeleted_;

  // The local vocabulary of these triples.
  LocalVocab localVocab_;

 public:
  // Construct for given index.
  DeltaTriples(const Index& index) : index_(index) {}

  // Clear `_triplesAdded` and `_triplesSubtracted` and all associated data
  // structures.
  void clear();

  // The number of delta triples added and subtracted.
  size_t numInserted() const { return triplesInserted_.size(); }
  size_t numDeleted() const { return triplesDeleted_.size(); }

  // Insert triple.
  void insertTriple(TurtleTriple turtleTriple);

  // Delete triple.
  void deleteTriple(TurtleTriple turtleTriple);

  // Get `TripleWithPosition` objects for given permutation.
  const TriplesWithPositionsPerBlock& getTriplesWithPositionsPerBlock(
      Index::Permutation permutation) const;

  // TODO: made public as long as we are trying to figure out how this works.
 private:
 public:
  // Get triples of `Id`s from `TurtleTriple` (which is the kind of triple we
  // get from `TurtleParser`, see the code currently handling insertions and
  // deletions in `Server.cpp`).
  //
  // NOTE: This is not `const` because translating to IDs may augment the local
  // vocabulary.
  IdTriple getIdTriple(const TurtleTriple& turtleTriple);

  // Find the position of the given triple in the given permutation and add it
  // to each of the six `TriplesWithPositionsPerBlock` maps (one per
  // permutation). Return the iterators of where it was added (so that we can
  // easily delete it again from these maps later).
  //
  // TODO: The function is name is misleading, since this method does not only
  // locate, but also add to the mentioned data structures.
  TriplesWithPositionsIterators locateTripleInAllPermutations(
      const IdTriple& idTriple);

  // The implementation of the above function for a given permutation.
  template <typename Permutation>
  TripleWithPosition locateTripleInPermutation(Id id1, Id id2, Id id3,
                                               Permutation& permutation) const;

  // Erase `TripleWithPosition` object from each `TriplesWithPositionsPerBlock`
  // list. The argument are iterators for each list, as returned by the method
  // `locateTripleInAllPermutations` above.
  //
  // NOTE: The iterators are invalid afterwards. That is OK, as long as we also
  // delete the respective entry in `triplesInserted_` or `triplesDeleted_`,
  // which stores these iterators.
  void eraseTripleInAllPermutations(TriplesWithPositionsIterators& iterators);

  // Resolve ID to name (useful for debugging and testing).
  std::string getNameForId(Id id) const;
};
