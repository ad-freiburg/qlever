// Copyright 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include "engine/LocalVocab.h"
#include "global/Id.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "parser/TurtleParser.h"
#include "util/HashSet.h"

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

  // Result record returned by `findTripleInPermutation`, containing: the
  // reference to the permutation, the index of the matching block, the index of
  // a position within that block, and the search triple in the right
  // permutation (for example, for SPO, `id1` is the subject, `id2` is the
  // predicate, and `id3` is the object).
  // template <typename Permutation>
  struct FindTripleResult {
    // const Permutation& permutation;
    size_t blockIndex;
    size_t rowIndexInBlock;
    Id id1;
    Id id2;
    Id id3;
  };

  // Data structures with position for a particular table (just an example, this
  // is not the final data structure).
  using Position = std::pair<size_t, IdTriple>;
  using Positions = ad_utility::HashMap<size_t, std::vector<Position>>;

 private:
  // The index to which these triples are added.
  const Index& index_;

  // The sets of triples added to and subtracted from the original index
  //
  // NOTE: The methods for adding and subtracting should make sure that only
  // triples are added that are not already contained in the original index and
  // that only triples are subtracted that are contained in the original index.
  // In particular, no triple can be in both of these sets.
  ad_utility::HashSet<IdTriple> triplesInserted_;
  ad_utility::HashSet<IdTriple> triplesDeleted_;

  // The local vocabulary of these triples.
  LocalVocab localVocab_;

 public:
  // The positions of the delta triples in each of the six permutations.
  //
  // TODO: Do the positions need to know to which permutation they belong?
  std::vector<FindTripleResult> posFindTripleResults_;
  std::vector<FindTripleResult> psoFindTripleResults_;
  std::vector<FindTripleResult> sopFindTripleResults_;
  std::vector<FindTripleResult> spoFindTripleResults_;
  std::vector<FindTripleResult> opsFindTripleResults_;
  std::vector<FindTripleResult> ospFindTripleResults_;

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

  // TODO: made public as long as we are trying to figure out how this works.
 private:
 public:
  // Get triples of `Id`s from `TurtleTriple` (which is the kind of triple we
  // get from `TurtleParser`, see the code currently handling insertions and
  // deletions in `Server.cpp`).
  //
  // NOTE: This is not `const` because translating to IDs may augment the local
  // vocabulary.
  IdTriple getIdTriple(TurtleTriple turtleTriple);

  // Find the position of the given triple in the given permutation (a pair of
  // block index and index within that block; see the documentation of the class
  // above for how exactly that position is defined in all cases).
  void findTripleInAllPermutations(const IdTriple& idTriple,
                                   bool visualize = false);

  // The implementation of the above function.
  template <typename Permutation>
  FindTripleResult findTripleInPermutation(
      Id id1, Id id2, Id id3, Permutation& permutation, bool visualize) const;

  // Resolve ID to name (useful for debugging and testing).
  std::string getNameForId(Id id) const;
};
