// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_AUXLOCATEDTRIPLES_H
#define QLEVER_SRC_INDEX_AUXLOCATEDTRIPLES_H

#include <memory>
#include <vector>

#include "index/AuxIndex.h"
#include "index/LocatedTriples.h"
#include "util/HashMap.h"
#include "util/LruCache.h"
#include "util/Synchronized.h"

// The triples of one permutation of an auxiliary index (see
// `index/AuxIndex.h`), presented as located triples of the corresponding
// permutation of the main index, so that a scan of the main index can merge
// them in exactly like it merges in the delta triples that are held in RAM (see
// `LocatedTriplesPerBlock`, which owns an instance of this class).
//
// The triples are *not* held in RAM. Instead, they are read from the auxiliary
// index on demand, one block of the main index at a time. This works well
// because both permutations are sorted in the same order, so the triples that
// belong to a block of the main index are a contiguous range of the auxiliary
// permutation, and that range is found from the block metadata of the two
// permutations alone (see `auxBlockRange`). To avoid decompressing the same
// block of the auxiliary index once for each block of the main index that it
// overlaps, the most recently used blocks are cached.
class AuxLocatedTriples {
 public:
  // A single triple of the auxiliary index, in the order of its permutation.
  struct AuxTriple {
    IdTriple<0> triple_;
    bool insertOrDelete_;
  };
  using AuxBlock = std::vector<AuxTriple>;

  // What the triples of the auxiliary index contribute to one block of the main
  // index. This is computed once, by a single sequential pass over the
  // auxiliary permutation (see the constructor), because the augmented block
  // metadata of the main permutation is recomputed after every update and needs
  // the *exact* bounds: the blocks of the augmented metadata must not overlap
  // (see `CompressedBlockMetadata::checkInvariantsForSortedBlocks`), and the
  // bounds of the *blocks* of the auxiliary index are too coarse for that,
  // because one of them can span many blocks of the main index.
  //
  // NOTE: This is the only part of the auxiliary index that is held in RAM, and
  // it is one small entry per block of the main index that the auxiliary index
  // has triples for -- not per triple.
  struct BlockInfo {
    CompressedBlockMetadata::PermutedTriple firstTriple_;
    CompressedBlockMetadata::PermutedTriple lastTriple_;
    size_t numTriples_ = 0;
    // The graphs of the *inserted* triples, or `std::nullopt` if there are too
    // many of them (see `computeDistinctGraphs`).
    std::optional<std::vector<Id>> graphsOfInsertedTriples_{std::vector<Id>{}};
  };

 private:
  using BlockMetadata = std::vector<CompressedBlockMetadata>;

  std::shared_ptr<const AuxIndex> auxIndex_;
  Permutation::Enum permutation_;
  bool isInternal_;
  // The block metadata of the corresponding permutation of the *main* index,
  // which defines to which block of the main index a triple belongs.
  std::shared_ptr<const BlockMetadata> mainBlockMetadata_;
  // The block metadata of the permutation of the auxiliary index.
  ql::span<const CompressedBlockMetadata> auxBlockMetadata_;

  ad_utility::HashMap<size_t, BlockInfo> blockInfos_;

  // The cache for the decompressed blocks of the auxiliary index, see the class
  // comment. It is `mutable` because reading triples does not change the
  // observable state of this class.
  static constexpr size_t cacheSize_ = 32;
  mutable ad_utility::Synchronized<
      ad_utility::util::LRUCache<size_t, std::shared_ptr<const AuxBlock>>>
      blockCache_{cacheSize_};

 public:
  // Construct from the given permutation of `auxIndex` and the block metadata
  // of the corresponding permutation of the main index.
  AuxLocatedTriples(std::shared_ptr<const AuxIndex> auxIndex,
                    Permutation::Enum permutation, bool isInternal,
                    std::shared_ptr<const BlockMetadata> mainBlockMetadata);

  // Return true iff this permutation of the auxiliary index has no triples at
  // all.
  bool isEmpty() const { return auxBlockMetadata_.empty(); }

  // Return true iff there are triples that belong to the block of the main
  // index with the given index.
  bool containsTriples(size_t mainBlockIndex) const {
    return blockInfos_.contains(mainBlockIndex);
  }

  // The number of triples that belong to the block of the main index with the
  // given index.
  size_t numTriples(size_t mainBlockIndex) const;

  // The triples that belong to the block of the main index with the given
  // index, sorted, and with their `blockIndex_` set to `mainBlockIndex`. This
  // reads the relevant blocks of the auxiliary index (possibly from the cache).
  std::vector<LocatedTriple> getTriplesForBlock(size_t mainBlockIndex) const;

  // The exact bounds of the triples that belong to the block of the main index
  // with the given index, and the graphs of those of them that are insertions
  // (`std::nullopt` means that there are too many graphs to keep track of).
  // Used to make the augmented block metadata of the main permutation account
  // for the triples of the auxiliary index, see
  // `LocatedTriplesPerBlock::updateAugmentedMetadata`. Return `std::nullopt` if
  // there are no such triples.
  const BlockInfo* blockInfo(size_t mainBlockIndex) const;

 private:
  // The range `[first, last)` of blocks of the auxiliary index that may contain
  // triples that belong to the block of the main index with the given index.
  // Note that `mainBlockIndex == mainBlockMetadata_->size()` is valid and
  // denotes the (synthetic) block that holds all triples that are larger than
  // all triples of the main permutation.
  std::pair<size_t, size_t> auxBlockRange(size_t mainBlockIndex) const;

  // The triples of the block of the auxiliary index with the given index, read
  // from disk or taken from the cache.
  std::shared_ptr<const AuxBlock> getAuxBlock(size_t auxBlockIndex) const;

  // Compute `blockInfos_` by a single sequential pass over the permutation of
  // the auxiliary index. Called by the constructor.
  void computeBlockInfos();
};

#endif  // QLEVER_SRC_INDEX_AUXLOCATEDTRIPLES_H
