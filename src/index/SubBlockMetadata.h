// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_SUBBLOCKMETADATA_H
#define QLEVER_SRC_INDEX_SUBBLOCKMETADATA_H

#include <memory>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "index/CompressedRelationMetadata.h"

// OVERVIEW
//
// The code that selects and reads blocks (for a scan, for a join, or for a
// `PrefilterExpression`) operates on `SubBlockMetadata` values, which are
// obtained from the `PermutationBlockMetadata` of a permutation. A follow-up
// will split the blocks of a permutation (described by
// `CompressedBlockMetadata`) into smaller sub-blocks, which can be read from
// disk in isolation, to make the selection of blocks more fine-grained.
// Currently, each block consists of exactly one sub-block, and
// `SubBlockMetadata` is just another name for `CompressedBlockMetadata`.

// The metadata of a sub-block as it is used by the scans. Currently, each
// block consists of exactly one sub-block, see above.
using SubBlockMetadata = CompressedBlockMetadata;

// A random access range of `SubBlockMetadata` values.
using SubBlockMetadataSpan = ql::span<const SubBlockMetadata>;
// Iterator with respect to a `SubBlockMetadata` value of a
// `SubBlockMetadataSpan`.
using SubBlockMetadataIt = SubBlockMetadataSpan::iterator;
// Section of relevant blocks as a subrange defined by `SubBlockMetadataIt`s.
using SubBlockMetadataRange = ql::ranges::subrange<SubBlockMetadataIt>;
// Vector containing `SubBlockMetadataRange`s.
using SubBlockMetadataRanges = std::vector<SubBlockMetadataRange>;

// The metadata of all blocks and sub-blocks of a permutation. This is the
// interface between the stored metadata (the `CompressedBlockMetadata` of the
// blocks) and the code that selects and reads (sub-)blocks. Copying this class
// is cheap, the actual data is shared.
class PermutationBlockMetadata {
 public:
  using Blocks = std::vector<CompressedBlockMetadata>;
  using PermutedTriple = CompressedBlockMetadata::PermutedTriple;

 private:
  // The metadata of the blocks. With updates, the last block may contain only
  // `LocatedTriples` (and no triples on disk).
  std::shared_ptr<const Blocks> blocks_ = std::make_shared<const Blocks>();

 public:
  PermutationBlockMetadata() = default;
  explicit PermutationBlockMetadata(std::shared_ptr<const Blocks> blocks)
      : blocks_{std::move(blocks)} {
    AD_CONTRACT_CHECK(blocks_ != nullptr);
  }
  // Convenience constructor (mostly for testing).
  explicit PermutationBlockMetadata(Blocks blocks)
      : PermutationBlockMetadata{
            std::make_shared<const Blocks>(std::move(blocks))} {}

  // Return a copy of this metadata, but with the given `blocks` (which have to
  // be the blocks of this metadata, but possibly with updated bounds and graph
  // info, plus possibly one additional block at the end that only consists of
  // `LocatedTriples`).
  PermutationBlockMetadata withAugmentedBlocks(Blocks blocks) const {
    return PermutationBlockMetadata{std::move(blocks)};
  }

  // Return the blocks.
  const Blocks& blocks() const { return *blocks_; }
  const std::shared_ptr<const Blocks>& blocksShared() const { return blocks_; }

  size_t numBlocks() const { return blocks_->size(); }
  size_t numSubBlocks() const { return numBlocks(); }

  // Return the metadata of the sub-block with the (global) index
  // `subBlockIndex`.
  const SubBlockMetadata& subBlock(size_t subBlockIndex) const {
    return blocks_->at(subBlockIndex);
  }

  // Return all the sub-blocks.
  SubBlockMetadataSpan subBlocks() const { return *blocks_; }
};

#endif  // QLEVER_SRC_INDEX_SUBBLOCKMETADATA_H
