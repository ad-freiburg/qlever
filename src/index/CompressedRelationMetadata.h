// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2024 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Hannes Baumann <baumannh@cs.uni-freiburg.de>, UFR
// 2025        Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026        Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONMETADATA_H
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONMETADATA_H

#include <limits>
#include <optional>
#include <ostream>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "util/Exception.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeOptional.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/StringUtils.h"

// The metadata of a compressed block of ID triples in an index permutation.
struct CompressedBlockMetadataNoBlockIndex {
  // Since we have column-based indices, the two columns of each block are
  // stored separately (but adjacently).
  struct OffsetAndCompressedSize {
    off_t offsetInFile_;
    size_t compressedSize_;
    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(OffsetAndCompressedSize,
                                                offsetInFile_, compressedSize_)
  };

  using GraphInfo = std::optional<std::vector<Id>>;

  // For each column, the offset and compressed size of the column in the
  // underlying file. `std::nullopt` is currently used for the last block which
  // purely consists of `LocatedTriples`, and thus is not stored at all in the
  // underlying file.
  std::optional<std::vector<OffsetAndCompressedSize>> offsetsAndCompressedSize_;
  size_t numRows_;

  // Store the first and the last triple of the block. First and last are meant
  // inclusively, that is, they are both part of the block. The order of the
  // triples depends on the stored permutation: For example, in the PSO
  // permutation, the first element of the triples is the P, the second one is
  // the S and the third one is the O. Note that the first key of the
  // permutation (for example the P in the PSO permutation) is not stored in
  // the blocks, but has to be retrieved via the corresponding
  // `CompressedRelationMetadata`.
  //
  // NOTE: Strictly speaking, storing one of `firstTriple_` or `lastTriple_`
  // would probably suffice. However, they make several functions much easier
  // to implement and don't really harm with respect to space efficiency. For
  // example, for Wikidata, we have only around 50K blocks with block size 8M
  // and around 5M blocks with block size 80K; even the latter takes only half
  // a GB in total.
  struct PermutedTriple {
    Id col0Id_;
    Id col1Id_;
    Id col2Id_;
    Id graphId_;

    QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(PermutedTriple, col0Id_,
                                                col1Id_, col2Id_, graphId_)

    // Formatted output for debugging.
    friend std::ostream& operator<<(std::ostream& str,
                                    const PermutedTriple& trip) {
      str << "Triple: " << trip.col0Id_ << ' ' << trip.col1Id_ << ' '
          << trip.col2Id_ << ' ' << trip.graphId_ << std::endl;
      return str;
    }

    template <typename T>
    friend std::true_type allowTrivialSerialization(PermutedTriple, T);

    // Helper function to make `PermutedTriple` easier to compare without
    // `graphId_`.
    auto tieWithoutGraph() const { return std::tie(col0Id_, col1Id_, col2Id_); }
  };
  PermutedTriple firstTriple_;
  PermutedTriple lastTriple_;

  // If there are only few graphs contained at all in this block, then
  // the IDs of those graphs are stored here. If there are many different graphs
  // inside this block, `std::nullopt` is stored.
  std::optional<std::vector<Id>> graphInfo_;
  // True if and only if this block contains (adjacent) triples which only
  // differ in their Graph ID. Those have to be filtered out when scanning the
  // blocks.
  bool containsDuplicatesWithDifferentGraphs_;

  // Check for constant values in `firstTriple_` and `lastTriple` over all
  // columns `< columnIndex`.
  // Returns `true` if the respective column values of `firstTriple_` and
  // `lastTriple_` differ.
  bool containsInconsistentTriples(size_t columnIndex) const;

  // Check if `lastTriple_` of this block contains consistent `ValueId`s up to
  // `columnIndex` compared to `firstTriple_` of block `other`.
  bool isConsistentWith(const CompressedBlockMetadataNoBlockIndex& other,
                        size_t columnIndex) const;

  // Get the offset and compressed size for the given column.
  OffsetAndCompressedSize getOffsetAndCompressedSizeForColumn(
      ColumnIndex columnIndex) const;

  // Two of these are equal if all members are equal.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(
      CompressedBlockMetadataNoBlockIndex, offsetsAndCompressedSize_, numRows_,
      firstTriple_, lastTriple_, graphInfo_,
      containsDuplicatesWithDifferentGraphs_)

  // Format CompressedBlockMetadata contents for debugging.
  friend std::ostream& operator<<(
      std::ostream& str,
      const CompressedBlockMetadataNoBlockIndex& blockMetadata) {
    str << "#CompressedBlockMetadata\n(first) " << blockMetadata.firstTriple_
        << "(last) " << blockMetadata.lastTriple_
        << "num. rows: " << blockMetadata.numRows_ << ".\n";
    if (blockMetadata.graphInfo_.has_value()) {
      str << "Graphs: ";
      ad_utility::lazyStrJoin(&str, blockMetadata.graphInfo_.value(), ", ");
      str << '\n';
    }
    str << "[possibly] contains duplicates: "
        << blockMetadata.containsDuplicatesWithDifferentGraphs_ << '\n';
    return str;
  }
};

// The same as the above struct, but this block additionally knows its index.
struct CompressedBlockMetadata : CompressedBlockMetadataNoBlockIndex {
  // The index of this block in the permutation. This is required to find
  // the corresponding block from the `LocatedTriples` when only a subset of
  // blocks is being used.
  size_t blockIndex_;

  // Two of these are equal if all members are equal (including the members of
  // the base class).
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_DERIVED(
      CompressedBlockMetadata, CompressedBlockMetadataNoBlockIndex, blockIndex_)

  // Format CompressedBlockMetadata contents for debugging.
  friend std::ostream& operator<<(
      std::ostream& str, const CompressedBlockMetadata& blockMetadata) {
    str << static_cast<const CompressedBlockMetadataNoBlockIndex&>(
        blockMetadata);
    str << "block index: " << blockMetadata.blockIndex_ << "\n";
    return str;
  }

  // Return true if a sequence of `CompressedBlockMetadata` is sorted, and if
  // all the triples that are the same when disregarding the graph are in the
  // same block.
  template <typename SequenceOfBlocks>
  static bool checkInvariantsForSortedBlocks(
      const SequenceOfBlocks& sequenceOfBlocks) {
    return ::ranges::all_of(
        ::ranges::views::sliding(sequenceOfBlocks, 2),
        [](const auto& adjacent) {
          const auto& first = adjacent.front().lastTriple_;
          const auto& second = adjacent.back().firstTriple_;
          return (first < second) &&
                 (first.tieWithoutGraph() != second.tieWithoutGraph());
        });
  }
};

// Serialization of the `OffsetAndcompressedSize` subclass.
AD_SERIALIZE_FUNCTION(CompressedBlockMetadata::OffsetAndCompressedSize) {
  serializer | arg.offsetInFile_;
  serializer | arg.compressedSize_;
}

// Serialization of the block metadata.
AD_SERIALIZE_FUNCTION(CompressedBlockMetadata) {
  if constexpr (ad_utility::serialization::WriteSerializer<S>) {
    AD_CORRECTNESS_CHECK(arg.offsetsAndCompressedSize_.has_value(),
                         "When serializing blocks offsets and compressed sizes "
                         "need to be present.");
  } else {
    static_assert(ad_utility::serialization::ReadSerializer<S>);
    // Insert a dummy to overwrite.
    arg.offsetsAndCompressedSize_.emplace();
  }
  serializer | arg.offsetsAndCompressedSize_.value();
  serializer | arg.numRows_;
  serializer | arg.firstTriple_;
  serializer | arg.lastTriple_;
  serializer | arg.graphInfo_;
  serializer | arg.containsDuplicatesWithDifferentGraphs_;
  serializer | arg.blockIndex_;
}

// `ql::span` containing `CompressedBlockMetadata` values.
using BlockMetadataSpan = ql::span<const CompressedBlockMetadata>;
// Iterator with respect to a `CompressedBlockMetadata` value of
// `std::span<const CompressedBlockMetadata>` (`BlockMetadataSpan`).
using BlockMetadataIt = BlockMetadataSpan::iterator;
// Section of relevant blocks as a subrange defined by `BlockMetadataIt`s.
using BlockMetadataRange = ql::ranges::subrange<BlockMetadataIt>;
// Vector containing `BlockMetadataRange`s.
using BlockMetadataRanges = std::vector<BlockMetadataRange>;

// The metadata of a whole compressed "relation", where relation refers to a
// maximal sequence of triples with equal first component (e.g., P for the PSO
// permutation).
struct CompressedRelationMetadata {
  Id col0Id_;
  // TODO: Is this still needed? Same for `offsetInBlock_`.
  size_t numRows_;
  float multiplicityCol1_;  // E.g., in PSO this is the multiplicity of "S".
  float multiplicityCol2_;  // E.g., in PSO this is the multiplicity of "O".
  // If this "relation" is contained in a block together with other "relations",
  // then all of these relations are contained only in this block and
  // `offsetInBlock_` stores the offset in this block (referring to the index in
  // the uncompressed sequence of triples).  Otherwise, this "relation" is
  // stored in one or several blocks of its own, and we set `offsetInBlock_` to
  // `Id(-1)`.
  uint64_t offsetInBlock_ = std::numeric_limits<uint64_t>::max();

  size_t getNofElements() const { return numRows_; }

  // Setters and getters for the multiplicities.
  float getCol1Multiplicity() const { return multiplicityCol1_; }
  float getCol2Multiplicity() const { return multiplicityCol2_; }
  void setCol1Multiplicity(float mult) { multiplicityCol1_ = mult; }
  void setCol2Multiplicity(float mult) { multiplicityCol2_ = mult; }

  bool isFunctional() const { return multiplicityCol1_ == 1.0f; }

  // Two of these are equal if all members are equal.
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(CompressedRelationMetadata,
                                              col0Id_, numRows_,
                                              multiplicityCol1_,
                                              multiplicityCol2_, offsetInBlock_)
};

// Serialization of the compressed "relation" meta data.
AD_SERIALIZE_FUNCTION(CompressedRelationMetadata) {
  serializer | arg.col0Id_;
  serializer | arg.numRows_;
  serializer | arg.multiplicityCol1_;
  serializer | arg.multiplicityCol2_;
  serializer | arg.offsetInBlock_;
}

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONMETADATA_H
