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

#include "index/CompressedRelationMetadata.h"

#include <array>

// Extract the Ids from the given `PermutedTriple` in an array w.r.t. the
// position (column index) defined by `ignoreIndex`. The ignored positions are
// filled with Ids `Id::min()`. `Id::min()` is guaranteed
// to be smaller than Ids of all other types.
static std::array<Id, 3> getMaskedTriple(
    const CompressedBlockMetadata::PermutedTriple& triple,
    size_t ignoreIndex = 3) {
  const Id& undefined = Id::min();
  switch (ignoreIndex) {
    case 3:
      return {triple.col0Id_, triple.col1Id_, triple.col2Id_};
    case 2:
      return {triple.col0Id_, triple.col1Id_, undefined};
    case 1:
      return {triple.col0Id_, undefined, undefined};
    case 0:
      return {undefined, undefined, undefined};
    default:
      // ignoreIndex out of bound.
      AD_FAIL();
  }
}

bool CompressedBlockMetadataNoBlockIndex::containsInconsistentTriples(
    size_t columnIndex) const {
  return getMaskedTriple(firstTriple_, columnIndex) !=
         getMaskedTriple(lastTriple_, columnIndex);
}

bool CompressedBlockMetadataNoBlockIndex::isConsistentWith(
    const CompressedBlockMetadataNoBlockIndex& other,
    size_t columnIndex) const {
  return getMaskedTriple(lastTriple_, columnIndex) ==
         getMaskedTriple(other.firstTriple_, columnIndex);
}

// _____________________________________________________________________________
CompressedBlockMetadataNoBlockIndex::OffsetAndCompressedSize
CompressedBlockMetadataNoBlockIndex::getOffsetAndCompressedSizeForColumn(
    ColumnIndex columnIndex) const {
  if (!offsetsAndCompressedSize_.has_value()) {
    return {0, 0};
  }
  return offsetsAndCompressedSize_.value().at(columnIndex);
}
