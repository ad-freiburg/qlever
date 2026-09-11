// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BLOBCONVERTER_LEGACYBLOBREADER_H
#define QLEVER_SRC_BLOBCONVERTER_LEGACYBLOBREADER_H

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "backports/span.h"
#include "engine/SpatialJoinCachedIndex.h"
#include "engine/VariableToColumnMap.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "util/BlankNodeManager.h"
#include "util/json.h"

// Reading of the legacy blob format that is written by
// `Qlever::serializeToUncompressedBlob` of the `demo-v1-c++17_unimodel` branch
// of the `qlever-bmw` fork. A legacy blob is a single ZSTD frame that is
// followed by the size of the uncompressed data (8 bytes, little endian). The
// uncompressed data consists of:
//
// 1. The magic header `"QLVUBLOB"` (serialized as a `std::string`) and the
//    format version `1` (a `uint32_t`).
// 2. The index metadata JSON (a `std::string`).
// 3. The vocabulary, in the format that the value of `"vocabulary-type"` in the
//    metadata determines (see `LegacyVocabulary`).
// 4. The `NamedResultCache` (see `LegacyNamedCacheEntry`), without any header.
//
// The byte-level rules of the legacy serializer (no padding for single values,
// padding to the alignment of the element type for the contents of vectors,
// spans, and strings) are the same as those of the current
// `AlignedByteBufferWriteSerializer`, so the legacy blob can be read with the
// current aligned read serializer, and all the types whose serialization has
// not changed (strings, vectors, the vocabularies, the
// `SpatialJoinCachedIndex`) are read via their current serialization functions.
namespace qlever::blobConverter {

// The vocabulary implementations of the legacy format that a blob can hold.
// Their byte layout is identical to that of the current implementations of the
// same name, so the current types are used directly.
using LegacyVocabulary =
    std::variant<VocabularyInMemory, CompressedVocabulary<VocabularyInMemory>>;

// One entry of the legacy `NamedResultCache`. The `Id`s of the result are kept
// as raw bits, because their datatype bits have to be reinterpreted for the
// current format (see `LegacyDatatype.h`).
struct LegacyNamedCacheEntry {
  std::string name_;
  std::vector<
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>
      blankNodeBlocks_;
  // The words of the `LocalVocab` of the entry, each preceded by the bits of
  // the legacy `Id` that referred to it.
  std::vector<std::pair<uint64_t, std::string>> localVocabWords_;
  size_t numRows_ = 0;
  size_t numColumns_ = 0;
  // `numColumns_` columns with `numRows_` legacy `Id`s each.
  std::vector<std::vector<uint64_t>> columns_;
  // The variable names (including the leading `?`) and their columns.
  std::vector<std::pair<std::string, ColumnIndexAndTypeInfo>> variables_;
  std::vector<ColumnIndex> resultSortedOn_;
  std::string cacheKey_;
  // The layout of the geo index has not changed, and it only refers to row
  // indices of the result, so it is read directly into the current type.
  std::optional<SpatialJoinCachedIndex> geoIndex_;
};

// The complete contents of a legacy blob.
struct LegacyBlob {
  nlohmann::json metadata_;
  LegacyVocabulary vocabulary_;
  std::vector<LegacyNamedCacheEntry> entries_;

  // The number of words in the vocabulary, and the word at a given index.
  size_t numWords() const;
  std::string word(uint64_t index) const;

  // Find the entry with the given `name`, or return `nullptr`.
  const LegacyNamedCacheEntry* findEntry(std::string_view name) const;
};

// The buffer for the decompressed blob. It is aligned to the maximal possible
// alignment, which the aligned read serializer requires.
using DecompressedBuffer =
    std::vector<char, NamedCachedQueryBlobManager::BlobAllocator>;

// Decompress a legacy blob (a ZSTD frame followed by the 8-byte uncompressed
// size). Throw a `std::runtime_error` with a descriptive message if the input
// is not a legacy blob.
DecompressedBuffer decompressLegacyBlob(ql::span<const char> compressedBlob);

// Read the decompressed contents of a legacy blob (see `decompressLegacyBlob`).
// Throw a `std::runtime_error` if the header is wrong, if the vocabulary type
// is not supported, if the contents are inconsistent, or if not all the bytes
// of the input are consumed. NOTE: The `decompressedBlob` has to be aligned to
// `alignof(std::max_align_t)` (which a `DecompressedBuffer` guarantees).
LegacyBlob readLegacyBlob(ql::span<const char> decompressedBlob);

// The combination of `decompressLegacyBlob` and `readLegacyBlob`.
LegacyBlob readLegacyBlobFromCompressed(ql::span<const char> compressedBlob);

}  // namespace qlever::blobConverter

#endif  // QLEVER_SRC_BLOBCONVERTER_LEGACYBLOBREADER_H
