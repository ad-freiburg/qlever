// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBLAYOUT_H
#define QLEVER_SRC_LIBQLEVER_BLOBLAYOUT_H

#include <cstdint>
#include <string>
#include <vector>

#include "backports/span.h"
#include "backports/three_way_comparison.h"

namespace qlever {

// A half-open range `[begin_, end_)` of byte offsets into a decompressed blob.
struct ByteRange {
  uint64_t begin_ = 0;
  uint64_t end_ = 0;

  // Return the number of bytes in this range.
  uint64_t size() const { return end_ - begin_; }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(ByteRange, begin_, end_)
};

// The layout of the geo index of a named cache entry (see
// `SpatialJoinCachedIndex`).
struct GeoIndexLayout {
  // The whole serialized geo index, including all its internal framing.
  ByteRange whole_;
  // Only for entries format version 2: the payload bytes of the serialized S2
  // index of each segment, and the payload of the `rowToShape_` array.
  std::vector<ByteRange> segmentPayloads_;
  ByteRange rowToShape_;
};

// The layout of one named cache entry (key and value) of a blob.
struct EntryLayout {
  std::string key_;
  // The complete entry, starting with its key.
  ByteRange whole_;
  size_t numRows_ = 0;
  size_t numColumns_ = 0;
  // For each column the range of the raw `Id`s (`numRows_ * 8` bytes), after
  // the size and the alignment padding.
  std::vector<ByteRange> columnPayloads_;
  // The payload bytes of the `resultSortedOn_` array, and its contents.
  ByteRange resultSortedOn_;
  std::vector<uint64_t> sortedOnColumns_;
  // The serialized `VariableToColumnMap`, and the cache key (payload bytes).
  ByteRange varToColMap_;
  ByteRange cacheKey_;
  bool hasGeoIndex_ = false;
  GeoIndexLayout geo_;
};

// The layout of the secondary vocabulary of a blob.
struct SecondaryVocabLayout {
  bool present_ = false;
  // The complete serialized secondary vocabulary.
  ByteRange whole_;
  // The complete serialization of each segment.
  std::vector<ByteRange> segments_;
  // The payload bytes of `segmentOffsets_` and `sortedIndices_`.
  ByteRange segmentOffsets_;
  ByteRange sortedIndices_;
};

// A description of the byte ranges of the sections of a DECOMPRESSED blob as
// written by `NamedCachedQueryBlobManager::serialize`. It is the basis for
// computing diffs between two blobs (see `BlobDiff.h`).
struct BlobLayout {
  uint16_t blobVersion_ = 0;
  uint16_t entriesVersion_ = 0;
  uint64_t totalSize_ = 0;
  // The magic bytes and the blob version.
  ByteRange header_;
  // The payload bytes (not the length) of the metadata JSON string.
  ByteRange metadata_;
  // The main vocabulary, which is opaque.
  ByteRange vocabulary_;
  SecondaryVocabLayout secondaryVocab_;
  // The magic byte, version, and number of entries of the named cache, and the
  // range of all entries including that header.
  ByteRange entriesHeader_;
  ByteRange entries_;
  std::vector<EntryLayout> entryLayouts_;

  // Parse the layout of the decompressed blob. The blob has to be aligned to
  // `alignof(std::max_align_t)`. The parsing mirrors the writer exactly and
  // throws if the blob is malformed (or has trailing bytes).
  static BlobLayout parse(ql::span<const char> decompressedBlob);

  // Return a human-readable description of the sizes of all the sections.
  std::string describe() const;
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_BLOBLAYOUT_H
