// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBLAYOUT_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBLAYOUT_H

#include <cstdint>
#include <string>
#include <vector>

#include "backports/span.h"
#include "engine/NamedResultCacheSerializer.h"
#include "engine/SpatialJoinCachedIndex.h"
#include "index/vocabulary/SecondaryVocabulary.h"
#include "util/Serializer/LayoutReadSerializer.h"

namespace qlever {

// The layouts of the sections of a blob. Each section is parsed by the
// component that also writes it (next to its serialization, so that the two
// stay in sync); `BlobLayout` only composes them.
using ByteRange = ad_utility::serialization::ByteRange;
using SecondaryVocabLayout = SecondaryVocabulary::Layout;
using GeoIndexLayout = SpatialJoinCachedIndex::Layout;
using EntryLayout = namedResultCacheSerializer::EntryLayout;

// A description of the byte ranges of the sections of a DECOMPRESSED blob as
// written by `NamedCachedQueryBlobManager::serialize`. It is the basis for
// computing diffs between two blobs section by section.
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
  // The secondary vocabulary, which only blobs with the format version
  // `NamedCachedQueryBlobManager::formatVersionWithSecondaryVocab` have (see
  // `SecondaryVocabulary::Layout::present_`).
  SecondaryVocabLayout secondaryVocab_;
  // The magic byte, version, and number of entries of the named cache, and the
  // range of all entries including that header.
  ByteRange entriesHeader_;
  ByteRange entries_;
  std::vector<EntryLayout> entryLayouts_;

  // Parse the layout of the decompressed blob. The blob has to be aligned to
  // `alignof(std::max_align_t)` (as the result of
  // `NamedCachedQueryBlobManager::tryToDecompressBlob` is). Read each section
  // via the reader of the component that writes it (see the `parseLayout`
  // functions above), and throw if the blob is malformed (or has trailing
  // bytes).
  static BlobLayout parse(ql::span<const char> decompressedBlob);

  // Return a human-readable description of the sizes of all the sections.
  std::string describe() const;
};

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBLAYOUT_H
