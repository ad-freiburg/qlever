// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBCOMPRESSION_H
#define QLEVER_SRC_LIBQLEVER_BLOBCOMPRESSION_H

#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"

// The compression of the blobs of `NamedCachedQueryBlobManager`.
//
// A compressed blob is always a single ZSTD frame that holds the complete
// (decompressed) blob, so every compressed blob can be read by
// `NamedCachedQueryBlobManager::tryToDecompressBlob` (also by the versions of
// QLever that preceded this file, and with every version of ZSTD).
//
// Optionally, the blob is a SEGMENTED frame (see
// `util/compressionUsingZstd/SegmentedZstdFrame.h`): its content is divided
// into segments (for example the vocabulary, the secondary vocabulary, and the
// named results), and a skippable frame at the end, which every ZSTD
// decompressor ignores, stores the size and the hash of each segment. The
// compressed form of the unchanged segments can then be copied to a new blob
// without decompressing and compressing them again, in particular when a blob
// diff is applied (see `blobDiff/SegmentedBlobDiff.h`).
namespace qlever::blobCompression {

// The default ZSTD compression level of a blob. The low levels are much faster
// than the default level 3 of ZSTD and the result is only slightly larger.
constexpr int defaultCompressionLevel = 1;

// How a blob is compressed.
struct BlobEncoding {
  // The ZSTD compression level.
  int compressionLevel_ = defaultCompressionLevel;
  // The sizes of the segments of the blob, which must add up to the size of
  // the blob. Without them, the blob is written as an ordinary ZSTD frame.
  std::optional<std::vector<uint64_t>> segmentSizes_;
};

// Compress `uncompressedBlob` with the given `encoding`. If the blob is
// segmented and `reusableBase` is a compressed segmented blob, then the
// compressed form of each segment (or group of segments, see
// `compressSegmented`) that has the same content in the `reusableBase` is
// copied from there.
std::vector<char> compressBlob(ql::span<const char> uncompressedBlob,
                               const BlobEncoding& encoding = {},
                               ql::span<const char> reusableBase = {});

}  // namespace qlever::blobCompression

#endif  // QLEVER_SRC_LIBQLEVER_BLOBCOMPRESSION_H
