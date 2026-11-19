// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_SEGMENTEDBLOBDIFF_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_SEGMENTEDBLOBDIFF_H

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "backports/span.h"
#include "libqlever/BlobCompression.h"
#include "libqlever/blobDiff/BlobDiff.h"
#include "util/memorySize/MemorySize.h"

namespace qlever::blobDiff {

// A SEGMENTED blob diff turns a compressed SEGMENTED blob `base` into a
// compressed segmented blob `target` (see `BlobCompression.h`) segment by
// segment:
//
//   base:    [ vocabulary ][ secondary vocab  ][ named results  ]
//                 │ copy          │ patch             │ patch
//   target:  [ vocabulary ][ secondary vocab' ][ named results' ]
//
// Each segment of the `target` is either a COPY of a segment of the `base`
// with the same content, or a PATCH: the content of the segment, compressed
// with ZSTD using one segment of the `base` (normally the one at the same
// position) as a reference (like `zstd --patch-from`, see `BlobDiff.h`).
//
// When the diff is applied (`applySegmentedBlobDiff`), the copied segments are
// neither decompressed nor compressed again: their compressed blocks are
// copied from the `base` to the `target` (see `SegmentedZstdFrame.h`). Only
// the base segments that are references of patches are decompressed, and only
// the patched segments are compressed. The identity of the base and the
// correctness of the result are verified via the hashes of the segments: the
// hashes of the copied segments are compared with the hashes in the index of
// the base, and the patched segments are hashed after they are decompressed.
//
// FILE FORMAT (all integers in the native byte order, which is little endian
// on all platforms that QLever currently supports):
//   8 bytes   the magic bytes `QLVRBDIF` (the same as for `BlobDiff.h`)
//   uint16_t  the format version 3
//   int32_t   the ZSTD compression level of the patched segments of the target
//   uint64_t  the number of segments of the target
//   for each segment of the target (57 bytes each):
//     uint64_t  the size of the segment
//     32 bytes  the SHA-256 hash of the segment
//     uint8_t   0 for a copy, 1 for a patch
//     uint64_t  the index of the copied segment in the base, or the index of
//               the reference segment for a patch (`noReference` for none)
//     uint64_t  the size of the payload (0 for a copy)
//   the payloads of the patches, in the order of the segments

// The reference of a patch whose segment is compressed without a reference.
constexpr uint64_t noReference = std::numeric_limits<uint64_t>::max();

// Return true iff `diffFile` starts with the magic bytes and the version of a
// segmented blob diff (the rest is not validated).
bool isSegmentedBlobDiff(ql::span<const char> diffFile);

// Compute the segmented diff that turns the compressed segmented blob
// `compressedBase` into the compressed segmented blob `compressedTarget`. The
// patched segments of the target are compressed with the
// `targetCompressionLevel` when the diff is applied. The payloads of the diff
// are compressed with the `diffCompressionLevel`. Throw if one of the blobs is
// not a segmented blob.
std::vector<char> computeSegmentedBlobDiff(
    ql::span<const char> compressedBase, ql::span<const char> compressedTarget,
    int targetCompressionLevel = blobCompression::defaultCompressionLevel,
    int diffCompressionLevel = defaultBlobDiffCompressionLevel);

// Apply the segmented `diffFile` to the compressed segmented blob
// `compressedBase` and return the compressed segmented target blob (see
// above). Report an error if the `diffFile` is invalid or corrupted, if it does
// not belong to the `compressedBase`, if the `compressedBase` is not a
// segmented blob or corrupted, or if the target would be larger than
// `maxTargetSize`. Do not throw for any input, except if memory cannot be
// allocated.
BlobOrError applySegmentedBlobDiff(ql::span<const char> compressedBase,
                                   ql::span<const char> diffFile,
                                   ad_utility::MemorySize maxTargetSize);

// Return a human-readable description of the segmented `diffFile`. Throw if it
// is invalid.
std::string describeSegmentedBlobDiff(ql::span<const char> diffFile);

}  // namespace qlever::blobDiff

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_SEGMENTEDBLOBDIFF_H
