// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFF_H
#define QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFF_H

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "backports/span.h"

namespace qlever {

// A blob diff file turns a (compressed) blob `base` into a (compressed) blob
// `target`. It is a generic binary delta: the decompressed `target` is
// compressed with ZSTD using the decompressed `base` as a reference (the
// "prefix dictionary" of ZSTD, the same mechanism as `zstd --patch-from`), so
// every piece of the `target` that also occurs somewhere in the `base` costs
// only a few bytes. The diff is small if the `target` was written
// incrementally against the `base` (see `BlobSerializationConfig::
// IncrementalBase`), because then the `Id`s of all words and the segments of
// the secondary vocabulary and of the geo indices are stable, and the tables
// of the named results only differ by the inserted and deleted rows. The diff
// knows nothing about the layout of a blob, which makes it robust against
// changes of the blob format.
//
// FILE FORMAT (all integers little endian):
//   8 bytes   the magic bytes `QLVRBDIF`
//   uint16_t  the format version (currently 2; version 1 was a layout-based
//             diff of a prototype, which is not supported)
//   uint64_t  the size of the decompressed base
//   32 bytes  the SHA-256 checksum of the decompressed base
//   uint64_t  the size of the decompressed target
//   32 bytes  the SHA-256 checksum of the decompressed target
//   uint64_t  the size of the payload
//   payload   a ZSTD frame of the decompressed target, compressed with the
//             decompressed base as reference
// The checksums identify the base (a diff applies to exactly one base) and
// verify the result of `applyBlobDiff` (the ZSTD frame itself only stores a
// 32-bit checksum, which detects a wrong base only with high probability).

// The fields of the header of a diff file (see the file format above).
struct BlobDiffHeader {
  using Checksum = std::array<char, 32>;
  uint64_t baseSize_ = 0;
  Checksum baseChecksum_{};
  uint64_t targetSize_ = 0;
  Checksum targetChecksum_{};
  uint64_t payloadSize_ = 0;
};

// The ZSTD compression level that is used for the payload of a diff if none
// is given. The high levels give noticeably smaller diffs (about 25 percent
// smaller than the default level 3 on the data that was measured), and the
// diff of a blob of a few hundred megabytes still takes only seconds.
constexpr int defaultBlobDiffCompressionLevel = 19;

// Compute the SHA-256 checksum of `bytes`.
BlobDiffHeader::Checksum blobChecksum(ql::span<const char> bytes);

// Compute the diff file that turns the decompressed blob `base` into the
// decompressed blob `target`.
std::vector<char> computeBlobDiff(
    ql::span<const char> base, ql::span<const char> target,
    int compressionLevel = defaultBlobDiffCompressionLevel);

// Parse the header of `diffFile`. Throw if `diffFile` is not a diff file, has
// an unsupported version, or is truncated.
BlobDiffHeader readBlobDiffHeader(ql::span<const char> diffFile);

// Apply `diffFile` to the decompressed blob `base` and return the decompressed
// target blob. Throw if the diff does not belong to the `base` (size or
// checksum differ), or if the result does not have the checksum that is stored
// in the diff.
std::vector<char> applyBlobDiffToDecompressed(ql::span<const char> base,
                                              ql::span<const char> diffFile);

// Apply `diffFile` to the compressed blob `compressedBase` and return the
// compressed target blob (a standard blob that can be loaded by
// `Qlever::deserializeVocabAndNamedCacheFromCompressedBlob`). Throw if the base
// cannot be decompressed, or if the diff does not belong to it (see above).
std::vector<char> applyBlobDiff(ql::span<const char> compressedBase,
                                ql::span<const char> diffFile);

// Return a human-readable description of `diffFile` (the sizes of base, target
// and payload). Throw if the header cannot be parsed.
std::string describeBlobDiff(ql::span<const char> diffFile);

}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFF_H
