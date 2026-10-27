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
#include <variant>
#include <vector>

#include "backports/span.h"
#include "util/MemorySize/MemorySize.h"
#include "util/UninitializedAllocator.h"

namespace qlever::blobDiff {

// A blob diff file turns a (compressed) blob `base` into a (compressed) blob
// `target`. It is a generic binary delta: the decompressed `target` is
// compressed with ZSTD using the decompressed `base` as a reference (the
// "prefix dictionary" of ZSTD, the same mechanism as `zstd --patch-from`), so
// every piece of the `target` that also occurs somewhere in the `base` costs
// only a few bytes.
//
// The diff is small if the `target` was written incrementally against the
// `base` (see `BlobSerializationConfig::IncrementalBase`), because then the
// `Id`s of all words and the segments of the secondary vocabulary and of the
// geo indices are stable, and the tables of the named results only differ by
// the inserted and deleted rows.
//
// The diff knows nothing about the layout of a blob, which makes it robust
// against changes of the blob format.
//
// FILE FORMAT (all integers in the native byte order, which is little endian
// on all platforms that QLever currently supports):
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
// verify the result of applying the diff (the ZSTD frame itself only stores a
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

// The reasons why reading or applying a diff file fails (see
// `BlobDiffError`).
enum class BlobDiffErrorType {
  // The diff file does not start with the expected magic bytes. This also
  // covers the case that it is too short to hold a complete header.
  invalidMagicBytes,
  // The magic bytes are correct, but the format version is not the supported
  // one.
  invalidVersion,
  // The size of the payload that is stored in the header does not match the
  // number of bytes that follow the header (the diff file is truncated or has
  // trailing bytes).
  invalidPayloadSize,
  // The size of the target that is stored in the header is not plausible: it
  // differs from the size that is stored in the ZSTD frame of the payload, or
  // it exceeds the `maxTargetSize` that was given by the caller. This is
  // detected before the target is allocated, so a corrupted size cannot lead to
  // a huge allocation.
  invalidTargetSize,
  // The compressed base blob cannot be decompressed (see
  // `applyBlobDiffToCompressedBase`).
  baseNotDecompressible,
  // The diff does not belong to the given base (the size or the checksum of
  // the base differs from the one that is stored in the header).
  wrongBase,
  // The payload cannot be decompressed, or the result does not have the
  // checksum of the target that is stored in the header.
  corruptedPayload
};

// The error that is reported by the functions below: the reason for the
// failure, and a descriptive message (which includes the details, for example
// the underlying ZSTD error).
struct BlobDiffError {
  BlobDiffErrorType type_;
  std::string message_;
};

// The result of `readBlobDiffHeader`.
using BlobDiffHeaderOrError = std::variant<BlobDiffHeader, BlobDiffError>;

// A decompressed blob. Its bytes are not zero-initialized on allocation,
// because they are overwritten anyway.
using DecompressedBlob =
    std::vector<char, ad_utility::default_init_allocator<char>>;

// The result of `applyBlobDiffToDecompressedBase`: the resulting decompressed
// blob on success, and the error otherwise.
using DecompressedBlobOrError = std::variant<DecompressedBlob, BlobDiffError>;

// The result of `applyBlobDiffToCompressedBase`: the resulting compressed blob
// on success, and the error otherwise.
using BlobOrError = std::variant<std::vector<char>, BlobDiffError>;

// Parse and validate the header of `diffFile` (see `BlobDiffErrorType` for the
// checks). A diff whose decompressed target is larger than `maxTargetSize` is
// rejected. On success, the `targetSize_` of the header can be allocated
// without exceeding that bound. Do not throw for any input.
BlobDiffHeaderOrError readBlobDiffHeader(ql::span<const char> diffFile,
                                         ad_utility::MemorySize maxTargetSize);

// Apply `diffFile` to the decompressed blob `base` and return the decompressed
// target blob. Report an error if the header of `diffFile` is invalid (see
// `readBlobDiffHeader`), if the diff does not belong to the `base` (size or
// checksum differ), or if the payload is corrupted (it cannot be decompressed,
// or the result does not have the checksum that is stored in the diff). For the
// meaning of `maxTargetSize` see `readBlobDiffHeader`. Do not throw for any
// input, except if the target cannot be allocated.
DecompressedBlobOrError applyBlobDiffToDecompressedBase(
    ql::span<const char> base, ql::span<const char> diffFile,
    ad_utility::MemorySize maxTargetSize);

// Apply `diffFile` to the compressed blob `compressedBase` and return the
// compressed target blob (a standard blob that can be loaded by
// `Qlever::deserializeVocabAndNamedCacheFromCompressedBlob`). Report an error
// if the base cannot be decompressed, or in the cases of
// `applyBlobDiffToDecompressedBase`. The `maxTargetSize` limits the size of the
// decompressed target (see `readBlobDiffHeader`). Do not throw for any input,
// except if the decompressed base or the target cannot be allocated.
BlobOrError applyBlobDiffToCompressedBase(ql::span<const char> compressedBase,
                                          ql::span<const char> diffFile,
                                          ad_utility::MemorySize maxTargetSize);

// Return a human-readable description of `diffFile` (the sizes of base, target
// and payload). Throw if the header cannot be parsed.
std::string describeBlobDiff(ql::span<const char> diffFile);

}  // namespace qlever::blobDiff

#endif  // QLEVER_SRC_LIBQLEVER_BLOBDIFF_BLOBDIFF_H
