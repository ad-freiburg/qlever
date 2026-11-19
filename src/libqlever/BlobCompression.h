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
// A compressed blob is a single ZSTD frame that holds the complete
// (decompressed) blob, optionally followed by a ZSTD "skippable frame" that
// holds a small `BlobIndex` (see below). Every ZSTD decompressor ignores the
// skippable frame, so every compressed blob can be read by
// `NamedCachedQueryBlobManager::tryToDecompressBlob` (also by the versions of
// QLever that preceded this file).
//
// THE STABLE PREFIX: The first part of a blob (the header, the metadata, and
// the vocabulary) typically does not change between two blobs that are written
// incrementally (see `BlobSerializationConfig::IncrementalBase`). It is also
// the part that takes most of the time of the compression (the vocabulary is
// large and already compressed in memory). To avoid compressing it again, the
// "stable prefix" of a blob is compressed such that the compressed blocks of
// the prefix can be copied verbatim to another frame, and the remainder of the
// other frame can be compressed independently by an ordinary ZSTD compressor.
//
// This works as follows. A compressed block of a ZSTD frame may depend on the
// state of the decoder after the previous blocks: (1) the window of the
// previous output, (2) the entropy tables of the previous block, and (3) the
// three "repeat offsets". A fresh compressor only refers to data that it has
// compressed itself (1), and never reuses the entropy tables in its first
// block (2), but it assumes that the repeat offsets are the ones at the start
// of a frame, namely `{1, 4, 8}` (3). The stable prefix is therefore encoded
// such that the repeat offsets at its end are `{1, 4, 8}`: its last block ends
// with three matches with the offsets 8, 4, and 1 (in this order), followed
// only by literals. Then the decoder is in the same state as at the start of a
// frame (as far as the following blocks are concerned), and the blocks of a
// fresh compression of the remainder can be appended.
//
// NOTE: Encoding the stable prefix requires the experimental sequence API of
// ZSTD, which is only used with ZSTD 1.5.5 or newer. With older versions (and
// if the stable prefix cannot be encoded, see `compressBlob`), the blob is
// written without a stable prefix. Copying an existing stable prefix works
// with every version.
namespace qlever::blobCompression {

// The default ZSTD compression level of a blob. The low levels are much faster
// than the default level 3 of ZSTD and the result is only slightly larger.
constexpr int defaultCompressionLevel = 1;

// The ZSTD compression level of the stable prefix. It is higher than the
// default level, because the stable prefix is typically compressed only once
// and then copied (see above). Note that the levels of ZSTD that use the
// `btopt` or `btultra` strategies are not supported by the sequence API of all
// versions of ZSTD.
constexpr int stablePrefixCompressionLevel = 9;

// The smallest stable prefix (in bytes) that is encoded as such. Smaller
// prefixes are not worth the effort, the blob is then written without a
// stable prefix.
constexpr uint64_t minStablePrefixSize = 4096;

// How a blob is compressed.
struct BlobEncoding {
  // The ZSTD compression level of everything except the stable prefix.
  int compressionLevel_ = defaultCompressionLevel;
  // The number of bytes at the start of the decompressed blob that form the
  // stable prefix (zero if there is none).
  uint64_t stablePrefixSize_ = 0;
};

// The information that is stored in the skippable frame at the end of a
// compressed blob.
struct BlobIndex {
  // The encoding of the blob. Its `stablePrefixSize_` is the one that was
  // actually written (which may be zero, see `compressBlob`).
  BlobEncoding encoding_;
  // The offset in the compressed blob at which the blocks of the stable prefix
  // end. The blocks of the stable prefix start right after the frame header.
  uint64_t stablePrefixBlocksEnd_ = 0;
};

// A previously compressed blob, whose stable prefix may be copied to a new
// blob (see `compressBlob`).
struct ReusableBlob {
  // The compressed blob.
  ql::span<const char> compressed_;
  // The decompressed blob (the result of decompressing `compressed_`).
  ql::span<const char> decompressed_;
};

// Compress `uncompressedBlob` with the given `encoding`. If its
// `stablePrefixSize_` is not zero, then the first `stablePrefixSize_` bytes are
// encoded as the stable prefix (see above). If a `reusableBlob` is given, whose
// stable prefix has the same size and the same content, then its compressed
// stable prefix is copied instead of being encoded again. The stable prefix is
// omitted if it is smaller than `minStablePrefixSize`, if it is not smaller
// than the whole blob, or if it cannot be encoded (see the NOTE above). The
// result is deterministic for the same version of ZSTD (in particular, copying
// the stable prefix yields the same result as encoding it again).
std::vector<char> compressBlob(
    ql::span<const char> uncompressedBlob, const BlobEncoding& encoding = {},
    const std::optional<ReusableBlob>& reusableBlob = std::nullopt);

// Read the `BlobIndex` from the skippable frame at the end of
// `compressedBlob`. Return `std::nullopt` if there is none (for example
// because the blob was written by a previous version of QLever) or if it is
// inconsistent with the compressed blob. Do not throw for any input.
std::optional<BlobIndex> readBlobIndex(ql::span<const char> compressedBlob);

}  // namespace qlever::blobCompression

#endif  // QLEVER_SRC_LIBQLEVER_BLOBCOMPRESSION_H
