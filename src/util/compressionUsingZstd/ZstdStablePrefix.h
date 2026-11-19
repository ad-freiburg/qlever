// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDSTABLEPREFIX_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDSTABLEPREFIX_H

#include <optional>
#include <vector>

#include "backports/span.h"

// A "stable prefix" is the beginning of some data, compressed as ZSTD blocks
// such that the blocks can be copied verbatim to the beginning of another
// frame, and the remainder of the other frame can be compressed independently
// by an ordinary ZSTD compressor (see `ZstdFrameFormat.h` for how the blocks of
// several frames are combined).
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
// NOTE: Encoding a stable prefix requires the experimental sequence API of
// ZSTD, which is only used with ZSTD 1.5.5 or newer (see
// `stablePrefixEncodingIsSupported`). Copying an existing stable prefix works
// with every version.
namespace ad_utility::zstd {

// Return true iff `encodeStablePrefix` can succeed with the version of ZSTD
// that QLever was compiled against.
bool stablePrefixEncodingIsSupported();

// Encode `prefix` as a stable prefix (see above) with the given ZSTD
// `compressionLevel`, and return its blocks (without a frame header, and with
// the last-block flag cleared). Return `std::nullopt` if this is not possible,
// because no suitable matches for the end of the prefix are found (or only far
// from its end), because the sequence API is not supported, or because the
// result does not pass `isStablePrefix`. The result is deterministic for the
// same version of ZSTD.
//
// NOTE: The levels of ZSTD that use the `btopt` or `btultra` strategies are not
// supported by the sequence API of all versions of ZSTD.
std::optional<std::vector<char>> encodeStablePrefix(ql::span<const char> prefix,
                                                    int compressionLevel);

// Check that the `prefixBlocks` decode to `prefix` and leave the decoder in
// the state of a frame start, by appending the blocks of fresh compressions of
// probes that use all three repeat offsets, and decompressing the result.
bool isStablePrefix(ql::span<const char> prefix,
                    ql::span<const char> prefixBlocks);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDSTABLEPREFIX_H
