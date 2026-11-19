// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDHANDWRITTENBLOCKS_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDHANDWRITTENBLOCKS_H

#include <cstdint>
#include <vector>

#include "backports/span.h"

// Writers for two kinds of ZSTD blocks (RFC 8878, section 3.1.1.2) that are
// written by hand instead of by the ZSTD compressor, because the compressor
// gives no control over them. None of the blocks has the last-block flag set.
namespace ad_utility::zstd {

// Append the `literals` to `out` as "raw" (uncompressed) blocks of at most
// `ZSTD_BLOCKSIZE_MAX` bytes each. Append nothing if `literals` is empty. Raw
// blocks do not change the state of the decoder (in particular not its repeat
// offsets).
void appendRawBlocks(std::vector<char>& out, ql::span<const char> literals);

// Append a compressed block to `out` that decodes to a single match of length
// 3 with the given `offset` (which has to be at least 1, and at most the
// number of bytes that were decoded before the block), and contains no
// literals. The offset is stored explicitly (not as a "repeat offset"), so the
// block decodes the same no matter which blocks precede it, and afterwards the
// most recent repeat offset of the decoder is `offset` (the previous two repeat
// offsets move back by one position).
//
// The block uses the "RLE" mode for all three symbol tables (a single sequence
// has a single symbol of each kind), so it needs no entropy coding and does not
// depend on the tables of previous blocks. It is 10 bytes large for small
// offsets.
void appendSingleMatchBlock(std::vector<char>& out, uint32_t offset);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDHANDWRITTENBLOCKS_H
