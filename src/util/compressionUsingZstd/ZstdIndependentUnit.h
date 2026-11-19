// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDINDEPENDENTUNIT_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDINDEPENDENTUNIT_H

#include <cstddef>
#include <optional>
#include <vector>

#include "backports/span.h"

// An "independent unit" is a sequence of ZSTD blocks that encodes some data
// such that the blocks can be put into any ZSTD frame at any position where
// the decoder is in the state of a frame start, and also be decompressed on
// their own (see `decompressBlocks` in `ZstdFrameFormat.h`).
//
// A compressed block of a ZSTD frame may depend on the state of the decoder
// after the previous blocks: (1) the window of the previous output, (2) the
// entropy tables of the previous block, and (3) the three "repeat offsets". The
// blocks of a fresh ZSTD compressor only refer to data that the compressor has
// compressed itself (1), never reuse the entropy tables in their first block
// (2), and assume the repeat offsets of a frame start, namely `{1, 4, 8}` (3).
// A unit therefore always starts with the blocks of a fresh compressor.
//
// If a unit additionally ENDS IN THE START STATE, then any unit can follow it.
// This is achieved by ending the unit with three matches with the explicit
// offsets 8, 4, and 1, followed only by uncompressed literals: afterwards the
// repeat offsets are `{1, 4, 8}` again, no matter what the blocks before were.
// The three matches are written by hand (see `ZstdHandWrittenBlocks.h`), and
// everything before them is compressed by an ordinary ZSTD compressor, so only
// the stable API of ZSTD is used. This requires that the data contains such
// matches close to its end (three runs of repeated bytes, which are common in
// typical binary data, but rare in random or already compressed data).
//
// Data without such matches can still end in the start state if it is stored
// uncompressed in RAW BLOCKS, which are part of the standard ZSTD format and
// neither use nor change the repeat offsets or the entropy tables. This is
// done if it costs at most a little more than an ordinary compression, which
// is the case for short, empty, random, or already compressed data.
namespace ad_utility::zstd {

// A compressed unit: its blocks (without a frame header, and with the
// last-block flag cleared), and whether it ends in the start state (see
// above).
struct CompressedUnit {
  std::vector<char> blocks_;
  bool endsInStartState_ = false;
};

// Compress `data` as an independent unit with the ZSTD `compressionLevel`. If
// `endInStartState` is false, the blocks are those of an ordinary compression.
// If it is true, the unit ends in the start state, or `std::nullopt` is
// returned if this is not possible for the `data` without a significantly
// worse compression (see above).
std::optional<CompressedUnit> compressIndependentUnit(ql::span<const char> data,
                                                      int compressionLevel,
                                                      bool endInStartState);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDINDEPENDENTUNIT_H
