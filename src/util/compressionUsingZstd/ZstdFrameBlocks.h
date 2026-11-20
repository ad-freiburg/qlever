// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEBLOCKS_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEBLOCKS_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "util/compressionUsingZstd/ZstdExpected.h"

// Helpers for the BLOCKS of ZSTD frames: reading and writing block headers,
// finding the blocks of a frame, changing their last-block flag, and wrapping
// blocks (possibly taken from several other frames) in a new SINGLE SEGMENT
// FRAME. For the terminology (frame, block, window, single segment frame, ...)
// see the comment at the top of `ZstdFrameHeader.h`. None of the functions
// throws for invalid input; errors are reported via `Expected` (see
// `ZstdExpected.h`).
//
// The blocks of a frame are stored back to back right after the frame header.
// Each block consists of a 3-byte block header and the block content:
//
//   ┌──────────────┬───────────────┬──────────────┬───────────────┬─────┐
//   │ block header │ block content │ block header │ block content │ ... │
//   │ (3 bytes)    │               │ (3 bytes)    │               │     │
//   └──────────────┴───────────────┴──────────────┴───────────────┴─────┘
//
// The blocks end with the block whose header has the last-block flag set
// (followed by the checksum, if the frame has one). The blocks contain no
// information about where they start or end other than their headers, so a
// sequence of blocks can only be traversed from its beginning.
namespace ad_utility::zstd {

// The header of a block of a ZSTD frame. In the file, it is a 24-bit
// little-endian integer, whose bits (bit 0 is the least significant bit) are:
//
//   bit  0    : the last-block flag (`isLast_`).
//   bits 1-2  : the block type (`type_`).
//   bits 3-23 : the block size (`blockSize_`), see below for its meaning.
struct BlockHeader {
  // The type of a block, which determines how its content is stored.
  enum class Type : uint8_t {
    // The block content is the decompressed content itself, stored verbatim.
    Raw = 0,
    // The block content is a single byte, which the decompressed content
    // repeats `blockSize_` times ("run-length encoding").
    Rle = 1,
    // The block content is the ZSTD-compressed content (literals and
    // sequences, possibly with entropy tables).
    Compressed = 2,
    // Invalid, a valid frame never contains a block of this type.
    Reserved = 3
  };
  // The size of the header in bytes.
  static constexpr size_t size = 3;
  // The largest `blockSize_` that fits into the 21 bits of the header.
  static constexpr uint32_t maxBlockSize = (1u << 21) - 1;

  // True iff this is the last block of its frame.
  bool isLast_ = false;
  // The type of the block (see above).
  Type type_ = Type::Raw;
  // For `Raw` and `Compressed` blocks: the size of the block content in bytes
  // (without the header). For `Rle` blocks: the number of times that the single
  // byte of the block content is repeated, i.e. the size of the decompressed
  // content. It has 21 bits, so it must be at most `maxBlockSize`.
  uint32_t blockSize_ = 0;

  // Return the number of bytes of the block including its header.
  size_t totalSize() const {
    // An RLE block only stores a single byte.
    return size + (type_ == Type::Rle ? 1 : blockSize_);
  }

  // Read the header of the block at the start of `bytes`. Return an error if
  // `bytes` is too short for the header or for the complete block, or if the
  // block has the reserved type.
  static Expected<BlockHeader> read(ql::span<const char> bytes);

  // Append the header to `out`. Return an error (and append nothing) if the
  // `blockSize_` is larger than `maxBlockSize`, or the type is `Reserved`.
  Expected<void> append(std::vector<char>& out) const;
};

// The blocks of a single frame that was written by the ZSTD compressor,
// without the frame header and without the checksum.
struct FrameBlocks {
  // All the blocks of the frame (each with its header), back to back. This is
  // a view into the frame that was passed to `blocksOfFrame`.
  ql::span<const char> blocks_;
  // The offset of the header of the last block (relative to `blocks_`).
  size_t lastHeader_;
};

// Return the blocks of `frame`, or an error if `frame` is not exactly one valid
// ZSTD frame. If the frame has a checksum, then the checksum is not part of the
// blocks: it is the checksum of the content of the whole frame, so it cannot be
// used for the blocks when they are put into another frame (and the frames that
// are assembled from blocks never have a checksum, see `SingleSegmentFrame`).
Expected<FrameBlocks> blocksOfFrame(ql::span<const char> frame);

// If `blocks` consists of one or more complete blocks, of which only the last
// one may have the last-block flag set, then return the offset of the header of
// the last block, else an error.
Expected<size_t> lastBlockHeaderOffset(ql::span<const char> blocks);

// Set or clear the last-block flag of the block whose header starts at
// `blocks[headerOffset]`. This is needed to concatenate the blocks of several
// frames, where only the very last block may have the flag set. Return an error
// (and change nothing) if `blocks` is too short for a header at
// `headerOffset`.
Expected<void> setLastBlockFlag(ql::span<char> blocks, size_t headerOffset,
                                bool isLast);

// A SINGLE SEGMENT FRAME (see `ZstdFrameHeader.h`) that is assembled by hand:
// a frame header with the single segment flag and the content size, followed
// by blocks that may have been taken from (several) other frames, and no
// checksum:
//
//   ┌────────────────────────┬──────────────────────────────────────────────┐
//   │ frame header           │ blocks (only the last one has the last-block │
//   │ (`headerSize` bytes)   │ flag set)                                    │
//   └────────────────────────┴──────────────────────────────────────────────┘
//
// The window of a single segment frame is its whole content, so the blocks may
// come from compressors with different window sizes: every match of every
// block refers to a position inside the window. Frames assembled like this
// neither have a checksum (it would have to be recomputed from the whole
// content) nor a dictionary.
//
// NOTE: A streaming decompressor needs a buffer for the whole window, so it has
// to allow a window of the size of the content (see `ZSTD_d_windowLogMax`). The
// one-shot decompression (`ZSTD_decompress`) is not affected by the possibly
// large window size.
struct SingleSegmentFrame {
  // The size of the frame header that is written by `appendHeader`. The
  // content size is always stored with 8 bytes, so the size of the header does
  // not depend on the content size, and the header can be overwritten once the
  // final content size is known.
  static constexpr size_t headerSize = 13;

  // Append the header of a single segment frame with the given `contentSize`
  // to `out`. The blocks of the frame have to be appended after it.
  static void appendHeader(std::vector<char>& out, uint64_t contentSize);

  // Decompress the `blocks` (as returned by `blocksOfFrame`, possibly with the
  // last-block flag cleared), which have to decode to exactly `contentSize`
  // bytes, into `out`, which has to have room for `contentSize` bytes. The
  // blocks must not depend on blocks that precede them (see
  // `ZstdIndependentUnit.h`). They are copied after a new frame header (and
  // their last block gets the last-block flag), which is cheap compared to the
  // decompression itself. Return an error if the blocks are corrupted, if they
  // do not decode to exactly `contentSize` bytes, or if `out` is too small.
  static Expected<void> decompressBlocks(ql::span<const char> blocks,
                                         uint64_t contentSize,
                                         ql::span<char> out);
};

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEBLOCKS_H
