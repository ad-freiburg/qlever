// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEFORMAT_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEFORMAT_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"

// Helpers for the ZSTD frame format (RFC 8878) that are needed to take frames
// apart and to assemble new frames from the blocks of other frames (see
// `SegmentedZstdFrame.h`).
//
// NOTE: Everything here only uses the stable API of ZSTD (no
// `ZSTD_STATIC_LINKING_ONLY`) that exists in all supported versions (1.4.0 and
// newer). The stable API has no functions for the headers of frames and
// blocks, nor for skippable frames, which is why these are implemented here.
// All integers of the frame format are little endian, which is the native byte
// order on all platforms that QLever currently supports.
namespace ad_utility::zstd {

// _____________________________________________________________________________
// Frame headers.

// The parts of the header of a (regular, not skippable) ZSTD frame that are
// needed here.
struct FrameHeader {
  // The size of the frame header in bytes; the first block starts right after
  // it.
  size_t headerSize_ = 0;
  // True iff the frame ends with a 4-byte checksum (after the last block).
  bool hasChecksum_ = false;
};

// Parse the header of the regular ZSTD frame at the start of `bytes`. Return
// `std::nullopt` if `bytes` does not start with a complete and valid frame
// header. Do not throw for any input.
std::optional<FrameHeader> parseFrameHeader(ql::span<const char> bytes);

// The size of the frame header that is written by
// `appendSingleSegmentFrameHeader`.
constexpr size_t singleSegmentFrameHeaderSize = 13;

// Append the header of a frame with the given `contentSize` to `out`. The frame
// is a "single segment" frame (its window is the whole content), such that
// blocks that were written by different compressors (with different windows)
// can follow it. It has neither a checksum nor a dictionary.
//
// NOTE: A streaming decompressor needs a buffer for the whole window, so it
// has to allow a window of the size of the content (see
// `ZSTD_d_windowLogMax`). The one-shot decompression is not affected.
void appendSingleSegmentFrameHeader(std::vector<char>& out,
                                    uint64_t contentSize);

// _____________________________________________________________________________
// Blocks.

// The header of a block of a ZSTD frame.
struct BlockHeader {
  enum class Type : uint8_t { Raw = 0, Rle = 1, Compressed = 2, Reserved = 3 };
  // The size of the header in bytes.
  static constexpr size_t size = 3;

  bool isLast_ = false;
  Type type_ = Type::Raw;
  uint32_t blockSize_ = 0;

  // Return the number of bytes of the block including its header.
  size_t totalSize() const {
    // An RLE block only stores a single byte.
    return size + (type_ == Type::Rle ? 1 : blockSize_);
  }

  // Read the header of the block at the start of `bytes`. Return
  // `std::nullopt` if `bytes` is too short for the header or for the complete
  // block, or if the block has the reserved type.
  static std::optional<BlockHeader> read(ql::span<const char> bytes);

  // Append the header to `out`.
  void append(std::vector<char>& out) const;
};

// The blocks of a single frame that was written by the ZSTD compressor,
// without the frame header and without a checksum.
struct FrameBlocks {
  ql::span<const char> blocks_;
  // The offset of the header of the last block (relative to `blocks_`).
  size_t lastHeader_;
};

// Return the blocks of `frame`, which has to be exactly one valid ZSTD frame
// without a checksum. Throw if it is not.
FrameBlocks blocksOfFrame(ql::span<const char> frame);

// If `blocks` consists of one or more complete blocks, of which only the last
// one may have the last-block flag set, then return the offset of the header of
// the last block, else `std::nullopt`. Do not throw for any input.
std::optional<size_t> lastBlockHeaderOffset(ql::span<const char> blocks);

// Set or clear the last-block flag of the block whose header starts at
// `blocks[headerOffset]`.
void setLastBlockFlag(ql::span<char> blocks, size_t headerOffset, bool isLast);

// Decompress the `blocks` (as returned by `blocksOfFrame`, possibly with the
// last-block flag cleared), which have to decode to exactly `contentSize`
// bytes, into `out`, which has to have room for `contentSize` bytes. The blocks
// must not depend on blocks that precede them (see `ZstdIndependentUnit.h`).
// They are copied after a new frame header, which is cheap compared to the
// decompression itself. Return false if the blocks are corrupted. Do not throw
// for any input.
bool decompressBlocks(ql::span<const char> blocks, uint64_t contentSize,
                      ql::span<char> out);

// _____________________________________________________________________________
// Skippable frames, which every ZSTD decompressor ignores.

// The size of the header of a skippable frame (its magic number and the size
// of its content).
constexpr size_t skippableFrameHeaderSize = 8;

// Append a skippable frame with the `magicVariant` (in `[0, 16)`) and the
// `content` to `out`. The frame also stores its own size in its last 4 bytes,
// such that it can be found from the end of the data (see
// `readTrailingSkippableFrame`).
void appendTrailingSkippableFrame(std::vector<char>& out, uint32_t magicVariant,
                                  ql::span<const char> content);

// If `bytes` ends with a skippable frame that was written by
// `appendTrailingSkippableFrame` with the `magicVariant`, then return its
// `content`, else `std::nullopt`. Do not throw for any input.
std::optional<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, uint32_t magicVariant);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEFORMAT_H
