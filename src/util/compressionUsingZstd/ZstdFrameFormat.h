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

// Helpers for the parts of the ZSTD frame format (RFC 8878) that the public
// API of ZSTD does not expose, but which are needed to combine the blocks of
// several compressed frames into a single frame (see `ZstdStablePrefix.h`).
//
// NOTE: ZSTD has no public type or parser for the header of a block (its
// `blockProperties_t` and `ZSTD_getcBlockSize` are internal to the library),
// and no function that writes only the header of a frame. The functions for
// skippable frames (`ZSTD_writeSkippableFrame`, `ZSTD_readSkippableFrame`) only
// exist since ZSTD 1.5.0 and 1.5.2 respectively, which is newer than the ZSTD
// of some of the supported platforms (for example Ubuntu 22.04 ships 1.4.8).
namespace ad_utility::zstd {

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

// Return the bytes of the frame that starts at `frame[0]` from the end of its
// header up to `end` (exclusive), if they consist of one or more complete
// blocks, none of which is the last block of the frame, else `std::nullopt`.
// Do not throw for any input.
std::optional<ql::span<const char>> leadingNonLastBlocks(
    ql::span<const char> frame, size_t end);

// Clear the last-block flag of the block whose header starts at
// `blocks[headerOffset]`, such that further blocks can follow it.
void clearLastBlockFlag(ql::span<char> blocks, size_t headerOffset);

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

// The size of the header of a skippable frame (its magic number and the size
// of its content).
constexpr size_t skippableFrameHeaderSize = 8;

// Append a skippable frame with the `magicVariant` (in `[0, 16)`) and the
// `content` to `out`. Every ZSTD decompressor ignores it.
void appendSkippableFrame(std::vector<char>& out, uint32_t magicVariant,
                          ql::span<const char> content);

// If `bytes` ends with a skippable frame with the `magicVariant` and with
// exactly `contentSize` bytes of content, then return that content, else
// `std::nullopt`. Do not throw for any input.
std::optional<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, uint32_t magicVariant, size_t contentSize);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEFORMAT_H
