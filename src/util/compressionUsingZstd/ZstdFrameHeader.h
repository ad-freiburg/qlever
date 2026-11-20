// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEHEADER_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEHEADER_H

#include <cstddef>
#include <optional>

#include "backports/span.h"

// THE ZSTD FRAME FORMAT (RFC 8878), AS FAR AS IT IS NEEDED IN QLEVER.
//
// The helpers in `ZstdFrameHeader.h`, `ZstdFrameBlocks.h`, and
// `ZstdSkippableFrame.h` take ZSTD frames apart and assemble new frames from
// the blocks of other frames (see `SegmentedZstdFrame.h`). This comment
// introduces the terminology that all of them use.
//
// FRAME: The unit of data that `ZSTD_compress` produces. A compressed file or
// buffer is a sequence of frames, and decompressing it yields the
// concatenation of the contents of all its (non-skippable) frames. A frame
// consists of a frame header, one or more blocks, and an optional checksum:
//
//   ┌──────────────┬─────────┬─────────┬─────┬──────────────┬──────────────┐
//   │ frame header │ block 0 │ block 1 │ ... │ block n      │ checksum     │
//   │ (5-18 bytes) │         │         │     │ (last block) │ (0/4 bytes)  │
//   └──────────────┴─────────┴─────────┴─────┴──────────────┴──────────────┘
//
// FRAME HEADER: The magic number `ZSTD_MAGICNUMBER` (4 bytes), followed by a
// one-byte frame header descriptor, which determines which of the following
// optional fields are present and how large they are: the window descriptor
// (1 byte), the dictionary ID (0-4 bytes), and the content size (0-8 bytes).
// The descriptor also says whether the frame ends with a checksum (the lower 4
// bytes of the XXH64 hash of the content). See `parseFrameHeader` for the
// exact bit layout.
//
// WINDOW: While decoding, the decompressor keeps the last `windowSize` bytes of
// the already decoded content of the frame, the window. A match (a "copy
// `length` bytes from `offset` bytes back" instruction) can only refer to
// bytes inside the window. A streaming decompressor has to allocate a buffer
// of the window size, which is why the window size is stored in the header
// (via the window descriptor), and why decompressors limit the window size
// that they accept (see `ZSTD_d_windowLogMax`).
//
// SINGLE SEGMENT FRAME: A frame whose header has the single segment flag set.
// Its window is the whole content of the frame, so the window descriptor is
// omitted and the content size is stored instead. Matches may refer back to
// any earlier byte of the content. In QLever, single segment frames are used
// to assemble a frame from blocks that were written by different compressors
// (with different window sizes), see `SingleSegmentFrame` in
// `ZstdFrameBlocks.h`.
//
// WARNING: The "segment" of a ZSTD single segment frame is unrelated to the
// "segments" of a `SegmentedFrame` (see `SegmentedZstdFrame.h`), which are
// parts of the content of a frame that are chosen by its writer. A segmented
// frame happens to be a single segment frame in the ZSTD sense, but it
// typically consists of many segments in the sense of `SegmentedZstdFrame.h`.
//
// BLOCK: The content of a frame is split into blocks of at most 128 KiB of
// decompressed content each. A block consists of a 3-byte block header and the
// block content. The block header stores whether this is the last block of the
// frame (the last-block flag), the type of the block, and its size. There are
// three types of blocks: a RAW block stores its content uncompressed, an RLE
// block stores a single byte that is repeated `blockSize` times, and a
// COMPRESSED block stores literals and sequences (matches). A compressed block
// may depend on the preceding blocks of the same frame, not only via matches
// into the window, but also via the entropy tables and the most recent match
// offsets (the repeat offsets), which the decoder carries over from block to
// block. See `ZstdFrameBlocks.h` for the details of the block header and
// `ZstdIndependentUnit.h` for how blocks can be made independent of their
// predecessors.
//
// SKIPPABLE FRAME: A frame with one of the 16 magic numbers
// `ZSTD_MAGIC_SKIPPABLE_START + i` (`i` in `[0, 16)`, the "magic variant"),
// followed by the size of its content (4 bytes) and the content itself. Every
// ZSTD decompressor skips such frames, so they can be used to store metadata
// next to the compressed data. See `ZstdSkippableFrame.h`.
//
// NOTE: Everything here only uses the stable API of ZSTD. The stable API has no
// functions for the headers of frames and blocks, nor for skippable frames,
// which is why these are implemented here. All integers of the frame format
// are little endian, which is the native byte order on all platforms that
// QLever currently supports.
namespace ad_utility::zstd {

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

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDFRAMEHEADER_H
