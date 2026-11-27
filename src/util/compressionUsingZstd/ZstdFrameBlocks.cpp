// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/ZstdFrameBlocks.h"

#include <zstd.h>

#include <string>

#include "util/Exception.h"
#include "util/compressionUsingZstd/ZstdFrameHeader.h"
#include "util/compressionUsingZstd/ZstdLittleEndian.h"

namespace ad_utility::zstd {

namespace {
// The size of the checksum at the end of a frame with the checksum flag.
constexpr size_t checksumSize = 4;
}  // namespace

// _____________________________________________________________________________
Expected<BlockHeader> BlockHeader::read(ql::span<const char> bytes) {
  if (bytes.size() < size) {
    return ZstdError{"A ZSTD block header is truncated"};
  }
  // The header is a 24-bit little-endian integer. Extract its fields: bit 0 is
  // the last-block flag, bits 1-2 are the type, and bits 3-23 are the size of
  // the block.
  const auto value = static_cast<uint32_t>(detail::readLittleEndian<3>(bytes));
  BlockHeader header{(value & 1) != 0, static_cast<Type>((value >> 1) & 3),
                     value >> 3};
  if (header.type_ == Type::Reserved) {
    return ZstdError{"A ZSTD block has the reserved block type"};
  }
  // Also check that the complete block (not only its header) is contained in
  // `bytes`, such that the caller can safely skip to the next block.
  if (header.totalSize() > bytes.size()) {
    return ZstdError{"A ZSTD block is truncated: it needs " +
                     std::to_string(header.totalSize()) + " bytes, but only " +
                     std::to_string(bytes.size()) + " bytes are given"};
  }
  return header;
}

// _____________________________________________________________________________
Expected<void> BlockHeader::append(std::vector<char>& out) const {
  if (blockSize_ > maxBlockSize) {
    return ZstdError{"The size " + std::to_string(blockSize_) +
                     " of a ZSTD block does not fit into the 21 bits of its "
                     "header"};
  }
  if (type_ == Type::Reserved) {
    return ZstdError{"A ZSTD block cannot have the reserved block type"};
  }
  // Assemble the 24-bit integer (see `read` for the layout).
  const uint32_t value = static_cast<uint32_t>(isLast_) |
                         static_cast<uint32_t>(type_) << 1 | blockSize_ << 3;
  detail::appendLittleEndian<size>(out, value);
  return {};
}

// _____________________________________________________________________________
Expected<FrameBlocks> blocksOfFrame(ql::span<const char> frame) {
  // The blocks start right after the frame header.
  auto header = parseFrameHeader(frame);
  if (!header.hasValue()) {
    return std::move(header).error();
  }
  // Let ZSTD check that `frame` is exactly one complete frame (no trailing
  // bytes, nothing missing).
  const size_t frameSize =
      ZSTD_findFrameCompressedSize(frame.data(), frame.size());
  if (ZSTD_isError(frameSize)) {
    return ZstdError{std::string{"Invalid ZSTD frame: "} +
                     ZSTD_getErrorName(frameSize)};
  }
  if (frameSize != frame.size()) {
    return ZstdError{"The data is not exactly one ZSTD frame: the frame has " +
                     std::to_string(frameSize) + " bytes, but the data has " +
                     std::to_string(frame.size()) + " bytes"};
  }
  // The blocks extend to the end of the frame, except for the checksum (if
  // any). ZSTD has checked that the frame is complete, so the subtraction
  // cannot underflow.
  const size_t blocksSize = frame.size() - header->headerSize_ -
                            (header->hasChecksum_ ? checksumSize : 0);
  auto blocks = frame.subspan(header->headerSize_, blocksSize);
  // The last of the blocks has to have the last-block flag set.
  auto lastHeader = lastBlockHeaderOffset(blocks);
  if (!lastHeader.hasValue()) {
    return std::move(lastHeader).error();
  }
  if (!BlockHeader::read(blocks.subspan(lastHeader.value()))->isLast_) {
    return ZstdError{"The last block of a ZSTD frame has no last-block flag"};
  }
  return FrameBlocks{blocks, lastHeader.value()};
}

// _____________________________________________________________________________
Expected<size_t> lastBlockHeaderOffset(ql::span<const char> blocks) {
  if (blocks.empty()) {
    return ZstdError{"A sequence of ZSTD blocks must not be empty"};
  }
  // Walk over the blocks from header to header, the only way to find the block
  // boundaries (see the comment at the top of `ZstdFrameBlocks.h`).
  size_t position = 0;
  size_t lastHeader = 0;
  while (position < blocks.size()) {
    // `read` fails if the header or the block is truncated.
    auto block = BlockHeader::read(blocks.subspan(position));
    if (!block.hasValue()) {
      return std::move(block).error();
    }
    lastHeader = position;
    position += block->totalSize();
    // A decoder stops after a block with the last-block flag, so if there are
    // more bytes after such a block, the blocks are not valid.
    if (block->isLast_ && position != blocks.size()) {
      return ZstdError{
          "A ZSTD block with the last-block flag is followed by more data"};
    }
  }
  return lastHeader;
}

// _____________________________________________________________________________
Expected<void> setLastBlockFlag(ql::span<char> blocks, size_t headerOffset,
                                bool isLast) {
  if (headerOffset > blocks.size() ||
      blocks.size() - headerOffset < BlockHeader::size) {
    return ZstdError{"There is no ZSTD block header at offset " +
                     std::to_string(headerOffset)};
  }
  // The last-block flag is bit 0 of the 24-bit little-endian header, so it is
  // the least significant bit of the first byte of the header.
  auto& byte = blocks[headerOffset];
  byte = static_cast<char>(isLast ? (byte | 1) : (byte & ~1));
  return {};
}

// _____________________________________________________________________________
void SingleSegmentFrame::appendHeader(std::vector<char>& out,
                                      uint64_t contentSize) {
  const size_t begin = out.size();
  // The header consists of the magic number (4 bytes), the frame header
  // descriptor (1 byte), and the content size (8 bytes). There is no window
  // descriptor (because of the single segment flag), and no dictionary ID.
  //
  // The frame header descriptor (see `parseFrameHeader` for its layout): a
  // content size flag of 3, which means 8 bytes for the content size
  // (`3 << 6`), and the single segment flag (`1 << 5`); no checksum, no
  // dictionary.
  constexpr uint8_t descriptor = (3 << 6) | (1 << 5);
  detail::appendLittleEndian<4>(out, ZSTD_MAGICNUMBER);
  detail::appendLittleEndian<1>(out, descriptor);
  detail::appendLittleEndian<8>(out, contentSize);
  AD_CORRECTNESS_CHECK(out.size() - begin == headerSize);
  // Let ZSTD and `parseFrameHeader` check that they parse the header as
  // intended.
  auto header = ql::span<const char>{out}.subspan(begin);
  AD_CORRECTNESS_CHECK(ZSTD_getFrameContentSize(header.data(), header.size()) ==
                       contentSize);
  AD_CORRECTNESS_CHECK(parseFrameHeader(header).value().headerSize_ ==
                       headerSize);
}

// _____________________________________________________________________________
Expected<void> SingleSegmentFrame::decompressBlocks(ql::span<const char> blocks,
                                                    uint64_t contentSize,
                                                    ql::span<char> out) {
  // Validate the block structure first, so that `ZSTD_decompress` never sees a
  // frame whose blocks are truncated.
  auto lastHeader = lastBlockHeaderOffset(blocks);
  if (!lastHeader.hasValue()) {
    return std::move(lastHeader).error();
  }
  if (out.size() < contentSize) {
    return ZstdError{"The output buffer of " + std::to_string(out.size()) +
                     " bytes is too small for " + std::to_string(contentSize) +
                     " bytes of decompressed content"};
  }
  // The stable API of ZSTD can only decompress complete frames, so wrap the
  // blocks in a new single segment frame, and make sure that their last block
  // has the last-block flag (it may have been cleared to concatenate the
  // blocks with other blocks).
  std::vector<char> frame;
  frame.reserve(headerSize + blocks.size());
  appendHeader(frame, contentSize);
  frame.insert(frame.end(), blocks.begin(), blocks.end());
  AD_CORRECTNESS_CHECK(
      setLastBlockFlag(ql::span<char>{frame}.subspan(headerSize),
                       lastHeader.value(), true)
          .hasValue());
  // The content size in the header lets ZSTD detect blocks that decode to too
  // many or too few bytes.
  const size_t size =
      ZSTD_decompress(out.data(), contentSize, frame.data(), frame.size());
  if (ZSTD_isError(size)) {
    return ZstdError{std::string{"Decompressing ZSTD blocks failed: "} +
                     ZSTD_getErrorName(size)};
  }
  if (size != contentSize) {
    return ZstdError{"ZSTD blocks decoded to " + std::to_string(size) +
                     " bytes instead of the expected " +
                     std::to_string(contentSize) + " bytes"};
  }
  return {};
}

}  // namespace ad_utility::zstd
