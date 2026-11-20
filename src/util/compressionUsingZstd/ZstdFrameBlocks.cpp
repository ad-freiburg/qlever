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

#include "util/Exception.h"
#include "util/compressionUsingZstd/ZstdFrameHeader.h"
#include "util/compressionUsingZstd/ZstdLittleEndian.h"

namespace ad_utility::zstd {

// _____________________________________________________________________________
std::optional<BlockHeader> BlockHeader::read(ql::span<const char> bytes) {
  using detail::byteAt;
  if (bytes.size() < size) {
    return std::nullopt;
  }
  // Assemble the 24-bit little-endian integer from its three bytes, and
  // extract its fields: bit 0 is the last-block flag, bits 1-2 are the type,
  // and bits 3-23 are the size of the block.
  uint32_t value =
      byteAt(bytes, 0) | byteAt(bytes, 1) << 8 | byteAt(bytes, 2) << 16;
  BlockHeader header{(value & 1) != 0, static_cast<Type>((value >> 1) & 3),
                     value >> 3};
  // Also check that the complete block (not only its header) is contained in
  // `bytes`, such that the caller can safely skip to the next block.
  if (header.type_ == Type::Reserved || header.totalSize() > bytes.size()) {
    return std::nullopt;
  }
  return header;
}

// _____________________________________________________________________________
void BlockHeader::append(std::vector<char>& out) const {
  // The size has to fit into the 21 bits of the size field.
  AD_CONTRACT_CHECK(blockSize_ < (1u << 21) && type_ != Type::Reserved);
  // Assemble the 24-bit integer (see `read` for the layout) and append its
  // three bytes, least significant byte first.
  uint32_t value = static_cast<uint32_t>(isLast_) |
                   static_cast<uint32_t>(type_) << 1 | blockSize_ << 3;
  out.push_back(static_cast<char>(value & 0xFF));
  out.push_back(static_cast<char>((value >> 8) & 0xFF));
  out.push_back(static_cast<char>(value >> 16));
}

// _____________________________________________________________________________
FrameBlocks blocksOfFrame(ql::span<const char> frame) {
  // The blocks start right after the frame header. A checksum would follow the
  // last block, which is not supported (callers compress without checksums).
  auto header = parseFrameHeader(frame);
  AD_CORRECTNESS_CHECK(header.has_value() && !header->hasChecksum_);
  // Let ZSTD check that `frame` is exactly one complete frame (no trailing
  // bytes, nothing missing).
  AD_CORRECTNESS_CHECK(
      ZSTD_findFrameCompressedSize(frame.data(), frame.size()) == frame.size());
  auto blocks = frame.subspan(header->headerSize_);
  // Without a checksum, the blocks extend to the end of the frame, and the last
  // of them has to have the last-block flag set.
  auto lastHeader = lastBlockHeaderOffset(blocks);
  AD_CORRECTNESS_CHECK(lastHeader.has_value());
  AD_CORRECTNESS_CHECK(
      BlockHeader::read(blocks.subspan(lastHeader.value()))->isLast_);
  return {blocks, lastHeader.value()};
}

// _____________________________________________________________________________
std::optional<size_t> lastBlockHeaderOffset(ql::span<const char> blocks) {
  // Walk over the blocks from header to header, the only way to find the block
  // boundaries (see the comment at the top of `ZstdFrameBlocks.h`).
  size_t position = 0;
  std::optional<size_t> lastHeader;
  while (position < blocks.size()) {
    // `read` fails if the header or the block is truncated.
    auto block = BlockHeader::read(blocks.subspan(position));
    if (!block.has_value()) {
      return std::nullopt;
    }
    lastHeader = position;
    position += block->totalSize();
    // A decoder stops after a block with the last-block flag, so if there are
    // more bytes after such a block, the blocks are not valid.
    if (block->isLast_ && position != blocks.size()) {
      return std::nullopt;
    }
  }
  // If `blocks` is empty, `lastHeader` is still `std::nullopt`.
  return lastHeader;
}

// _____________________________________________________________________________
void setLastBlockFlag(ql::span<char> blocks, size_t headerOffset, bool isLast) {
  AD_CONTRACT_CHECK(headerOffset + BlockHeader::size <= blocks.size());
  // The last-block flag is bit 0 of the 24-bit little-endian header, so it is
  // the least significant bit of the first byte of the header.
  auto& byte = blocks[headerOffset];
  byte = static_cast<char>(isLast ? (byte | 1) : (byte & ~1));
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
  auto serializer = detail::appendingSerializer(out);
  serializer << static_cast<uint32_t>(ZSTD_MAGICNUMBER);
  serializer << descriptor;
  serializer << contentSize;
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
bool SingleSegmentFrame::decompressBlocks(ql::span<const char> blocks,
                                          uint64_t contentSize,
                                          ql::span<char> out) {
  // Validate the block structure first, so that `ZSTD_decompress` never sees a
  // frame whose blocks are truncated.
  auto lastHeader = lastBlockHeaderOffset(blocks);
  if (!lastHeader.has_value() || out.size() < contentSize) {
    return false;
  }
  // The stable API of ZSTD can only decompress complete frames, so wrap the
  // blocks in a new single segment frame, and make sure that their last block
  // has the last-block flag (it may have been cleared to concatenate the
  // blocks with other blocks).
  std::vector<char> frame;
  frame.reserve(headerSize + blocks.size());
  appendHeader(frame, contentSize);
  frame.insert(frame.end(), blocks.begin(), blocks.end());
  setLastBlockFlag(ql::span<char>{frame}.subspan(headerSize),
                   lastHeader.value(), true);
  // The content size in the header lets ZSTD detect blocks that decode to too
  // many or too few bytes.
  size_t size =
      ZSTD_decompress(out.data(), contentSize, frame.data(), frame.size());
  return !ZSTD_isError(size) && size == contentSize;
}

}  // namespace ad_utility::zstd
