// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// `ZSTD_frameHeaderSize` and `ZSTD_getFrameHeader` are part of the
// experimental API of ZSTD, which has to be enabled before `zstd.h` is included
// for the first time.
#define ZSTD_STATIC_LINKING_ONLY
#include "util/compressionUsingZstd/ZstdFrameFormat.h"

#include <zstd.h>

#include "util/Exception.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/FromCallableSerializer.h"

namespace ad_utility::zstd {

namespace {
// Return a serializer that appends to `out`. All integers of the ZSTD frame
// format are little endian, which is the native byte order on all platforms
// that QLever currently supports.
auto appendingSerializer(std::vector<char>& out) {
  return serialization::WriteViaCallableSerializer{
      [&out](const char* bytes, size_t numBytes) {
        out.insert(out.end(), bytes, bytes + numBytes);
      }};
}
}  // namespace

// _____________________________________________________________________________
std::optional<BlockHeader> BlockHeader::read(ql::span<const char> bytes) {
  if (bytes.size() < size) {
    return std::nullopt;
  }
  auto byte = [&bytes](size_t i) {
    return static_cast<uint32_t>(static_cast<uint8_t>(bytes[i]));
  };
  // The header is a 24-bit little-endian integer: bit 0 is the last-block
  // flag, bits 1-2 are the type, and bits 3-23 are the size of the block.
  uint32_t value = byte(0) | byte(1) << 8 | byte(2) << 16;
  BlockHeader header{(value & 1) != 0, static_cast<Type>((value >> 1) & 3),
                     value >> 3};
  if (header.type_ == Type::Reserved || header.totalSize() > bytes.size()) {
    return std::nullopt;
  }
  return header;
}

// _____________________________________________________________________________
FrameBlocks blocksOfFrame(ql::span<const char> frame) {
  ZSTD_frameHeader header;
  AD_CORRECTNESS_CHECK(
      ZSTD_getFrameHeader(&header, frame.data(), frame.size()) == 0);
  AD_CORRECTNESS_CHECK(header.frameType == ZSTD_frame);
  AD_CORRECTNESS_CHECK(header.checksumFlag == 0);
  auto blocks = frame.subspan(header.headerSize);
  size_t position = 0;
  for (;;) {
    auto block = BlockHeader::read(blocks.subspan(position));
    AD_CORRECTNESS_CHECK(block.has_value());
    if (block->isLast_) {
      AD_CORRECTNESS_CHECK(position + block->totalSize() == blocks.size());
      return {blocks, position};
    }
    position += block->totalSize();
  }
}

// _____________________________________________________________________________
std::optional<ql::span<const char>> leadingNonLastBlocks(
    ql::span<const char> frame, size_t end) {
  size_t begin = ZSTD_frameHeaderSize(frame.data(), frame.size());
  if (ZSTD_isError(begin) || begin >= end || end > frame.size()) {
    return std::nullopt;
  }
  auto blocks = frame.subspan(begin, end - begin);
  for (auto rest = blocks; !rest.empty();) {
    auto block = BlockHeader::read(rest);
    if (!block.has_value() || block->isLast_) {
      return std::nullopt;
    }
    rest = rest.subspan(block->totalSize());
  }
  return blocks;
}

// _____________________________________________________________________________
void clearLastBlockFlag(ql::span<char> blocks, size_t headerOffset) {
  AD_CONTRACT_CHECK(headerOffset + BlockHeader::size <= blocks.size());
  blocks[headerOffset] = static_cast<char>(blocks[headerOffset] & ~1);
}

// _____________________________________________________________________________
void appendSingleSegmentFrameHeader(std::vector<char>& out,
                                    uint64_t contentSize) {
  const size_t begin = out.size();
  // The frame header descriptor: 8 bytes for the content size (`3 << 6`), and
  // the single segment flag (`1 << 5`); no checksum, no dictionary.
  constexpr uint8_t descriptor = (3 << 6) | (1 << 5);
  auto serializer = appendingSerializer(out);
  serializer << static_cast<uint32_t>(ZSTD_MAGICNUMBER);
  serializer << descriptor;
  serializer << contentSize;
  AD_CORRECTNESS_CHECK(out.size() - begin == singleSegmentFrameHeaderSize);
  // Let ZSTD check that it parses the header as intended.
  ZSTD_frameHeader header;
  AD_CORRECTNESS_CHECK(ZSTD_getFrameHeader(&header, out.data() + begin,
                                           singleSegmentFrameHeaderSize) == 0);
  AD_CORRECTNESS_CHECK(header.headerSize == singleSegmentFrameHeaderSize &&
                       header.frameContentSize == contentSize &&
                       header.windowSize == contentSize &&
                       header.checksumFlag == 0 && header.dictID == 0);
}

// _____________________________________________________________________________
void appendSkippableFrame(std::vector<char>& out, uint32_t magicVariant,
                          ql::span<const char> content) {
  AD_CONTRACT_CHECK(magicVariant < 16);
  auto serializer = appendingSerializer(out);
  serializer << static_cast<uint32_t>(ZSTD_MAGIC_SKIPPABLE_START +
                                      magicVariant);
  serializer << static_cast<uint32_t>(content.size());
  out.insert(out.end(), content.begin(), content.end());
}

// _____________________________________________________________________________
std::optional<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, uint32_t magicVariant, size_t contentSize) {
  const size_t frameSize = skippableFrameHeaderSize + contentSize;
  if (bytes.size() < frameSize) {
    return std::nullopt;
  }
  serialization::ByteBufferReadSerializerT reader{
      bytes.last(frameSize).first(skippableFrameHeaderSize)};
  uint32_t magic;
  uint32_t size;
  reader >> magic;
  reader >> size;
  if (magic != ZSTD_MAGIC_SKIPPABLE_START + magicVariant ||
      size != contentSize) {
    return std::nullopt;
  }
  return bytes.last(contentSize);
}

}  // namespace ad_utility::zstd
