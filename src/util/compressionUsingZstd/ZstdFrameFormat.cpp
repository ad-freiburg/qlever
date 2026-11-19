// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/ZstdFrameFormat.h"

#include <zstd.h>

#include <array>
#include <limits>

#include "util/Exception.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/FromCallableSerializer.h"

namespace ad_utility::zstd {

namespace {
// Return a serializer that appends to `out`.
auto appendingSerializer(std::vector<char>& out) {
  return serialization::WriteViaCallableSerializer{
      [&out](const char* bytes, size_t numBytes) {
        out.insert(out.end(), bytes, bytes + numBytes);
      }};
}

// Read the `T` at the start of `bytes`, which has to be large enough.
template <typename T>
T readValue(ql::span<const char> bytes) {
  serialization::ByteBufferReadSerializerT reader{bytes.first(sizeof(T))};
  T value;
  reader >> value;
  return value;
}

// Return the byte at `bytes[i]` as an unsigned integer.
uint32_t byteAt(ql::span<const char> bytes, size_t i) {
  return static_cast<uint8_t>(bytes[i]);
}
}  // namespace

// _____________________________________________________________________________
std::optional<FrameHeader> parseFrameHeader(ql::span<const char> bytes) {
  // The magic number (4 bytes) and the frame header descriptor (1 byte).
  if (bytes.size() < 5 || readValue<uint32_t>(bytes) != ZSTD_MAGICNUMBER) {
    return std::nullopt;
  }
  const uint32_t descriptor = byteAt(bytes, 4);
  const uint32_t contentSizeFlag = descriptor >> 6;
  const bool singleSegment = (descriptor >> 5) & 1;
  const bool reservedBit = (descriptor >> 3) & 1;
  const bool hasChecksum = (descriptor >> 2) & 1;
  const uint32_t dictionaryIdFlag = descriptor & 3;
  if (reservedBit) {
    return std::nullopt;
  }
  constexpr std::array<size_t, 4> dictionaryIdSizes{0, 1, 2, 4};
  constexpr std::array<size_t, 4> contentSizeSizes{0, 2, 4, 8};
  // A single segment frame without a content size flag still stores the
  // content size in a single byte.
  const size_t contentSizeSize = contentSizeFlag == 0 && singleSegment
                                     ? 1
                                     : contentSizeSizes[contentSizeFlag];
  // The window descriptor is omitted in a single segment frame.
  const size_t headerSize = 5 + (singleSegment ? 0 : 1) +
                            dictionaryIdSizes[dictionaryIdFlag] +
                            contentSizeSize;
  if (bytes.size() < headerSize) {
    return std::nullopt;
  }
  return FrameHeader{headerSize, hasChecksum};
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
  auto header = ql::span<const char>{out}.subspan(begin);
  AD_CORRECTNESS_CHECK(ZSTD_getFrameContentSize(header.data(), header.size()) ==
                       contentSize);
  AD_CORRECTNESS_CHECK(parseFrameHeader(header).value().headerSize_ ==
                       singleSegmentFrameHeaderSize);
}

// _____________________________________________________________________________
std::optional<BlockHeader> BlockHeader::read(ql::span<const char> bytes) {
  if (bytes.size() < size) {
    return std::nullopt;
  }
  // The header is a 24-bit little-endian integer: bit 0 is the last-block
  // flag, bits 1-2 are the type, and bits 3-23 are the size of the block.
  uint32_t value =
      byteAt(bytes, 0) | byteAt(bytes, 1) << 8 | byteAt(bytes, 2) << 16;
  BlockHeader header{(value & 1) != 0, static_cast<Type>((value >> 1) & 3),
                     value >> 3};
  if (header.type_ == Type::Reserved || header.totalSize() > bytes.size()) {
    return std::nullopt;
  }
  return header;
}

// _____________________________________________________________________________
void BlockHeader::append(std::vector<char>& out) const {
  AD_CONTRACT_CHECK(blockSize_ < (1u << 21) && type_ != Type::Reserved);
  uint32_t value = static_cast<uint32_t>(isLast_) |
                   static_cast<uint32_t>(type_) << 1 | blockSize_ << 3;
  out.push_back(static_cast<char>(value & 0xFF));
  out.push_back(static_cast<char>((value >> 8) & 0xFF));
  out.push_back(static_cast<char>(value >> 16));
}

// _____________________________________________________________________________
FrameBlocks blocksOfFrame(ql::span<const char> frame) {
  auto header = parseFrameHeader(frame);
  AD_CORRECTNESS_CHECK(header.has_value() && !header->hasChecksum_);
  AD_CORRECTNESS_CHECK(
      ZSTD_findFrameCompressedSize(frame.data(), frame.size()) == frame.size());
  auto blocks = frame.subspan(header->headerSize_);
  auto lastHeader = lastBlockHeaderOffset(blocks);
  AD_CORRECTNESS_CHECK(lastHeader.has_value());
  AD_CORRECTNESS_CHECK(
      BlockHeader::read(blocks.subspan(lastHeader.value()))->isLast_);
  return {blocks, lastHeader.value()};
}

// _____________________________________________________________________________
std::optional<size_t> lastBlockHeaderOffset(ql::span<const char> blocks) {
  size_t position = 0;
  std::optional<size_t> lastHeader;
  while (position < blocks.size()) {
    auto block = BlockHeader::read(blocks.subspan(position));
    if (!block.has_value()) {
      return std::nullopt;
    }
    lastHeader = position;
    position += block->totalSize();
    if (block->isLast_ && position != blocks.size()) {
      return std::nullopt;
    }
  }
  return lastHeader;
}

// _____________________________________________________________________________
void setLastBlockFlag(ql::span<char> blocks, size_t headerOffset, bool isLast) {
  AD_CONTRACT_CHECK(headerOffset + BlockHeader::size <= blocks.size());
  auto& byte = blocks[headerOffset];
  byte = static_cast<char>(isLast ? (byte | 1) : (byte & ~1));
}

// _____________________________________________________________________________
bool decompressBlocks(ql::span<const char> blocks, uint64_t contentSize,
                      ql::span<char> out) {
  auto lastHeader = lastBlockHeaderOffset(blocks);
  if (!lastHeader.has_value() || out.size() < contentSize) {
    return false;
  }
  std::vector<char> frame;
  frame.reserve(singleSegmentFrameHeaderSize + blocks.size());
  appendSingleSegmentFrameHeader(frame, contentSize);
  frame.insert(frame.end(), blocks.begin(), blocks.end());
  setLastBlockFlag(ql::span<char>{frame}.subspan(singleSegmentFrameHeaderSize),
                   lastHeader.value(), true);
  size_t size =
      ZSTD_decompress(out.data(), contentSize, frame.data(), frame.size());
  return !ZSTD_isError(size) && size == contentSize;
}

// _____________________________________________________________________________
void appendTrailingSkippableFrame(std::vector<char>& out, uint32_t magicVariant,
                                  ql::span<const char> content) {
  AD_CONTRACT_CHECK(magicVariant < 16);
  // The content of the skippable frame is the `content` followed by the size
  // of the complete frame.
  const uint64_t frameSize =
      skippableFrameHeaderSize + content.size() + sizeof(uint32_t);
  AD_CONTRACT_CHECK(frameSize <= std::numeric_limits<uint32_t>::max());
  auto serializer = appendingSerializer(out);
  serializer << static_cast<uint32_t>(ZSTD_MAGIC_SKIPPABLE_START +
                                      magicVariant);
  serializer << static_cast<uint32_t>(frameSize - skippableFrameHeaderSize);
  out.insert(out.end(), content.begin(), content.end());
  serializer << static_cast<uint32_t>(frameSize);
}

// _____________________________________________________________________________
std::optional<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, uint32_t magicVariant) {
  constexpr size_t minFrameSize = skippableFrameHeaderSize + sizeof(uint32_t);
  if (bytes.size() < minFrameSize) {
    return std::nullopt;
  }
  const size_t frameSize = readValue<uint32_t>(bytes.last(sizeof(uint32_t)));
  if (frameSize < minFrameSize || frameSize > bytes.size()) {
    return std::nullopt;
  }
  auto frame = bytes.last(frameSize);
  if (readValue<uint32_t>(frame) != ZSTD_MAGIC_SKIPPABLE_START + magicVariant ||
      readValue<uint32_t>(frame.subspan(sizeof(uint32_t))) !=
          frameSize - skippableFrameHeaderSize) {
    return std::nullopt;
  }
  return frame.subspan(skippableFrameHeaderSize, frameSize - minFrameSize);
}

}  // namespace ad_utility::zstd
