// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <zstd.h>

#include <random>
#include <string>
#include <vector>

#include "util/compressionUsingZstd/ZstdFrameFormat.h"
#include "util/compressionUsingZstd/ZstdStablePrefix.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"

using namespace ad_utility::zstd;
using Bytes = std::vector<char>;

namespace {
// Compressible bytes that are deterministic for a given `seed` and large
// enough to be split into several blocks.
Bytes someData(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  Bytes result;
  while (result.size() < numBytes) {
    auto word = std::to_string(generator() % 10000);
    result.insert(result.end(), word.begin(), word.end());
    result.push_back(static_cast<char>(generator() % 3));
  }
  result.resize(numBytes);
  return result;
}

// Decompress `frame`, whose content has the size `size`.
Bytes decompress(const Bytes& frame, size_t size) {
  Bytes result(size);
  EXPECT_EQ(
      ZSTD_decompress(result.data(), result.size(), frame.data(), frame.size()),
      size);
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(ZstdFrameFormat, blockHeader) {
  // Last block, compressed, size 5 (`(5 << 3) | (2 << 1) | 1`), followed by
  // the 5 bytes of the block.
  Bytes bytes{static_cast<char>(0x2D), 0, 0, 1, 2, 3, 4, 5};
  auto header = BlockHeader::read(bytes);
  ASSERT_TRUE(header.has_value());
  EXPECT_TRUE(header->isLast_);
  EXPECT_EQ(header->type_, BlockHeader::Type::Compressed);
  EXPECT_EQ(header->blockSize_, 5u);
  EXPECT_EQ(header->totalSize(), 8u);
  // The block is truncated.
  EXPECT_FALSE(BlockHeader::read(ql::span<const char>{bytes}.first(7)));
  EXPECT_FALSE(BlockHeader::read(ql::span<const char>{bytes}.first(2)));
  // An RLE block of size 1000 only stores a single byte.
  Bytes rle{static_cast<char>(((1000 << 3) | (1 << 1)) & 0xFF),
            static_cast<char>((1000 << 3) >> 8), 0, 'x'};
  header = BlockHeader::read(rle);
  ASSERT_TRUE(header.has_value());
  EXPECT_FALSE(header->isLast_);
  EXPECT_EQ(header->type_, BlockHeader::Type::Rle);
  EXPECT_EQ(header->blockSize_, 1000u);
  EXPECT_EQ(header->totalSize(), 4u);
  // The reserved block type.
  EXPECT_FALSE(BlockHeader::read(Bytes{static_cast<char>(3 << 1), 0, 0}));
}

// Combine the blocks of two independently compressed frames into one frame.
TEST(ZstdFrameFormat, combineBlocksOfTwoFrames) {
  auto first = someData(300'000, 1);
  auto second = someData(200'000, 2);
  auto firstFrame = ZstdWrapper::compress(first.data(), first.size(), 1);
  auto secondFrame = ZstdWrapper::compress(second.data(), second.size(), 1);
  auto [firstBlocks, firstLast] = blocksOfFrame(firstFrame);
  auto secondBlocks = blocksOfFrame(secondFrame).blocks_;
  EXPECT_GT(firstLast, 0u);

  Bytes combined;
  appendSingleSegmentFrameHeader(combined, first.size() + second.size());
  EXPECT_EQ(combined.size(), singleSegmentFrameHeaderSize);
  const size_t prefixBegin = combined.size();
  combined.insert(combined.end(), firstBlocks.begin(), firstBlocks.end());
  clearLastBlockFlag(ql::span<char>{combined}.subspan(prefixBegin), firstLast);
  const size_t prefixEnd = combined.size();
  combined.insert(combined.end(), secondBlocks.begin(), secondBlocks.end());

  // The blocks of `first` can be found again, but only up to a block boundary.
  auto prefix = leadingNonLastBlocks(combined, prefixEnd);
  ASSERT_TRUE(prefix.has_value());
  EXPECT_EQ(prefix->size(), prefixEnd - prefixBegin);
  EXPECT_FALSE(leadingNonLastBlocks(combined, prefixEnd - 1));
  EXPECT_FALSE(leadingNonLastBlocks(combined, prefixBegin));
  EXPECT_FALSE(leadingNonLastBlocks(combined, combined.size()));
  EXPECT_FALSE(leadingNonLastBlocks(combined, combined.size() + 1));
  EXPECT_FALSE(leadingNonLastBlocks(Bytes(100, 'x'), 50));

  // The second frame starts with fresh repeat offsets, so the combination is
  // not guaranteed to decompress to `first + second`. This is what the stable
  // prefix (see `ZstdStablePrefix.h`) is for. The frame structure is valid
  // nevertheless.
  EXPECT_EQ(ZSTD_findFrameCompressedSize(combined.data(), combined.size()),
            combined.size());
  EXPECT_EQ(ZSTD_getFrameContentSize(combined.data(), combined.size()),
            first.size() + second.size());

  // `blocksOfFrame` requires a complete frame.
  EXPECT_ANY_THROW(blocksOfFrame(
      ql::span<const char>{firstFrame}.first(firstFrame.size() - 1)));
  EXPECT_ANY_THROW(blocksOfFrame(Bytes(100, 'x')));
}

// _____________________________________________________________________________
TEST(ZstdFrameFormat, skippableFrame) {
  auto data = someData(1000, 3);
  auto blob = ZstdWrapper::compress(data.data(), data.size(), 1);
  const size_t frameSize = blob.size();
  std::string content = "some content";
  appendSkippableFrame(blob, 5, content);
  EXPECT_EQ(blob.size(), frameSize + skippableFrameHeaderSize + content.size());
  // Every decompressor ignores the skippable frame.
  EXPECT_EQ(decompress(blob, data.size()), data);

  auto read = readTrailingSkippableFrame(blob, 5, content.size());
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(std::string(read->begin(), read->end()), content);
  // Wrong magic variant, wrong size, or too short.
  EXPECT_FALSE(readTrailingSkippableFrame(blob, 4, content.size()));
  EXPECT_FALSE(readTrailingSkippableFrame(blob, 5, content.size() - 1));
  EXPECT_FALSE(readTrailingSkippableFrame(blob, 5, blob.size()));
  EXPECT_FALSE(readTrailingSkippableFrame(Bytes{}, 5, 0));
  EXPECT_ANY_THROW(appendSkippableFrame(blob, 16, content));
}

// _____________________________________________________________________________
TEST(ZstdStablePrefix, encodeAndAppend) {
  auto prefix = someData(500'000, 4);
  auto blocks = encodeStablePrefix(prefix, 9);
  if (!stablePrefixEncodingIsSupported()) {
    EXPECT_FALSE(blocks.has_value());
    return;
  }
  ASSERT_TRUE(blocks.has_value());
  EXPECT_TRUE(isStablePrefix(prefix, blocks.value()));

  // Append the blocks of a fresh compression of another remainder.
  auto remainder = someData(100'000, 5);
  auto remainderFrame =
      ZstdWrapper::compress(remainder.data(), remainder.size(), 3);
  auto remainderBlocks = blocksOfFrame(remainderFrame).blocks_;
  Bytes frame;
  appendSingleSegmentFrameHeader(frame, prefix.size() + remainder.size());
  frame.insert(frame.end(), blocks->begin(), blocks->end());
  frame.insert(frame.end(), remainderBlocks.begin(), remainderBlocks.end());
  auto expected = prefix;
  expected.insert(expected.end(), remainder.begin(), remainder.end());
  EXPECT_EQ(decompress(frame, expected.size()), expected);

  // The blocks do not belong to a different prefix.
  auto otherPrefix = prefix;
  otherPrefix[1000] ^= 1;
  EXPECT_FALSE(isStablePrefix(otherPrefix, blocks.value()));
  // Without suitable matches at the end, there is no stable prefix.
  Bytes noMatches(100'000);
  std::mt19937 generator{6};
  for (auto& byte : noMatches) {
    byte = static_cast<char>(generator());
  }
  EXPECT_FALSE(encodeStablePrefix(noMatches, 9).has_value());
}
