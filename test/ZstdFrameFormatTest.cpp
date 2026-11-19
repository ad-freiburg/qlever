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

// Compress `data` with ZSTD at level 1.
Bytes compress(const Bytes& data) {
  return ZstdWrapper::compress(data.data(), data.size(), 1);
}
}  // namespace

// The frame header of frames that were written by ZSTD and by
// `appendSingleSegmentFrameHeader` is parsed correctly, and invalid headers are
// rejected.
TEST(ZstdFrameFormat, frameHeader) {
  for (size_t size : {size_t{0}, size_t{100}, size_t{1'000'000}}) {
    auto frame = compress(someData(size, 1));
    auto header = parseFrameHeader(frame);
    ASSERT_TRUE(header.has_value()) << size;
    EXPECT_FALSE(header->hasChecksum_);
    // The first block starts right after the header.
    EXPECT_TRUE(BlockHeader::read(
                    ql::span<const char>{frame}.subspan(header->headerSize_))
                    .has_value());
  }
  Bytes ours;
  appendSingleSegmentFrameHeader(ours, 12345);
  EXPECT_EQ(ours.size(), singleSegmentFrameHeaderSize);
  EXPECT_EQ(parseFrameHeader(ours)->headerSize_, singleSegmentFrameHeaderSize);
  EXPECT_EQ(ZSTD_getFrameContentSize(ours.data(), ours.size()), 12345u);

  // Invalid: wrong magic number, truncated, reserved bit set.
  EXPECT_FALSE(parseFrameHeader(Bytes(20, 'x')));
  EXPECT_FALSE(parseFrameHeader(ql::span<const char>{ours}.first(12)));
  EXPECT_FALSE(parseFrameHeader(Bytes{}));
  auto reserved = ours;
  reserved[4] = static_cast<char>(reserved[4] | (1 << 3));
  EXPECT_FALSE(parseFrameHeader(reserved));
}

// Block headers are read and written correctly.
TEST(ZstdFrameFormat, blockHeader) {
  // Last block, compressed, size 5, followed by the 5 bytes of the block.
  Bytes bytes;
  BlockHeader{true, BlockHeader::Type::Compressed, 5}.append(bytes);
  EXPECT_EQ(bytes, (Bytes{0x2D, 0, 0}));
  bytes.insert(bytes.end(), {1, 2, 3, 4, 5});
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
  Bytes rle;
  BlockHeader{false, BlockHeader::Type::Rle, 1000}.append(rle);
  rle.push_back('x');
  header = BlockHeader::read(rle);
  ASSERT_TRUE(header.has_value());
  EXPECT_FALSE(header->isLast_);
  EXPECT_EQ(header->type_, BlockHeader::Type::Rle);
  EXPECT_EQ(header->blockSize_, 1000u);
  EXPECT_EQ(header->totalSize(), 4u);
  // The reserved block type.
  EXPECT_FALSE(BlockHeader::read(Bytes{static_cast<char>(3 << 1), 0, 0}));
}

// The blocks of a frame are found, their last-block flag can be changed, and
// they can be decompressed without their original frame header.
TEST(ZstdFrameFormat, blocks) {
  auto data = someData(400'000, 2);
  auto frame = compress(data);
  auto [blocks, lastHeader] = blocksOfFrame(frame);
  EXPECT_GT(lastHeader, 0u);
  EXPECT_EQ(lastBlockHeaderOffset(blocks), lastHeader);
  EXPECT_EQ(blocks.data() + blocks.size(), frame.data() + frame.size());

  Bytes copy{blocks.begin(), blocks.end()};
  setLastBlockFlag(copy, lastHeader, false);
  EXPECT_FALSE(BlockHeader::read(ql::span<const char>{copy}.subspan(lastHeader))
                   ->isLast_);
  EXPECT_EQ(lastBlockHeaderOffset(copy), lastHeader);
  // Decompress the blocks with and without the last-block flag.
  for (const auto& b : {Bytes{blocks.begin(), blocks.end()}, copy}) {
    Bytes out(data.size());
    EXPECT_TRUE(decompressBlocks(b, data.size(), out));
    EXPECT_EQ(out, data);
  }
  // Errors: wrong size, too small output, truncated blocks, garbage.
  Bytes out(data.size());
  EXPECT_FALSE(decompressBlocks(copy, data.size() - 1, out));
  EXPECT_FALSE(decompressBlocks(copy, data.size(),
                                ql::span<char>{out}.first(data.size() - 1)));
  auto truncated = ql::span<const char>{copy}.first(copy.size() - 1);
  EXPECT_FALSE(lastBlockHeaderOffset(truncated));
  EXPECT_FALSE(decompressBlocks(truncated, data.size(), out));
  EXPECT_FALSE(decompressBlocks(Bytes(100, 'x'), 10, out));
  EXPECT_FALSE(lastBlockHeaderOffset(Bytes{}));

  // A last-block flag that is set before the end is invalid.
  Bytes twice{blocks.begin(), blocks.end()};
  twice.insert(twice.end(), blocks.begin(), blocks.end());
  EXPECT_FALSE(lastBlockHeaderOffset(twice));

  // `blocksOfFrame` requires a complete frame.
  EXPECT_ANY_THROW(
      blocksOfFrame(ql::span<const char>{frame}.first(frame.size() - 1)));
  EXPECT_ANY_THROW(blocksOfFrame(Bytes(100, 'x')));
}

// A trailing skippable frame is ignored by ZSTD and can be found from the end.
TEST(ZstdFrameFormat, trailingSkippableFrame) {
  auto data = someData(1000, 3);
  auto blob = compress(data);
  const size_t frameSize = blob.size();
  std::string content = "some content";
  appendTrailingSkippableFrame(blob, 5, content);
  EXPECT_EQ(blob.size(), frameSize + skippableFrameHeaderSize + content.size() +
                             sizeof(uint32_t));
  // Every decompressor ignores the skippable frame.
  Bytes decompressed(data.size());
  EXPECT_EQ(ZSTD_decompress(decompressed.data(), decompressed.size(),
                            blob.data(), blob.size()),
            data.size());
  EXPECT_EQ(decompressed, data);
  EXPECT_EQ(ZSTD_findFrameCompressedSize(blob.data(), blob.size()), frameSize);

  auto read = readTrailingSkippableFrame(blob, 5);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(std::string(read->begin(), read->end()), content);
  // An empty content.
  Bytes empty;
  appendTrailingSkippableFrame(empty, 0, {});
  EXPECT_EQ(readTrailingSkippableFrame(empty, 0)->size(), 0u);

  // Wrong magic variant, no frame, truncated, or corrupted size.
  EXPECT_FALSE(readTrailingSkippableFrame(blob, 4));
  EXPECT_FALSE(readTrailingSkippableFrame(compress(data), 5));
  EXPECT_FALSE(readTrailingSkippableFrame(Bytes{}, 5));
  EXPECT_FALSE(readTrailingSkippableFrame(
      ql::span<const char>{blob}.first(blob.size() - 1), 5));
  auto corrupted = blob;
  corrupted[frameSize + 4] ^= 1;
  EXPECT_FALSE(readTrailingSkippableFrame(corrupted, 5));
  corrupted = blob;
  corrupted.back() = 0x7F;
  EXPECT_FALSE(readTrailingSkippableFrame(corrupted, 5));
  EXPECT_ANY_THROW(appendTrailingSkippableFrame(blob, 16, content));
}
