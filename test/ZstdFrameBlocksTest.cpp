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

#include <vector>

#include "backports/algorithm.h"
#include "util/ZstdFrameTestHelpers.h"
#include "util/compressionUsingZstd/ZstdFrameBlocks.h"
#include "util/compressionUsingZstd/ZstdFrameHeader.h"

using namespace ad_utility::zstd;
using namespace zstdFrameTestHelpers;

// Block headers are read and written correctly.
TEST(ZstdFrameBlocks, blockHeader) {
  // Last block, compressed, size 5, followed by the 5 bytes of the block.
  Bytes bytes;
  EXPECT_TRUE(hasValue(
      BlockHeader{true, BlockHeader::Type::Compressed, 5}.append(bytes)));
  // `1 | 2 << 1 | 5 << 3`.
  EXPECT_EQ(bytes, (Bytes{0x2D, 0, 0}));
  bytes.insert(bytes.end(), {1, 2, 3, 4, 5});
  auto header = BlockHeader::read(bytes);
  ASSERT_TRUE(hasValue(header));
  EXPECT_TRUE(header->isLast_);
  EXPECT_EQ(header->type_, BlockHeader::Type::Compressed);
  EXPECT_EQ(header->blockSize_, 5u);
  EXPECT_EQ(header->totalSize(), 8u);
  // The block is truncated.
  EXPECT_TRUE(isError(BlockHeader::read(ql::span<const char>{bytes}.first(7)),
                      "block is truncated"));
  EXPECT_TRUE(isError(BlockHeader::read(ql::span<const char>{bytes}.first(2)),
                      "block header is truncated"));
  // An RLE block of size 1000 only stores a single byte.
  Bytes rle;
  EXPECT_TRUE(
      hasValue(BlockHeader{false, BlockHeader::Type::Rle, 1000}.append(rle)));
  rle.push_back('x');
  header = BlockHeader::read(rle);
  ASSERT_TRUE(hasValue(header));
  EXPECT_FALSE(header->isLast_);
  EXPECT_EQ(header->type_, BlockHeader::Type::Rle);
  EXPECT_EQ(header->blockSize_, 1000u);
  EXPECT_EQ(header->totalSize(), 4u);
  // The largest block size that fits into the 21 bits uses all three bytes.
  Bytes large;
  EXPECT_TRUE(hasValue(
      BlockHeader{false, BlockHeader::Type::Rle, BlockHeader::maxBlockSize}
          .append(large)));
  large.push_back('x');
  EXPECT_EQ(BlockHeader::read(large)->blockSize_, BlockHeader::maxBlockSize);
  // Block sizes that need more than 21 bits and the reserved type cannot be
  // written, and nothing is appended.
  const auto sizeBefore = large.size();
  EXPECT_TRUE(isError(
      BlockHeader{false, BlockHeader::Type::Raw, BlockHeader::maxBlockSize + 1}
          .append(large),
      "does not fit"));
  EXPECT_TRUE(
      isError(BlockHeader{false, BlockHeader::Type::Reserved, 0}.append(large),
              "reserved"));
  EXPECT_EQ(large.size(), sizeBefore);
  // The reserved block type.
  EXPECT_TRUE(isError(BlockHeader::read(Bytes{static_cast<char>(3 << 1), 0, 0}),
                      "reserved"));
}

// The blocks of a frame are found, and their last-block flag can be changed.
TEST(ZstdFrameBlocks, blocksOfFrame) {
  auto data = someData(400'000, 2);
  auto frame = compress(data);
  auto frameBlocks = blocksOfFrame(frame);
  ASSERT_TRUE(hasValue(frameBlocks));
  auto [blocks, lastHeader] = frameBlocks.value();
  EXPECT_GT(lastHeader, 0u);
  EXPECT_EQ(lastBlockHeaderOffset(blocks).value(), lastHeader);
  // The blocks start right after the frame header and end with the frame.
  EXPECT_EQ(blocks.data(), frame.data() + parseFrameHeader(frame)->headerSize_);
  EXPECT_EQ(blocks.data() + blocks.size(), frame.data() + frame.size());

  Bytes copy{blocks.begin(), blocks.end()};
  EXPECT_TRUE(hasValue(setLastBlockFlag(copy, lastHeader, false)));
  EXPECT_FALSE(BlockHeader::read(ql::span<const char>{copy}.subspan(lastHeader))
                   ->isLast_);
  EXPECT_EQ(lastBlockHeaderOffset(copy).value(), lastHeader);
  EXPECT_TRUE(hasValue(setLastBlockFlag(copy, lastHeader, true)));
  EXPECT_TRUE(ql::ranges::equal(copy, blocks));
  // There is no complete header at these offsets.
  EXPECT_TRUE(isError(setLastBlockFlag(copy, copy.size() - 2, true),
                      "no ZSTD block header"));
  EXPECT_TRUE(isError(setLastBlockFlag(copy, copy.size() + 10, true),
                      "no ZSTD block header"));
  EXPECT_TRUE(ql::ranges::equal(copy, blocks));

  // Truncated or empty blocks.
  auto truncated = ql::span<const char>{copy}.first(copy.size() - 1);
  EXPECT_TRUE(isError(lastBlockHeaderOffset(truncated), "truncated"));
  EXPECT_TRUE(isError(lastBlockHeaderOffset(Bytes{}), "empty"));

  // A last-block flag that is set before the end is invalid.
  Bytes twice{blocks.begin(), blocks.end()};
  twice.insert(twice.end(), blocks.begin(), blocks.end());
  EXPECT_TRUE(isError(lastBlockHeaderOffset(twice), "followed by more data"));

  // `blocksOfFrame` requires exactly one complete frame.
  EXPECT_TRUE(isError(
      blocksOfFrame(ql::span<const char>{frame}.first(frame.size() - 1)),
      "Invalid ZSTD frame"));
  auto withTrailingByte = frame;
  withTrailingByte.push_back('x');
  EXPECT_TRUE(
      isError(blocksOfFrame(withTrailingByte), "not exactly one ZSTD frame"));
  EXPECT_TRUE(isError(blocksOfFrame(Bytes(100, 'x')), "magic number"));
}

// The checksum of a frame is not part of its blocks.
TEST(ZstdFrameBlocks, blocksOfFrameWithChecksum) {
  auto data = someData(400'000, 3);
  auto frame = compressWithChecksum(data);
  ASSERT_TRUE(parseFrameHeader(frame)->hasChecksum_);
  auto frameBlocks = blocksOfFrame(frame);
  ASSERT_TRUE(hasValue(frameBlocks));
  auto [blocks, lastHeader] = frameBlocks.value();
  // The blocks end 4 bytes before the end of the frame.
  EXPECT_EQ(blocks.data() + blocks.size() + 4, frame.data() + frame.size());
  EXPECT_TRUE(BlockHeader::read(blocks.subspan(lastHeader))->isLast_);
  // The blocks decompress to the content of the frame.
  Bytes out(data.size());
  EXPECT_TRUE(
      hasValue(SingleSegmentFrame::decompressBlocks(blocks, data.size(), out)));
  EXPECT_EQ(out, data);
}

// The header of a single segment frame is written as intended.
TEST(ZstdFrameBlocks, singleSegmentFrameHeader) {
  for (uint64_t contentSize :
       {uint64_t{0}, uint64_t{12345}, uint64_t{1} << 40}) {
    Bytes header{'a', 'b'};
    SingleSegmentFrame::appendHeader(header, contentSize);
    // The bytes that were already there are kept.
    EXPECT_EQ(header.size(), 2 + SingleSegmentFrame::headerSize);
    EXPECT_EQ(header[0], 'a');
    auto ours = ql::span<const char>{header}.subspan(2);
    EXPECT_EQ(parseFrameHeader(ours)->headerSize_,
              SingleSegmentFrame::headerSize);
    EXPECT_FALSE(parseFrameHeader(ours)->hasChecksum_);
    EXPECT_EQ(ZSTD_getFrameContentSize(ours.data(), ours.size()), contentSize);
  }
}

// The blocks of a frame can be decompressed without their original frame
// header, with and without the last-block flag.
TEST(ZstdFrameBlocks, singleSegmentFrameDecompressBlocks) {
  auto data = someData(400'000, 2);
  auto frame = compress(data);
  auto [blocks, lastHeader] = blocksOfFrame(frame).value();
  Bytes copy{blocks.begin(), blocks.end()};
  ASSERT_TRUE(hasValue(setLastBlockFlag(copy, lastHeader, false)));
  for (const auto& b : {Bytes{blocks.begin(), blocks.end()}, copy}) {
    Bytes out(data.size());
    EXPECT_TRUE(
        hasValue(SingleSegmentFrame::decompressBlocks(b, data.size(), out)));
    EXPECT_EQ(out, data);
  }
  // Errors: wrong size, too small output, truncated blocks, garbage.
  Bytes out(data.size());
  EXPECT_TRUE(
      isError(SingleSegmentFrame::decompressBlocks(copy, data.size() - 1, out),
              "Decompressing ZSTD blocks failed"));
  Bytes largerOut(data.size() + 1);
  EXPECT_TRUE(isError(
      SingleSegmentFrame::decompressBlocks(copy, data.size() + 1, largerOut)));
  EXPECT_TRUE(isError(
      SingleSegmentFrame::decompressBlocks(
          copy, data.size(), ql::span<char>{out}.first(data.size() - 1)),
      "too small"));
  auto truncated = ql::span<const char>{copy}.first(copy.size() - 1);
  EXPECT_TRUE(
      isError(SingleSegmentFrame::decompressBlocks(truncated, data.size(), out),
              "truncated"));
  EXPECT_TRUE(
      isError(SingleSegmentFrame::decompressBlocks(Bytes(100, 'x'), 10, out)));
}
