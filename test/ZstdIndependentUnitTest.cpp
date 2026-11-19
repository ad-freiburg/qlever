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
#include "util/compressionUsingZstd/ZstdHandWrittenBlocks.h"
#include "util/compressionUsingZstd/ZstdIndependentUnit.h"

using namespace ad_utility::zstd;
using Bytes = std::vector<char>;

namespace {
// Bytes that resemble typical binary data: numbers as text, separated by
// runs of zero bytes. Deterministic for a given `seed`.
Bytes binaryLike(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  Bytes result;
  while (result.size() < numBytes) {
    auto word = std::to_string(generator() % 100000);
    result.insert(result.end(), word.begin(), word.end());
    result.insert(result.end(), generator() % 6, '\0');
  }
  result.resize(numBytes);
  return result;
}

// `numBytes` pseudo-random bytes, which contain (almost) no matches.
Bytes randomBytes(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  Bytes result(numBytes);
  for (auto& byte : result) {
    byte = static_cast<char>(generator());
  }
  return result;
}

// Compressible data without matches close to its end: zeros followed by
// random bytes that are longer than the uncompressed tail of a unit.
Bytes compressibleWithoutFinalMatches(unsigned seed) {
  Bytes result(400'000, '\0');
  auto random = randomBytes(100'000, seed);
  result.insert(result.end(), random.begin(), random.end());
  return result;
}

// Return the concatenation of `a` and `b`.
Bytes concat(const Bytes& a, const Bytes& b) {
  Bytes result{a};
  result.insert(result.end(), b.begin(), b.end());
  return result;
}

// Return a frame that consists of the given `blocks` (the last of which is
// marked as the last block) and decompresses to `contentSize` bytes.
Bytes frameOf(const Bytes& blocks, uint64_t contentSize) {
  Bytes frame;
  appendSingleSegmentFrameHeader(frame, contentSize);
  frame.insert(frame.end(), blocks.begin(), blocks.end());
  auto blockSpan = ql::span<char>{frame}.subspan(singleSegmentFrameHeaderSize);
  setLastBlockFlag(blockSpan, lastBlockHeaderOffset(blockSpan).value(), true);
  return frame;
}

// Decompress the `frame` with ZSTD and return the result (empty on failure).
Bytes decompress(const Bytes& frame, size_t contentSize) {
  Bytes result(contentSize);
  size_t size =
      ZSTD_decompress(result.data(), result.size(), frame.data(), frame.size());
  EXPECT_FALSE(ZSTD_isError(size)) << ZSTD_getErrorName(size);
  return ZSTD_isError(size) ? Bytes{} : result;
}
}  // namespace

// A hand-written single-match block copies three bytes from the given offset,
// also for offsets that need more than one byte in the bitstream, and also if
// the match overlaps itself (offset 1).
TEST(ZstdHandWrittenBlocks, singleMatchBlock) {
  Bytes literals = randomBytes(2000, 1);
  Bytes blocks;
  appendRawBlocks(blocks, literals);
  appendSingleMatchBlock(blocks, 8);
  appendSingleMatchBlock(blocks, 1);
  appendSingleMatchBlock(blocks, 1000);
  appendSingleMatchBlock(blocks, 2000);

  Bytes expected = literals;
  auto copyMatch = [&expected](size_t offset) {
    for (size_t i = 0; i < 3; ++i) {
      expected.push_back(expected[expected.size() - offset]);
    }
  };
  copyMatch(8);
  copyMatch(1);
  copyMatch(1000);
  copyMatch(2000);
  EXPECT_EQ(decompress(frameOf(blocks, expected.size()), expected.size()),
            expected);
}

// Large literals are split into several raw blocks.
TEST(ZstdHandWrittenBlocks, rawBlocks) {
  Bytes literals = randomBytes(300'000, 2);
  Bytes blocks;
  appendRawBlocks(blocks, Bytes{});
  EXPECT_TRUE(blocks.empty());
  appendRawBlocks(blocks, literals);
  EXPECT_EQ(blocks.size(), literals.size() + 3 * BlockHeader::size);
  EXPECT_EQ(decompress(frameOf(blocks, literals.size()), literals.size()),
            literals);
}

// A unit that ends in the start state can be followed by any other unit, and
// each unit can also be decompressed on its own.
TEST(ZstdIndependentUnit, unitsCanBeCombined) {
  auto first = binaryLike(500'000, 3);
  auto second = binaryLike(200'000, 4);
  auto firstUnit = compressIndependentUnit(first, 3, true);
  ASSERT_TRUE(firstUnit.has_value());
  EXPECT_TRUE(firstUnit->endsInStartState_);
  // The compression is not much worse than a regular compression.
  auto regularSize = compressIndependentUnit(first, 3, false)->blocks_.size();
  EXPECT_LT(firstUnit->blocks_.size(), regularSize + regularSize / 20);
  auto secondUnit = compressIndependentUnit(second, 1, false);
  ASSERT_TRUE(secondUnit.has_value());
  EXPECT_FALSE(secondUnit->endsInStartState_);

  auto combined = concat(firstUnit->blocks_, secondUnit->blocks_);
  auto expected = concat(first, second);
  EXPECT_EQ(decompress(frameOf(combined, expected.size()), expected.size()),
            expected);

  // Each unit on its own.
  for (const auto& [unit, data] : {std::pair{&firstUnit.value(), &first},
                                   std::pair{&secondUnit.value(), &second}}) {
    Bytes out(data->size());
    EXPECT_TRUE(decompressBlocks(unit->blocks_, data->size(), out));
    EXPECT_EQ(out, *data);
  }
}

// Data without matches close to its end is stored uncompressed in raw blocks
// if this is cheap (random, short, or empty data), otherwise it cannot end in
// the start state.
TEST(ZstdIndependentUnit, withoutFinalMatches) {
  // Return true iff `unit` consists of raw blocks that decompress to `data`.
  auto isRawUnitOf = [](const CompressedUnit& unit, const Bytes& data) {
    Bytes out(data.size());
    size_t position = 0;
    while (position < unit.blocks_.size()) {
      auto block = BlockHeader::read(
          ql::span<const char>{unit.blocks_}.subspan(position));
      if (!block.has_value() || block->type_ != BlockHeader::Type::Raw) {
        return false;
      }
      position += block->totalSize();
    }
    return unit.endsInStartState_ &&
           decompressBlocks(unit.blocks_, data.size(), out) && out == data;
  };
  for (const auto& data :
       {randomBytes(500'000, 5), Bytes(5, 'x'), Bytes{},
        concat(Bytes(1000, '\0'), randomBytes(500'000, 6))}) {
    auto unit = compressIndependentUnit(data, 3, true);
    ASSERT_TRUE(unit.has_value());
    EXPECT_TRUE(isRawUnitOf(unit.value(), data));
    // A raw unit can be followed by any other unit.
    auto next = binaryLike(10'000, 8);
    auto nextUnit = compressIndependentUnit(next, 3, false).value();
    auto expected = concat(data, next);
    EXPECT_EQ(decompress(frameOf(concat(unit->blocks_, nextUnit.blocks_),
                                 expected.size()),
                         expected.size()),
              expected);
  }
  // Compressible data without final matches cannot end in the start state.
  EXPECT_EQ(
      compressIndependentUnit(compressibleWithoutFinalMatches(7), 3, true),
      std::nullopt);
  // Followed by a few zeros, it is compressed with the final matches.
  auto withZeros = concat(compressibleWithoutFinalMatches(7), Bytes(20, '\0'));
  auto unit = compressIndependentUnit(withZeros, 3, true);
  ASSERT_TRUE(unit.has_value());
  EXPECT_LT(unit->blocks_.size(), withZeros.size() / 2);
  Bytes out(withZeros.size());
  EXPECT_TRUE(decompressBlocks(unit->blocks_, withZeros.size(), out));
  EXPECT_EQ(out, withZeros);

  // Without the start state, everything can be compressed, also empty data.
  for (const auto& data : {randomBytes(1000, 7), Bytes{}}) {
    auto plain = compressIndependentUnit(data, 3, false);
    ASSERT_TRUE(plain.has_value());
    Bytes decompressed(data.size());
    EXPECT_TRUE(decompressBlocks(plain->blocks_, data.size(), decompressed));
    EXPECT_EQ(decompressed, data);
  }
}
