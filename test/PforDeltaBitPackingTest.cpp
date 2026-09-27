// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "index/PforDeltaBitPacking.h"
#include "util/GTestHelpers.h"

using ql::index::compression::PforDeltaBitPacking;
using ::testing::HasSubstr;

namespace {
constexpr size_t BLOCK_SIZE = PforDeltaBitPacking::BLOCK_SIZE;
constexpr uint64_t ALL_BITS = ~uint64_t{0};

// _____________________________________________________________________________
// Return the bits of `ids`. The codec works on the bits of the `Id`s, and the
// test `Id`s are arbitrary bit patterns, so they are compared by their bits
// and not by `Id::operator==`, which interprets some datatypes.
std::vector<uint64_t> bitsOf(ql::span<const Id> ids) {
  std::vector<uint64_t> bits;
  bits.reserve(ids.size());
  for (Id id : ids) {
    bits.push_back(id.getBits());
  }
  return bits;
}

// _____________________________________________________________________________
// Return `n` `Id`s, the `i`-th of which has the bits `start + i * step`.
std::vector<Id> arithmetic(uint64_t start, uint64_t step, size_t n) {
  std::vector<Id> ids;
  ids.reserve(n);
  for (uint64_t bits = start; ids.size() < n; bits += step) {
    ids.push_back(Id::fromBits(bits));
  }
  return ids;
}

// _____________________________________________________________________________
// Compress `ids`, check that the block holds `ids.size()` values with
// `expectedBitWidth` bits each and that decompression restores the bits of
// `ids`. Return the compressed block for further checks.
PforDeltaBitPacking::CompressedBlock expectRoundTrip(
    ql::span<const Id> ids, uint8_t expectedBitWidth,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  // Attach the location of the caller to failures.
  auto trace = generateLocationTrace(l);
  auto compressed = PforDeltaBitPacking::compressBlock(ids);
  EXPECT_EQ(compressed.bitWidth_, expectedBitWidth);
  EXPECT_EQ(compressed.numValues_, ids.size());
  EXPECT_EQ(compressed.packedWords_.size(),
            (ids.size() * expectedBitWidth + 63) / 64);
  std::vector<Id> decompressed(ids.size());
  PforDeltaBitPacking::decompressBlock(compressed, decompressed);
  EXPECT_EQ(bitsOf(decompressed), bitsOf(ids));
  return compressed;
}
}  // namespace

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, WidthIsBitWidthOfLargestOffset) {
  // The largest offset of a full block with step 3 is `63 * 3 = 189`, which
  // needs 8 bits, so every value fits into one byte of a word.
  expectRoundTrip(arithmetic(1'000'000, 3, BLOCK_SIZE), 8);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, ValuesThatCrossWordBoundaries) {
  // The largest offset of a full block with step 2 is `63 * 2 = 126`, which
  // needs 7 bits. 7 does not divide 64, so some values (the first at bit 63)
  // are split between two packed words.
  expectRoundTrip(arithmetic(1'000'000, 2, BLOCK_SIZE), 7);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, EqualValuesNeedNoBits) {
  // All offsets are 0, so the block has width 0 and no packed words.
  auto block = expectRoundTrip(arithmetic(42, 0, BLOCK_SIZE), 0);
  EXPECT_TRUE(block.packedWords_.empty());
  EXPECT_EQ(block.baseValue_.getBits(), 42u);
  // The same holds for a single value.
  expectRoundTrip(arithmetic(7, 1, 1), 0);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, FullRangeNeedsSixtyFourBits) {
  // The offset of `ALL_BITS` from the base 0 is `ALL_BITS`, so this block
  // stores every value with all 64 bits.
  std::vector<Id> ids{Id::fromBits(0), Id::fromBits(ALL_BITS), Id::fromBits(1)};
  expectRoundTrip(ids, 64);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, PartialBlock) {
  // 10 values from 100 to 145: the largest offset 45 needs 6 bits, so the
  // block has `(10 * 6 + 63) / 64 = 1` packed word.
  expectRoundTrip(arithmetic(100, 5, 10), 6);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, UnsortedIdsUseTheMinimumAsBase) {
  // The base is the smallest value by bits (1000), not the first one, so the
  // largest offset is `1015 - 1000 = 15`, which needs 4 bits.
  std::vector<Id> ids{Id::fromBits(1'010), Id::fromBits(1'000),
                      Id::fromBits(1'015), Id::fromBits(1'003)};
  auto block = expectRoundTrip(ids, 4);
  EXPECT_EQ(block.baseValue_.getBits(), 1'000u);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, CompressNeedsOneToBlockSizeIds) {
  auto message = HasSubstr("needs between 1 and 64");
  AD_EXPECT_THROW_WITH_MESSAGE(
      PforDeltaBitPacking::compressBlock(std::vector<Id>{}), message);
  AD_EXPECT_THROW_WITH_MESSAGE(
      PforDeltaBitPacking::compressBlock(arithmetic(0, 1, BLOCK_SIZE + 1)),
      message);
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, DecompressNeedsLargeEnoughOutput) {
  auto block = PforDeltaBitPacking::compressBlock(arithmetic(0, 1, 10));
  std::vector<Id> tooSmall(block.numValues_ - 1);
  AD_EXPECT_THROW_WITH_MESSAGE(
      PforDeltaBitPacking::decompressBlock(block, tooSmall),
      HasSubstr("Output too small"));
}

// _____________________________________________________________________________
TEST(PforDeltaBitPackingTest, DecompressRejectsInconsistentBlocks) {
  auto block = PforDeltaBitPacking::compressBlock(arithmetic(0, 1, 10));
  std::vector<Id> output(BLOCK_SIZE);
  auto message = HasSubstr("Invalid `CompressedBlock`");

  // The packed words of 10 values with 4 bits each are too few for 64 values.
  auto tooManyValues = block;
  tooManyValues.numValues_ = BLOCK_SIZE;
  AD_EXPECT_THROW_WITH_MESSAGE(
      PforDeltaBitPacking::decompressBlock(tooManyValues, output), message);

  auto noValues = block;
  noValues.numValues_ = 0;
  AD_EXPECT_THROW_WITH_MESSAGE(
      PforDeltaBitPacking::decompressBlock(noValues, output), message);

  auto tooWide = block;
  tooWide.bitWidth_ = 65;
  AD_EXPECT_THROW_WITH_MESSAGE(
      PforDeltaBitPacking::decompressBlock(tooWide, output), message);
}
