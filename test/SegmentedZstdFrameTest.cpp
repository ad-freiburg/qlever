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

#include "backports/algorithm.h"
#include "util/compressionUsingZstd/SegmentedZstdFrame.h"
#include "util/compressionUsingZstd/ZstdFrameBlocks.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"

using namespace ad_utility::zstd;
using Bytes = std::vector<char>;
using Sizes = std::vector<uint64_t>;

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

// Compressible data without matches close to its end, which cannot end in the
// start state (see `ZstdIndependentUnit.h`): zeros followed by random bytes
// that are longer than the uncompressed tail of a unit.
Bytes compressibleWithoutFinalMatches(unsigned seed) {
  Bytes result(400'000, '\0');
  auto random = randomBytes(100'000, seed);
  result.insert(result.end(), random.begin(), random.end());
  return result;
}

// Return the concatenation of the `parts`.
Bytes concat(const std::vector<Bytes>& parts) {
  Bytes result;
  for (const auto& part : parts) {
    result.insert(result.end(), part.begin(), part.end());
  }
  return result;
}

// Return the sizes of the `parts`.
Sizes sizesOf(const std::vector<Bytes>& parts) {
  Sizes result;
  for (const auto& part : parts) {
    result.push_back(part.size());
  }
  return result;
}

// Check that `compressed` is a valid segmented frame of `data` (with the given
// `segmentSizes`): every ZSTD decompressor reads `data`, and each unit
// decompresses on its own to the corresponding part of `data`. Return the
// parsed frame (throw if there is none).
SegmentedFrame expectValidFrame(const Bytes& compressed, const Bytes& data,
                                const Sizes& segmentSizes) {
  Bytes decompressed(data.size());
  EXPECT_EQ(ZSTD_decompress(decompressed.data(), decompressed.size(),
                            compressed.data(), compressed.size()),
            data.size());
  EXPECT_EQ(decompressed, data);
  auto frame = SegmentedFrame::read(compressed).value();
  EXPECT_EQ(frame.numSegments(), segmentSizes.size());
  uint64_t begin = 0;
  for (size_t i = 0; i < segmentSizes.size(); ++i) {
    EXPECT_EQ(frame.segmentBegin(i), begin);
    EXPECT_EQ(frame.segment(i).size_, segmentSizes[i]);
    EXPECT_EQ(frame.segment(i).hash_,
              hashOfContent(
                  ql::span<const char>{data}.subspan(begin, segmentSizes[i])));
    begin += segmentSizes[i];
  }
  for (size_t u = 0; u < frame.numUnits(); ++u) {
    auto unit = frame.decompressUnit(u).value();
    auto from = frame.segmentBegin(frame.firstSegmentOfUnit(u));
    EXPECT_TRUE(ql::ranges::equal(
        unit, ql::span<const char>{data}.subspan(from, unit.size())));
    // All units except the last end in the start state.
    if (u + 1 < frame.numUnits()) {
      EXPECT_TRUE(frame.unit(u).endsInStartState_);
    }
  }
  return frame;
}
}  // namespace

// Typical data: every segment is a unit of its own.
TEST(SegmentedZstdFrame, oneUnitPerSegment) {
  std::vector<Bytes> parts{binaryLike(300'000, 1), binaryLike(10'000, 2),
                           binaryLike(200'000, 3)};
  auto data = concat(parts);
  auto compressed = compressSegmented(data, sizesOf(parts), 3);
  auto frame = expectValidFrame(compressed, data, sizesOf(parts));
  EXPECT_EQ(frame.numUnits(), 3u);
  for (size_t u = 0; u < 3; ++u) {
    EXPECT_EQ(frame.firstSegmentOfUnit(u), u);
    EXPECT_EQ(frame.unitOfSegment(u), u);
  }
  // Also the last unit ends in the start state, so it can be copied to any
  // position of another frame.
  EXPECT_TRUE(frame.unit(2).endsInStartState_);
  // The segmentation costs little compared to a regular compression.
  auto regular = ZstdWrapper::compress(data.data(), data.size(), 3);
  EXPECT_LT(compressed.size(), regular.size() + regular.size() / 10);
}

// A segment that cannot end in the start state is merged with the next one,
// also if the next one is copied from another frame. Random and empty segments
// are stored uncompressed, and are therefore units of their own.
TEST(SegmentedZstdFrame, mergedSegments) {
  std::vector<Bytes> parts{
      binaryLike(100'000, 1), compressibleWithoutFinalMatches(2),
      binaryLike(100'000, 3), Bytes{},
      randomBytes(1000, 4),   binaryLike(50'000, 5)};
  auto data = concat(parts);
  auto compressed = compressSegmented(data, sizesOf(parts), 1);
  auto frame = expectValidFrame(compressed, data, sizesOf(parts));
  // Only the second segment is merged with the next segment.
  ASSERT_EQ(frame.numUnits(), 5u);
  EXPECT_EQ(frame.unit(0).numSegments_, 1u);
  EXPECT_EQ(frame.unit(1).numSegments_, 2u);
  EXPECT_EQ(frame.unitOfSegment(2), 1u);
  for (size_t u = 2; u < 5; ++u) {
    EXPECT_EQ(frame.unit(u).numSegments_, 1u);
    EXPECT_EQ(frame.unitOfSegment(u + 1), u);
  }
  // The empty and the random segment are stored uncompressed (the size of the
  // empty raw block, and the random bytes plus the size of a block header).
  EXPECT_EQ(frame.unitBlocks(2).size(), 3u);
  EXPECT_EQ(frame.unitBlocks(3).size(), 1003u);

  // Copy the first unit of `frame` after a segment that cannot end in the
  // start state, which forces it to be decompressed and merged.
  SegmentedFrameBuilder builder{1};
  auto withoutMatches = compressibleWithoutFinalMatches(6);
  builder.addSegment(withoutMatches);
  builder.copyUnit(frame, 0);
  auto merged = std::move(builder).finish();
  auto expected = concat({withoutMatches, parts[0]});
  auto mergedFrame = expectValidFrame(merged, expected,
                                      {withoutMatches.size(), parts[0].size()});
  EXPECT_EQ(mergedFrame.numUnits(), 1u);
}

// Units of a base frame whose segments are unchanged are copied.
TEST(SegmentedZstdFrame, reuseUnitsOfBase) {
  std::vector<Bytes> baseParts{binaryLike(300'000, 1), binaryLike(50'000, 2),
                               binaryLike(100'000, 3), binaryLike(70'000, 4)};
  auto base = concat(baseParts);
  auto baseCompressed = compressSegmented(base, sizesOf(baseParts), 3);
  auto baseFrame = SegmentedFrame::read(baseCompressed).value();

  // The second segment changes, the third one is new, the others are equal.
  std::vector<Bytes> targetParts{baseParts[0], binaryLike(60'000, 5),
                                 binaryLike(10'000, 6), baseParts[2],
                                 baseParts[3]};
  auto target = concat(targetParts);
  auto reused = compressSegmented(target, sizesOf(targetParts), 3, &baseFrame);
  auto reusedFrame = expectValidFrame(reused, target, sizesOf(targetParts));
  // Copying yields the same result as compressing again.
  EXPECT_EQ(reused, compressSegmented(target, sizesOf(targetParts), 3));

  // The copied units have the same compressed blocks as in the base (up to
  // the last-block flag).
  auto blocksWithoutLastFlag = [](const SegmentedFrame& frame, size_t u) {
    Bytes blocks{frame.unitBlocks(u).begin(), frame.unitBlocks(u).end()};
    setLastBlockFlag(blocks, frame.unitLastBlockHeader(u), false);
    return blocks;
  };
  for (auto [baseUnit, targetUnit] :
       {std::pair{0, 0}, std::pair{2, 3}, std::pair{3, 4}}) {
    EXPECT_EQ(blocksWithoutLastFlag(baseFrame, baseUnit),
              blocksWithoutLastFlag(reusedFrame, targetUnit));
  }

  // The units end in the start state, so they can be copied in any order, also
  // the last unit of the base.
  std::vector<Bytes> reordered{baseParts[3], baseParts[0]};
  auto data = concat(reordered);
  auto compressed = compressSegmented(data, sizesOf(reordered), 3, &baseFrame);
  auto frame = expectValidFrame(compressed, data, sizesOf(reordered));
  EXPECT_EQ(blocksWithoutLastFlag(frame, 0),
            blocksWithoutLastFlag(baseFrame, 3));
  EXPECT_EQ(compressed, compressSegmented(data, sizesOf(reordered), 3));
}

// A last unit that does not end in the start state is only copied to the end
// of another frame.
TEST(SegmentedZstdFrame, unitWithoutStartStateIsOnlyCopiedToTheEnd) {
  std::vector<Bytes> baseParts{binaryLike(100'000, 1),
                               compressibleWithoutFinalMatches(2)};
  auto base = compressSegmented(concat(baseParts), sizesOf(baseParts), 3);
  auto baseFrame = SegmentedFrame::read(base).value();
  EXPECT_FALSE(baseFrame.unit(1).endsInStartState_);

  // At the end, the unit is copied.
  std::vector<Bytes> sameEnd{binaryLike(10'000, 3), baseParts[1]};
  auto data = concat(sameEnd);
  auto compressed = compressSegmented(data, sizesOf(sameEnd), 3, &baseFrame);
  auto frame = expectValidFrame(compressed, data, sizesOf(sameEnd));
  EXPECT_TRUE(ql::ranges::equal(frame.unitBlocks(1), baseFrame.unitBlocks(1)));

  // Elsewhere, it is compressed again (and merged with the next segment).
  std::vector<Bytes> atStart{baseParts[1], baseParts[0]};
  data = concat(atStart);
  compressed = compressSegmented(data, sizesOf(atStart), 3, &baseFrame);
  frame = expectValidFrame(compressed, data, sizesOf(atStart));
  EXPECT_EQ(frame.numUnits(), 1u);
}

// Empty data, and data without segments.
TEST(SegmentedZstdFrame, emptyData) {
  for (const auto& sizes : {Sizes{}, Sizes{0}, Sizes{0, 0}}) {
    auto compressed = compressSegmented({}, sizes, 3);
    auto frame = expectValidFrame(compressed, {}, sizes);
    // Each empty segment is a unit of its own, and a frame has at least one
    // unit.
    EXPECT_EQ(frame.numUnits(), std::max<size_t>(sizes.size(), 1));
  }
  // The segment sizes must match the data.
  EXPECT_ANY_THROW(compressSegmented(Bytes(10, 'x'), Sizes{5}, 3));
  EXPECT_ANY_THROW(compressSegmented(Bytes(10, 'x'), Sizes{5, 6}, 3));
}

// `SegmentedFrame::read` rejects everything that is not a valid segmented
// frame.
TEST(SegmentedZstdFrame, readRejectsInvalidInput) {
  std::vector<Bytes> parts{binaryLike(100'000, 1), binaryLike(50'000, 2)};
  auto data = concat(parts);
  auto compressed = compressSegmented(data, sizesOf(parts), 1);
  ASSERT_TRUE(SegmentedFrame::read(compressed).has_value());

  EXPECT_FALSE(SegmentedFrame::read(Bytes{}));
  EXPECT_FALSE(SegmentedFrame::read(Bytes(100, 'x')));
  EXPECT_FALSE(
      SegmentedFrame::read(ZstdWrapper::compress(data.data(), data.size(), 1)));
  // Flipping any single byte of the frame header or of the index is detected,
  // except for the bytes of the hashes, which are not checked against the
  // content (that would require a decompression). The compressed blocks
  // themselves are only checked by the decompression.
  auto frameSize =
      ZSTD_findFrameCompressedSize(compressed.data(), compressed.size());
  // The skippable frame header, the magic bytes, the version, and the two
  // counts precede the segments, whose hashes follow their size.
  const size_t segmentsBegin = frameSize + 8 + 8 + 2 + 2 * 8;
  auto isHashByte = [segmentsBegin](size_t i) {
    for (size_t s = 0; s < 2; ++s) {
      size_t hashBegin = segmentsBegin + s * 40 + 8;
      if (i >= hashBegin && i < hashBegin + 32) {
        return true;
      }
    }
    return false;
  };
  // Bit 4 of the frame header descriptor (byte 4) is unused, ZSTD ignores it.
  constexpr size_t descriptorByte = 4;
  for (size_t i = 0; i < compressed.size(); ++i) {
    if ((i >= SingleSegmentFrame::headerSize && i < frameSize) ||
        isHashByte(i) || i == descriptorByte) {
      continue;
    }
    auto corrupted = compressed;
    corrupted[i] ^= 0x10;
    EXPECT_FALSE(SegmentedFrame::read(corrupted)) << i;
  }
  // Trailing bytes after the index.
  auto withTrailing = compressed;
  withTrailing.push_back('x');
  EXPECT_FALSE(SegmentedFrame::read(withTrailing));
}
