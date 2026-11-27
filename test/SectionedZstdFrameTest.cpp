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
#include "util/ZstdFrameTestHelpers.h"
#include "util/compressionUsingZstd/SectionedZstdFrame.h"
#include "util/compressionUsingZstd/ZstdFrameBlocks.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"

using namespace ad_utility::zstd;
using Bytes = std::vector<char>;
using Sizes = std::vector<uint64_t>;
using zstdFrameTestHelpers::hasValue;
using zstdFrameTestHelpers::isError;

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

// Return `compressSectioned(data, sectionSizes, compressionLevel, base)`, which
// has to succeed.
Bytes compressOrFail(ql::span<const char> data, const Sizes& sectionSizes,
                     int compressionLevel,
                     const SectionedFrame* base = nullptr) {
  auto result = compressSectioned(data, sectionSizes, compressionLevel, base);
  EXPECT_TRUE(hasValue(result));
  return std::move(result).value();
}

// Check that `compressed` is a valid sectioned frame of `data` (with the given
// `sectionSizes`): every ZSTD decompressor reads `data`, and each unit
// decompresses on its own to the corresponding part of `data`. Return the
// parsed frame (throw if there is none).
SectionedFrame expectValidFrame(const Bytes& compressed, const Bytes& data,
                                const Sizes& sectionSizes) {
  Bytes decompressed(data.size());
  EXPECT_EQ(ZSTD_decompress(decompressed.data(), decompressed.size(),
                            compressed.data(), compressed.size()),
            data.size());
  EXPECT_EQ(decompressed, data);
  auto frame = SectionedFrame::read(compressed).value();
  EXPECT_EQ(frame.numSections(), sectionSizes.size());
  uint64_t begin = 0;
  for (size_t i = 0; i < sectionSizes.size(); ++i) {
    EXPECT_EQ(frame.sectionBegin(i), begin);
    EXPECT_EQ(frame.section(i).size_, sectionSizes[i]);
    EXPECT_EQ(frame.section(i).hash_,
              hashOfContent(
                  ql::span<const char>{data}.subspan(begin, sectionSizes[i])));
    begin += sectionSizes[i];
  }
  for (size_t u = 0; u < frame.numUnits(); ++u) {
    auto unit = frame.decompressUnit(u).value();
    auto from = frame.sectionBegin(frame.firstSectionOfUnit(u));
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

// Typical data: every section is a unit of its own.
TEST(SectionedZstdFrame, oneUnitPerSection) {
  std::vector<Bytes> parts{binaryLike(300'000, 1), binaryLike(10'000, 2),
                           binaryLike(200'000, 3)};
  auto data = concat(parts);
  auto compressed = compressOrFail(data, sizesOf(parts), 3);
  auto frame = expectValidFrame(compressed, data, sizesOf(parts));
  EXPECT_EQ(frame.numUnits(), 3u);
  for (size_t u = 0; u < 3; ++u) {
    EXPECT_EQ(frame.firstSectionOfUnit(u), u);
    EXPECT_EQ(frame.unitOfSection(u).value(), u);
  }
  // Also the last unit ends in the start state, so it can be copied to any
  // position of another frame.
  EXPECT_TRUE(frame.unit(2).endsInStartState_);
  // The sectioning costs little compared to a regular compression.
  auto regular = ZstdWrapper::compress(data.data(), data.size(), 3);
  EXPECT_LT(compressed.size(), regular.size() + regular.size() / 10);
}

// A section that cannot end in the start state is merged with the next one,
// also if the next one is copied from another frame. Random and empty sections
// are stored uncompressed, and are therefore units of their own.
TEST(SectionedZstdFrame, mergedSections) {
  std::vector<Bytes> parts{
      binaryLike(100'000, 1), compressibleWithoutFinalMatches(2),
      binaryLike(100'000, 3), Bytes{},
      randomBytes(1000, 4),   binaryLike(50'000, 5)};
  auto data = concat(parts);
  auto compressed = compressOrFail(data, sizesOf(parts), 1);
  auto frame = expectValidFrame(compressed, data, sizesOf(parts));
  // Only the second section is merged with the next section.
  ASSERT_EQ(frame.numUnits(), 5u);
  EXPECT_EQ(frame.unit(0).numSections_, 1u);
  EXPECT_EQ(frame.unit(1).numSections_, 2u);
  EXPECT_EQ(frame.unitOfSection(2).value(), 1u);
  for (size_t u = 2; u < 5; ++u) {
    EXPECT_EQ(frame.unit(u).numSections_, 1u);
    EXPECT_EQ(frame.unitOfSection(u + 1).value(), u);
  }
  // The empty and the random section are stored uncompressed (the size of the
  // empty raw block, and the random bytes plus the size of a block header).
  EXPECT_EQ(frame.unitBlocks(2).size(), 3u);
  EXPECT_EQ(frame.unitBlocks(3).size(), 1003u);

  // Copy the first unit of `frame` after a section that cannot end in the
  // start state, which forces it to be decompressed and merged.
  SectionedFrameBuilder builder{1};
  auto withoutMatches = compressibleWithoutFinalMatches(6);
  EXPECT_TRUE(hasValue(builder.addSection(withoutMatches)));
  EXPECT_TRUE(hasValue(builder.copyUnit(frame, 0)));
  auto merged = std::move(builder).finish().value();
  auto expected = concat({withoutMatches, parts[0]});
  auto mergedFrame = expectValidFrame(merged, expected,
                                      {withoutMatches.size(), parts[0].size()});
  EXPECT_EQ(mergedFrame.numUnits(), 1u);
}

// Units of a base frame whose sections are unchanged are copied.
TEST(SectionedZstdFrame, reuseUnitsOfBase) {
  std::vector<Bytes> baseParts{binaryLike(300'000, 1), binaryLike(50'000, 2),
                               binaryLike(100'000, 3), binaryLike(70'000, 4)};
  auto base = concat(baseParts);
  auto baseCompressed = compressOrFail(base, sizesOf(baseParts), 3);
  auto baseFrame = SectionedFrame::read(baseCompressed).value();

  // The second section changes, the third one is new, the others are equal.
  std::vector<Bytes> targetParts{baseParts[0], binaryLike(60'000, 5),
                                 binaryLike(10'000, 6), baseParts[2],
                                 baseParts[3]};
  auto target = concat(targetParts);
  auto reused = compressOrFail(target, sizesOf(targetParts), 3, &baseFrame);
  auto reusedFrame = expectValidFrame(reused, target, sizesOf(targetParts));
  // Copying yields the same result as compressing again.
  EXPECT_EQ(reused, compressOrFail(target, sizesOf(targetParts), 3));

  // The copied units have the same compressed blocks as in the base (up to
  // the last-block flag).
  auto blocksWithoutLastFlag = [](const SectionedFrame& frame, size_t u) {
    Bytes blocks{frame.unitBlocks(u).begin(), frame.unitBlocks(u).end()};
    EXPECT_TRUE(hasValue(
        setLastBlockFlag(blocks, frame.unitLastBlockHeader(u), false)));
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
  auto compressed = compressOrFail(data, sizesOf(reordered), 3, &baseFrame);
  auto frame = expectValidFrame(compressed, data, sizesOf(reordered));
  EXPECT_EQ(blocksWithoutLastFlag(frame, 0),
            blocksWithoutLastFlag(baseFrame, 3));
  EXPECT_EQ(compressed, compressOrFail(data, sizesOf(reordered), 3));
}

// A last unit that does not end in the start state is only copied to the end
// of another frame.
TEST(SectionedZstdFrame, unitWithoutStartStateIsOnlyCopiedToTheEnd) {
  std::vector<Bytes> baseParts{binaryLike(100'000, 1),
                               compressibleWithoutFinalMatches(2)};
  auto base = compressOrFail(concat(baseParts), sizesOf(baseParts), 3);
  auto baseFrame = SectionedFrame::read(base).value();
  EXPECT_FALSE(baseFrame.unit(1).endsInStartState_);

  // At the end, the unit is copied.
  std::vector<Bytes> sameEnd{binaryLike(10'000, 3), baseParts[1]};
  auto data = concat(sameEnd);
  auto compressed = compressOrFail(data, sizesOf(sameEnd), 3, &baseFrame);
  auto frame = expectValidFrame(compressed, data, sizesOf(sameEnd));
  EXPECT_TRUE(ql::ranges::equal(frame.unitBlocks(1), baseFrame.unitBlocks(1)));

  // Elsewhere, it is compressed again (and merged with the next section).
  std::vector<Bytes> atStart{baseParts[1], baseParts[0]};
  data = concat(atStart);
  compressed = compressOrFail(data, sizesOf(atStart), 3, &baseFrame);
  frame = expectValidFrame(compressed, data, sizesOf(atStart));
  EXPECT_EQ(frame.numUnits(), 1u);
}

// Empty data, and data without sections.
TEST(SectionedZstdFrame, emptyData) {
  for (const auto& sizes : {Sizes{}, Sizes{0}, Sizes{0, 0}}) {
    auto compressed = compressOrFail({}, sizes, 3);
    auto frame = expectValidFrame(compressed, {}, sizes);
    // Each empty section is a unit of its own, and a frame has at least one
    // unit.
    EXPECT_EQ(frame.numUnits(), std::max<size_t>(sizes.size(), 1));
  }
  // The section sizes must match the data.
  EXPECT_TRUE(
      isError(compressSectioned(Bytes(10, 'x'), Sizes{5}, 3), "must add up"));
  EXPECT_TRUE(isError(compressSectioned(Bytes(10, 'x'), Sizes{5, 6}, 3),
                      "must add up"));
}

// `SectionedFrame::read` rejects everything that is not a valid sectioned
// frame.
TEST(SectionedZstdFrame, readRejectsInvalidInput) {
  std::vector<Bytes> parts{binaryLike(100'000, 1), binaryLike(50'000, 2)};
  auto data = concat(parts);
  auto compressed = compressOrFail(data, sizesOf(parts), 1);
  ASSERT_TRUE(hasValue(SectionedFrame::read(compressed)));

  EXPECT_TRUE(isError(SectionedFrame::read(Bytes{})));
  EXPECT_TRUE(isError(SectionedFrame::read(Bytes(100, 'x'))));
  EXPECT_TRUE(isError(SectionedFrame::read(
      ZstdWrapper::compress(data.data(), data.size(), 1))));
  // Flipping any single byte of the frame header or of the index is detected,
  // except for the bytes of the hashes, which are not checked against the
  // content (that would require a decompression). The compressed blocks
  // themselves are only checked by the decompression.
  auto frameSize =
      ZSTD_findFrameCompressedSize(compressed.data(), compressed.size());
  // The skippable frame header, the magic bytes, the version, and the two
  // counts precede the sections, whose hashes follow their size.
  const size_t sectionsBegin = frameSize + 8 + 8 + 2 + 2 * 8;
  auto isHashByte = [sectionsBegin](size_t i) {
    for (size_t s = 0; s < 2; ++s) {
      size_t hashBegin = sectionsBegin + s * 40 + 8;
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
    EXPECT_TRUE(isError(SectionedFrame::read(corrupted))) << i;
  }
  // Trailing bytes after the index.
  auto withTrailing = compressed;
  withTrailing.push_back('x');
  EXPECT_TRUE(isError(SectionedFrame::read(withTrailing)));
}

// Invalid uses of the API of a sectioned frame and its builder are reported as
// errors.
TEST(SectionedZstdFrame, invalidUse) {
  std::vector<Bytes> parts{binaryLike(100'000, 1),
                           compressibleWithoutFinalMatches(2)};
  auto data = concat(parts);
  auto compressed = compressOrFail(data, sizesOf(parts), 3);
  auto frame = SectionedFrame::read(compressed).value();
  ASSERT_EQ(frame.numUnits(), 2u);
  EXPECT_TRUE(isError(frame.unitOfSection(2), "does not exist"));
  EXPECT_TRUE(isError(frame.decompressUnit(2), "does not exist"));

  // After a unit that does not end in the start state, nothing can be
  // appended.
  SectionedFrameBuilder builder{3};
  EXPECT_TRUE(isError(builder.copyUnit(frame, 2), "does not exist"));
  ASSERT_FALSE(frame.unit(1).endsInStartState_);
  EXPECT_TRUE(hasValue(builder.copyUnit(frame, 1)));
  EXPECT_TRUE(isError(builder.addSection(parts[0]), "Nothing can be appended"));
  EXPECT_TRUE(isError(builder.copyUnit(frame, 0), "Nothing can be appended"));
  // The frame is still valid.
  auto finished = std::move(builder).finish();
  ASSERT_TRUE(hasValue(finished));
  expectValidFrame(finished.value(), parts[1], {parts[1].size()});
}

// A unit of a corrupted base frame that has to be decompressed to be copied
// is reported as an error.
TEST(SectionedZstdFrame, copyCorruptedUnit) {
  std::vector<Bytes> parts{binaryLike(100'000, 1)};
  auto compressed = compressOrFail(parts[0], sizesOf(parts), 3);
  // Increase the size of the section in the index and the content size in
  // the frame header by one (both little endian, so the lowest byte is
  // first): the frame can still be read, but its unit decodes to one byte less
  // than expected.
  auto corrupted = compressed;
  const size_t frameSize =
      ZSTD_findFrameCompressedSize(compressed.data(), compressed.size());
  // The skippable frame header, the magic bytes, the version, and the two
  // counts precede the size of the first section.
  const size_t sectionSizeBegin = frameSize + 8 + 8 + 2 + 2 * 8;
  // The content size starts after the magic number and the descriptor.
  const size_t contentSizeBegin = 5;
  for (size_t begin : {sectionSizeBegin, contentSizeBegin}) {
    ASSERT_NE(static_cast<uint8_t>(corrupted[begin]), 0xFF);
    ++corrupted[begin];
  }
  auto corruptedFrame = SectionedFrame::read(corrupted);
  ASSERT_TRUE(hasValue(corruptedFrame));
  EXPECT_TRUE(isError(corruptedFrame->decompressUnit(0)));
  // After a section without the start state, the unit has to be decompressed.
  SectionedFrameBuilder builder{3};
  EXPECT_TRUE(hasValue(builder.addSection(compressibleWithoutFinalMatches(2))));
  EXPECT_TRUE(isError(builder.copyUnit(corruptedFrame.value(), 0)));
}
