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

#include <cstring>
#include <vector>

#include "util/ZstdFrameTestHelpers.h"
#include "util/compressionUsingZstd/ZstdFrameHeader.h"
#include "util/serializer/FromCallableSerializer.h"

using namespace ad_utility::zstd;
using namespace zstdFrameTestHelpers;

namespace {
// Return the magic number of a ZSTD frame, followed by the frame header
// `descriptor` and `numFurtherBytes` zero bytes.
Bytes handWrittenHeader(uint8_t descriptor, size_t numFurtherBytes) {
  Bytes result;
  ad_utility::serialization::WriteViaCallableSerializer serializer{
      [&result](const char* bytes, size_t numBytes) {
        result.insert(result.end(), bytes, bytes + numBytes);
      }};
  serializer << static_cast<uint32_t>(ZSTD_MAGICNUMBER);
  serializer << descriptor;
  result.insert(result.end(), numFurtherBytes, 0);
  return result;
}
}  // namespace

// The frame headers of frames that were written by ZSTD are parsed correctly.
TEST(ZstdFrameHeader, framesWrittenByZstd) {
  for (size_t size : {size_t{0}, size_t{100}, size_t{1'000'000}}) {
    auto frame = compress(someData(size, 1));
    auto header = parseFrameHeader(frame);
    ASSERT_TRUE(hasValue(header)) << size;
    EXPECT_FALSE(header->hasChecksum_);
    // The header is followed by exactly one frame's worth of blocks, so it
    // has to be shorter than the frame, and ZSTD has to be able to read the
    // content size from it alone.
    EXPECT_LT(header->headerSize_, frame.size());
    EXPECT_EQ(ZSTD_getFrameContentSize(frame.data(), header->headerSize_),
              size);
  }
}

// The size of the header is computed correctly for all combinations of the
// fields of the frame header descriptor.
TEST(ZstdFrameHeader, handWrittenHeaders) {
  // Expect that a header with the `descriptor` has the `expectedSize` and
  // `expectedChecksum`, and that it is rejected when it is truncated.
  auto expectHeader = [](uint8_t descriptor, size_t expectedSize,
                         bool expectedChecksum = false) {
    auto bytes = handWrittenHeader(descriptor, expectedSize - 5);
    auto header = parseFrameHeader(bytes);
    ASSERT_TRUE(hasValue(header)) << static_cast<int>(descriptor);
    EXPECT_EQ(header->headerSize_, expectedSize);
    EXPECT_EQ(header->hasChecksum_, expectedChecksum);
    EXPECT_TRUE(isError(
        parseFrameHeader(ql::span<const char>{bytes}.first(expectedSize - 1)),
        "truncated"));
  };
  // Single segment flag (`1 << 5`) and the content size flag in bits 6-7:
  // no window descriptor, and 1 (for flag 0), 2, 4, or 8 bytes of content size.
  expectHeader(0x20, 5 + 1);
  expectHeader(0x60, 5 + 2);
  expectHeader(0xA0, 5 + 4);
  expectHeader(0xE0, 5 + 8);
  // Without the single segment flag: a window descriptor, and 0, 2, 4, or 8
  // bytes of content size.
  expectHeader(0x00, 5 + 1);
  expectHeader(0x40, 5 + 1 + 2);
  expectHeader(0x80, 5 + 1 + 4);
  expectHeader(0xC0, 5 + 1 + 8);
  // The dictionary ID flag in bits 0-1: 0, 1, 2, or 4 bytes of dictionary ID.
  expectHeader(0x01, 5 + 1 + 1);
  expectHeader(0x02, 5 + 1 + 2);
  expectHeader(0x03, 5 + 1 + 4);
  expectHeader(0xE3, 5 + 8 + 4);
  // The checksum flag (bit 2) does not change the size of the header.
  expectHeader(0x04, 5 + 1, true);
  expectHeader(0xE4, 5 + 8, true);
  // The unused bit (bit 4) is ignored.
  expectHeader(0x10, 5 + 1);
}

// Invalid frame headers are rejected.
TEST(ZstdFrameHeader, invalidHeaders) {
  // Wrong magic number, empty, or too short for the descriptor.
  EXPECT_TRUE(isError(parseFrameHeader(Bytes(20, 'x')), "magic number"));
  EXPECT_TRUE(isError(parseFrameHeader(Bytes{}), "at least 5 bytes"));
  auto valid = handWrittenHeader(0xE0, 8);
  ASSERT_TRUE(hasValue(parseFrameHeader(valid)));
  EXPECT_TRUE(isError(parseFrameHeader(ql::span<const char>{valid}.first(4)),
                      "at least 5 bytes"));
  // The reserved bit (bit 3) is set.
  auto reserved = valid;
  reserved[4] = static_cast<char>(reserved[4] | (1 << 3));
  EXPECT_TRUE(isError(parseFrameHeader(reserved), "reserved bit"));
  // A skippable frame is not a regular frame.
  Bytes skippable(16, 0);
  uint32_t magic = ZSTD_MAGIC_SKIPPABLE_START;
  std::memcpy(skippable.data(), &magic, sizeof(magic));
  EXPECT_TRUE(isError(parseFrameHeader(skippable), "magic number"));
}
