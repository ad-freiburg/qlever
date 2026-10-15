// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "backports/span.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/LayoutReader.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"

using ad_utility::serialization::AlignedByteBufferWriteSerializer;
using ad_utility::serialization::ByteRange;
using ad_utility::serialization::LayoutReader;
using ::testing::ElementsAre;

namespace {
// Write a byte, a `uint16_t`, a vector of `uint64_t`s, a string, and a `double`
// with the aligned serialization, and return the buffer.
auto writeTestBuffer() {
  AlignedByteBufferWriteSerializer writer;
  writer << uint8_t{0xC3};
  writer << uint16_t{2};
  writer << std::vector<uint64_t>{7, 8, 9};
  writer << std::string{"hello"};
  writer << double{1.5};
  return std::move(writer).data();
}
}  // namespace

// _____________________________________________________________________________
TEST(ByteRange, basics) {
  ByteRange range{3, 10};
  EXPECT_EQ(range.size(), 7u);
  EXPECT_EQ(range, (ByteRange{3, 10}));
  EXPECT_NE(range, (ByteRange{3, 11}));
  EXPECT_EQ(ByteRange{}.size(), 0u);
}

// _____________________________________________________________________________
TEST(LayoutReader, readScalarArrayAndPosition) {
  auto buffer = writeTestBuffer();
  LayoutReader reader{ql::span<const char>{buffer.data(), buffer.size()}};
  EXPECT_EQ(reader.size(), buffer.size());
  EXPECT_EQ(reader.position(), 0u);
  EXPECT_FALSE(reader.atEnd());

  // Scalars are not aligned (only the payloads of arrays are, see below).
  EXPECT_EQ(reader.readScalar<uint8_t>(), 0xC3);
  EXPECT_EQ(reader.position(), 1u);
  EXPECT_EQ(reader.readScalar<uint16_t>(), 2u);
  EXPECT_EQ(reader.position(), 3u);

  // The vector: its size (8 bytes, not aligned), then the payload, which is
  // aligned to 8 bytes.
  auto [range, contents] = reader.readArrayWithContents<uint64_t>();
  EXPECT_EQ(range, (ByteRange{16, 40}));
  EXPECT_THAT(contents, ElementsAre(7, 8, 9));
  EXPECT_EQ(reader.position(), 40u);
  uint64_t second;
  std::memcpy(&second, buffer.data() + range.begin_ + 8, sizeof(second));
  EXPECT_EQ(second, 8u);

  // The string: its size (8 bytes), then the characters (aligned to 1 byte).
  auto stringRange = reader.readArray<char>();
  EXPECT_EQ(stringRange, (ByteRange{48, 53}));
  EXPECT_EQ(
      std::string_view(buffer.data() + stringRange.begin_, stringRange.size()),
      "hello");
  EXPECT_EQ(reader.position(), 53u);

  EXPECT_EQ(reader.readScalar<double>(), 1.5);
  EXPECT_EQ(reader.position(), 61u);
  EXPECT_TRUE(reader.atEnd());
  EXPECT_EQ(reader.position(), reader.size());

  // The underlying serializer can be used directly.
  EXPECT_EQ(reader.reader().getCurrentPosition(), 61u);
}

// _____________________________________________________________________________
TEST(LayoutReader, readPastTheEndThrows) {
  auto buffer = writeTestBuffer();
  LayoutReader reader{ql::span<const char>{buffer.data(), buffer.size()}};
  reader.readScalar<uint8_t>();
  reader.readScalar<uint16_t>();
  reader.readArray<uint64_t>();
  reader.readArray<char>();
  reader.readScalar<double>();
  ASSERT_TRUE(reader.atEnd());
  EXPECT_ANY_THROW(reader.readScalar<uint8_t>());
  EXPECT_ANY_THROW(reader.readArray<char>());

  // A truncated buffer: the size of the vector is read, but its payload is
  // missing.
  LayoutReader truncated{ql::span<const char>{buffer.data(), 20}};
  truncated.readScalar<uint8_t>();
  truncated.readScalar<uint16_t>();
  EXPECT_ANY_THROW(truncated.readArray<uint64_t>());

  // An empty buffer.
  LayoutReader empty{ql::span<const char>{}};
  EXPECT_TRUE(empty.atEnd());
  EXPECT_ANY_THROW(empty.readScalar<uint8_t>());
}

// _____________________________________________________________________________
TEST(LayoutReader, emptyArray) {
  AlignedByteBufferWriteSerializer writer;
  writer << std::vector<uint64_t>{};
  writer << std::string{};
  auto buffer = std::move(writer).data();
  LayoutReader reader{ql::span<const char>{buffer.data(), buffer.size()}};
  auto [range, contents] = reader.readArrayWithContents<uint64_t>();
  EXPECT_EQ(range.size(), 0u);
  EXPECT_EQ(range.begin_, 8u);
  EXPECT_TRUE(contents.empty());
  EXPECT_EQ(reader.readArray<char>().size(), 0u);
  EXPECT_TRUE(reader.atEnd());
}
