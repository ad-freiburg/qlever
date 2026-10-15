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
#include "util/Serializer/LayoutReadSerializer.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"

using ad_utility::serialization::AlignedByteBufferWriteSerializer;
using ad_utility::serialization::ByteRange;
using ad_utility::serialization::LayoutReadSerializer;
using ad_utility::serialization::zeroCopyDeserializeToSpan;
using ::testing::ElementsAre;

namespace {
// Read an array of `T`s from the `reader` and return the byte range of its
// payload, that is, of the raw elements, which excludes the preceding size
// and alignment padding.
template <typename T>
ByteRange readArrayRange(LayoutReadSerializer& reader) {
  return reader.rangeOf(zeroCopyDeserializeToSpan<T>(reader));
}

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
TEST(LayoutReadSerializer, readScalarArrayAndPosition) {
  auto buffer = writeTestBuffer();
  LayoutReadSerializer reader{
      ql::span<const char>{buffer.data(), buffer.size()}};
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
  auto range = readArrayRange<uint64_t>(reader);
  EXPECT_EQ(range, (ByteRange{16, 40}));
  EXPECT_EQ(reader.position(), 40u);
  uint64_t second;
  std::memcpy(&second, buffer.data() + range.begin_ + 8, sizeof(second));
  EXPECT_EQ(second, 8u);

  // The string: its size (8 bytes), then the characters (aligned to 1 byte).
  auto stringRange = readArrayRange<char>(reader);
  EXPECT_EQ(stringRange, (ByteRange{48, 53}));
  EXPECT_EQ(
      std::string_view(buffer.data() + stringRange.begin_, stringRange.size()),
      "hello");
  EXPECT_EQ(reader.position(), 53u);

  // Scalars can also be read via the regular serialization.
  double d = 0;
  reader >> d;
  EXPECT_EQ(d, 1.5);
  EXPECT_EQ(reader.position(), 61u);
  EXPECT_TRUE(reader.atEnd());
  EXPECT_EQ(reader.position(), reader.size());
}

// _____________________________________________________________________________
TEST(LayoutReadSerializer, isAZeroCopyReadSerializerAndRangeOf) {
  using namespace ad_utility::serialization;
  static_assert(ReadSerializer<LayoutReadSerializer>);
  static_assert(ZeroCopyReadSerializer<LayoutReadSerializer>);
  static_assert(LayoutSerializer<LayoutReadSerializer>);
  static_assert(LayoutSerializer<LayoutReadSerializer&>);
  static_assert(!LayoutSerializer<AlignedByteBufferReadSerializer>);
  static_assert(!LayoutSerializer<AlignedByteBufferWriteSerializer>);
  static_assert(!LayoutSerializer<int>);

  auto buffer = writeTestBuffer();
  LayoutReadSerializer reader{
      ql::span<const char>{buffer.data(), buffer.size()}};
  reader.readScalar<uint8_t>();
  reader.readScalar<uint16_t>();

  // Zero-copy reads work, and `rangeOf` converts their result to the byte
  // range.
  auto span = zeroCopyDeserializeToSpan<uint64_t>(reader);
  EXPECT_THAT(span, ElementsAre(7, 8, 9));
  EXPECT_EQ(reader.rangeOf(span), (ByteRange{16, 40}));
  auto string = zeroCopyDeserializeToStringView(reader);
  EXPECT_EQ(string, "hello");
  EXPECT_EQ(reader.rangeOf(ql::span<const char>{string.data(), string.size()}),
            (ByteRange{48, 53}));
  EXPECT_EQ(reader.rangeOf(span.subspan(1, 1)), (ByteRange{24, 32}));
  EXPECT_EQ(reader.rangeOf(span.subspan(3)), (ByteRange{40, 40}));
}

// _____________________________________________________________________________
TEST(LayoutReadSerializer, readPastTheEndThrows) {
  auto buffer = writeTestBuffer();
  LayoutReadSerializer reader{
      ql::span<const char>{buffer.data(), buffer.size()}};
  reader.readScalar<uint8_t>();
  reader.readScalar<uint16_t>();
  readArrayRange<uint64_t>(reader);
  readArrayRange<char>(reader);
  reader.readScalar<double>();
  ASSERT_TRUE(reader.atEnd());
  EXPECT_ANY_THROW(reader.readScalar<uint8_t>());
  EXPECT_ANY_THROW(readArrayRange<char>(reader));

  // A truncated buffer: the size of the vector is read, but its payload is
  // missing.
  LayoutReadSerializer truncated{ql::span<const char>{buffer.data(), 20}};
  truncated.readScalar<uint8_t>();
  truncated.readScalar<uint16_t>();
  EXPECT_ANY_THROW(readArrayRange<uint64_t>(truncated));

  // An empty buffer.
  LayoutReadSerializer empty{ql::span<const char>{}};
  EXPECT_TRUE(empty.atEnd());
  EXPECT_ANY_THROW(empty.readScalar<uint8_t>());
}

// _____________________________________________________________________________
TEST(LayoutReadSerializer, emptyArray) {
  AlignedByteBufferWriteSerializer writer;
  writer << std::vector<uint64_t>{};
  writer << std::string{};
  auto buffer = std::move(writer).data();
  LayoutReadSerializer reader{
      ql::span<const char>{buffer.data(), buffer.size()}};
  auto range = readArrayRange<uint64_t>(reader);
  EXPECT_EQ(range.size(), 0u);
  EXPECT_EQ(range.begin_, 8u);
  EXPECT_EQ(readArrayRange<char>(reader).size(), 0u);
  EXPECT_TRUE(reader.atEnd());
}
