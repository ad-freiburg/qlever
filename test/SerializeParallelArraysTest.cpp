// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "util/GTestHelpers.h"
#include "util/serializer/AllocationLimitedReadSerializer.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/SerializeParallelArrays.h"
#include "util/serializer/SerializeString.h"

using ad_utility::serialization::AlignedByteBufferReadSerializer;
using ad_utility::serialization::AlignedByteBufferWriteSerializer;
using ad_utility::serialization::AllocationLimitedReadSerializer;
using ad_utility::serialization::ByteBufferReadSerializer;
using ad_utility::serialization::ByteBufferWriteSerializer;
using ad_utility::serialization::ParallelArrays;
using ad_utility::serialization::zeroCopyDeserializeParallelArrays;

// Test that `ParallelArrays` of different kinds and types are written with
// their size once and read back into arrays that are resized (`std::vector`)
// or that already have the size (`std::array`, `ql::span`), and that arrays of
// different sizes cannot be written or read.
TEST(SerializeParallelArrays, writeAndRead) {
  std::vector ints{3, 4, 5};
  std::array chars{'a', 'b', 'c'};
  std::vector<std::string> strings{"eins", "zwei", "drei"};
  // Arrays that are passed as lvalues are held by reference, temporary ones by
  // value, so no array is copied.
  static_assert(
      std::is_same_v<decltype(ParallelArrays{ints, ql::span{strings}}),
                     ParallelArrays<std::vector<int>&, ql::span<std::string>>>);

  ByteBufferWriteSerializer writer;
  writer | ParallelArrays{ints, chars, ql::span{strings}};
  writer | ParallelArrays{ints, chars};

  // The size is written only once, followed by the elements of each array.
  ByteBufferWriteSerializer twoArrays;
  twoArrays | ParallelArrays{ints, chars};
  EXPECT_EQ(twoArrays.data().size(),
            sizeof(size_t) + 3 * sizeof(int) + 3 * sizeof(char));

  ByteBufferWriteSerializer differentSizes;
  AD_EXPECT_THROW_WITH_MESSAGE(
      (differentSizes | ParallelArrays{ints, ql::span{chars}.first(2)}),
      ::testing::HasSubstr("must have the same size"));

  ByteBufferReadSerializer reader{std::move(writer).data()};
  std::vector<int> intResult;
  std::array<char, 3> charResult{};
  std::vector<std::string> stringResult(3);
  reader | ParallelArrays{intResult, charResult, ql::span{stringResult}};
  EXPECT_EQ(intResult, ints);
  EXPECT_EQ(charResult, chars);
  EXPECT_EQ(stringResult, strings);

  std::array<char, 2> tooFewChars{};
  AD_EXPECT_THROW_WITH_MESSAGE(
      (reader | ParallelArrays{intResult, tooFewChars}),
      ::testing::HasSubstr("must have the same size"));
}

// Test that an aligned serializer reads back `ParallelArrays` with padding
// between the arrays, with a copy and without one
// (`zeroCopyDeserializeParallelArrays`, whose spans point into the buffer of
// the serializer), and that the next value can be read after them.
TEST(SerializeParallelArrays, alignedReadWithPadding) {
  std::vector chars{'a', 'b', 'c'};
  std::vector doubles{1.5, 2.5, 3.5};
  AlignedByteBufferWriteSerializer writer;
  writer | ParallelArrays{chars, doubles};
  writer | 42;
  auto bytes = std::move(writer).data();
  auto expectNextValue = [](auto& reader) {
    int next = 0;
    reader | next;
    EXPECT_EQ(next, 42);
  };

  {
    AlignedByteBufferReadSerializer reader{bytes};
    std::vector<char> charResult;
    std::vector<double> doubleResult;
    reader | ParallelArrays{charResult, doubleResult};
    EXPECT_EQ(charResult, chars);
    EXPECT_EQ(doubleResult, doubles);
    expectNextValue(reader);
  }

  AlignedByteBufferReadSerializer reader{std::move(bytes)};
  auto [charSpan, doubleSpan] =
      zeroCopyDeserializeParallelArrays<char, double>(reader);
  EXPECT_THAT(charSpan, ::testing::ElementsAreArray(chars));
  EXPECT_THAT(doubleSpan, ::testing::ElementsAreArray(doubles));
  const auto& buffer = reader.data();
  for (const char* begin :
       {charSpan.data(), reinterpret_cast<const char*>(doubleSpan.data())}) {
    EXPECT_TRUE(begin >= buffer.data() &&
                begin < buffer.data() + buffer.size());
  }
  expectNextValue(reader);
}

// Test that resizing the arrays when reading respects the allocation limit of
// the serializer, which protects against a huge size in a corrupted input.
TEST(SerializeParallelArrays, readWithAllocationLimit) {
  std::vector ints{3, 4, 5};
  std::vector chars{'a', 'b', 'c'};
  ByteBufferWriteSerializer writer;
  writer | ParallelArrays{ints, chars};
  auto bytes = std::move(writer).data();
  auto read = [&bytes](size_t maxNumBytes) {
    ByteBufferReadSerializer underlying{bytes};
    AllocationLimitedReadSerializer reader{underlying, maxNumBytes};
    std::vector<int> intResult;
    std::vector<char> charResult;
    reader | ParallelArrays{intResult, charResult};
    return std::pair{intResult, charResult};
  };

  size_t numBytes = 3 * sizeof(int) + 3 * sizeof(char);
  EXPECT_EQ(read(numBytes), std::pair(ints, chars));
  AD_EXPECT_THROW_WITH_MESSAGE(
      read(numBytes - 1),
      ::testing::HasSubstr("would allocate more than the maximum"));
}
