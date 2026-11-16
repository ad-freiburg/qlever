// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests that only make sense for the `SplitLayoutId`, which stores a full
// 64-bit payload next to a datatype byte. The tests for both layouts are in
// `IdBaseTest.cpp`.

#include <absl/base/casts.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>

#include "../../../ValueIdTestHelpers.h"
#include "engine/idTable/splitLayout/SplitLayoutId.h"
#include "util/Random.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/Serializer.h"

namespace columnBasedIdTable::splitLayout {

namespace {
// The bit representation of an `Id` with the given datatype and payload.
constexpr SplitLayoutIdBitRepresentation makeBits(Datatype datatype,
                                                  uint64_t payload) {
  return {static_cast<uint8_t>(datatype), payload};
}

// Every bit pattern is the payload of a valid `double`.
auto bitsGenerator = ad_utility::SlowRandomIntGenerator<uint64_t>();

// Whether `a` and `b` have the same bytes, including the padding.
bool bytesEqual(SplitLayoutId a, SplitLayoutId b) {
  return std::memcmp(&a, &b, sizeof(SplitLayoutId)) == 0;
}
}  // namespace

// Test the layout: a full 64-bit payload word and one byte for the datatype,
// which are kept apart in the bit representation, and the ranges of the
// integers and the indices that follow from it.
TEST(SplitLayoutId, layout) {
  static_assert(std::is_same_v<SplitLayoutId::PayloadType, uint64_t>);
  static_assert(std::is_same_v<SplitLayoutId::DatatypeType, uint8_t>);
  static_assert(SplitLayoutId::numDataBits == 64);
  static_assert(SplitLayoutId::numDatatypeBits == 8);
  static_assert(SplitLayoutId::maxIndex ==
                std::numeric_limits<uint64_t>::max());
  static_assert(SplitLayoutId::IntegerType::min() ==
                std::numeric_limits<int64_t>::min());
  static_assert(SplitLayoutId::IntegerType::max() ==
                std::numeric_limits<int64_t>::max());
  static_assert(SplitLayoutId::maxInt == std::numeric_limits<int64_t>::max());
  static_assert(SplitLayoutId::min().getBits() ==
                makeBits(Datatype::Undefined, 0));

  for (auto id : makeRandomIds<SplitLayoutId>()) {
    EXPECT_EQ(id.getBits(), makeBits(id.getDatatype(), id.getPayloadBits()));
  }
  // `fromBits` accepts every bit representation, also the one of `max()`,
  // whose datatype byte is not a `Datatype`.
  const auto maxBits = SplitLayoutId::max().getBits();
  EXPECT_EQ(SplitLayoutId::fromBits(maxBits).getBits(), maxBits);
  // The highest bit of the payload is a regular bit of an index: it makes the
  // `Id` larger and does not change the datatype.
  EXPECT_LT(makeVocabId<SplitLayoutId>((uint64_t{1} << 63) - 1),
            makeVocabId<SplitLayoutId>(uint64_t{1} << 63));
}

// Test that a `double` is stored without any loss of precision: the payload is
// its bit pattern, so also the signs of zero, the NaNs and the subnormal
// numbers are kept.
TEST(SplitLayoutId, makeFromDoubleIsLossless) {
  for (size_t i = 0; i < numElements; ++i) {
    auto bits = bitsGenerator();
    auto id = SplitLayoutId::makeFromDouble(absl::bit_cast<double>(bits));
    EXPECT_EQ(id.getPayloadBits(), bits);
    EXPECT_EQ(absl::bit_cast<uint64_t>(id.getDouble()), bits);
  }

  // Positive and negative 0 are different bit patterns and hence different
  // `Id`s, although `0.0 == -0.0`.
  EXPECT_NE(SplitLayoutId::makeFromDouble(0.0),
            SplitLayoutId::makeFromDouble(-0.0));
  EXPECT_TRUE(std::signbit(SplitLayoutId::makeFromDouble(-0.0).getDouble()));
  // The smallest subnormal number is not rounded to zero.
  using Limits = std::numeric_limits<double>;
  EXPECT_EQ(SplitLayoutId::minPositiveDouble, Limits::denorm_min());
  EXPECT_NE(SplitLayoutId::makeFromDouble(Limits::denorm_min()),
            SplitLayoutId::makeFromDouble(0.0));
}

// Test that the bytes of an `Id` are completely determined by its payload and
// datatype (the padding is always zero), because the serialization copies the
// raw bytes.
TEST(SplitLayoutId, byteRepresentation) {
  // 8 bytes payload + 1 byte datatype + 7 bytes explicit padding.
  static_assert(sizeof(SplitLayoutId) == 16);
  // The compiler doesn't insert any further (uninitialized) padding.
  static_assert(std::has_unique_object_representations_v<SplitLayoutId>);

  // The same `Id`, created in different ways, has the same bytes.
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeFromInt(5),
                         SplitLayoutId::fromBits(Datatype::Int, 5)));
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeFromInt(5),
                         SplitLayoutId::fromBits(makeBits(Datatype::Int, 5))));
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeFromInt(5).incremented(),
                         SplitLayoutId::makeFromInt(6)));
  EXPECT_TRUE(bytesEqual(
      makeVocabId<SplitLayoutId>(SplitLayoutId::maxIndex).incremented(),
      SplitLayoutId::fromBits(Datatype::LocalVocabIndex, 0)));
  // Different `Id`s have different bytes, in particular if only the datatype
  // or only the payload differs.
  EXPECT_FALSE(bytesEqual(SplitLayoutId::makeFromInt(0),
                          SplitLayoutId::makeFromDouble(0.0)));
  EXPECT_FALSE(
      bytesEqual(SplitLayoutId::makeFromInt(0), SplitLayoutId::makeFromInt(1)));

  // The serialization copies these 16 bytes.
  for (auto id : makeRandomIds<SplitLayoutId>()) {
    EXPECT_TRUE(bytesEqual(id, SplitLayoutId::fromBits(id.getBits())));
    ad_utility::serialization::ByteBufferWriteSerializer writer;
    writer << id;
    EXPECT_EQ(std::move(writer).data().size(), sizeof(SplitLayoutId));
  }
}
}  // namespace columnBasedIdTable::splitLayout
