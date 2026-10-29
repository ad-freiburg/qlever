// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for `SplitLayoutId`. They mirror the tests of the legacy `ValueId` in
// `FoldedValueIdTest.cpp`. Where the split layout behaves differently (a full
// 64 bit payload next to a datatype byte), the test is adapted and the
// difference is explained.
//
// NOTE: This test is only built if `USE_SPLIT_LAYOUT` is set (see
// `CMakeLists.txt`), because then `Id` is the `SplitLayoutId`, which the test
// utilities (e.g. `LocalVocabId`) create.

#include <absl/base/casts.h>
#include <absl/hash/hash_testing.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "../../../util/GTestHelpers.h"
#include "../../../util/IdTestHelpers.h"
#include "../../../util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/splitLayout/SplitLayoutId.h"
#include "index/LocalVocabEntry.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "util/Algorithm.h"
#include "util/BitUtils.h"
#include "util/HashSet.h"
#include "util/Random.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

namespace columnBasedIdTable::splitLayout {
static_assert(std::is_same_v<Id, SplitLayoutId>);

namespace {
using ad_utility::testing::BlankNodeId;
using ad_utility::testing::LocalVocabId;
using ad_utility::testing::TextRecordId;
using ad_utility::testing::VocabId;
using ad_utility::testing::WordVocabId;

// Enabling cheaper unit tests when building in Debug mode.
#ifdef QLEVER_RUN_EXPENSIVE_TESTS
constexpr size_t numElements = 10'000;
// The number of random values that some tests run through.
constexpr size_t numRandomChecks = 10'000;
#else
constexpr size_t numElements = 10;
constexpr size_t numRandomChecks = 10;
#endif

// The number of datatypes, and the number of values that the datatype byte can
// hold (which is larger).
constexpr size_t numDatatypes = static_cast<size_t>(Datatype::MaxValue) + 1;
constexpr size_t numDatatypeByteValues = size_t{1}
                                         << SplitLayoutId::numDatatypeBits;

// A payload with all bits set: the largest index, and the integer -1.
constexpr uint64_t allBitsSet = std::numeric_limits<uint64_t>::max();

// Some payloads that are combined with every datatype.
constexpr std::array<uint64_t, 4> examplePayloads{0, 1, 123456789, allBitsSet};

// All 64 bits of the payload are available for the index types.
auto indexGenerator =
    ad_utility::SlowRandomIntGenerator<uint64_t>(0, SplitLayoutId::maxIndex);
// Every `int64_t` can be represented.
auto integerGenerator = ad_utility::SlowRandomIntGenerator<int64_t>();
// Every bit pattern is the payload of a valid `double`.
auto bitsGenerator = ad_utility::SlowRandomIntGenerator<uint64_t>();
// A random `double` that is uniformly distributed over the bit patterns (and
// hence over all exponents). NaNs are skipped, because they can't be ordered.
auto randomDouble = [] {
  while (true)
    if (const auto d = absl::bit_cast<double>(bitsGenerator()); !std::isnan(d))
      return d;
};

// There is no factory for the `SecondaryVocabIndex` in the test utilities.
SplitLayoutId makeSecondaryVocabId(uint64_t value) {
  return SplitLayoutId::makeFromSecondaryVocabIndex(
      SecondaryVocabIndex::make(value));
}

// The word of a `LocalVocabIndex` `Id` of the test utilities: the decimal
// representation of the number that the `Id` was created from.
std::string getLocalVocabIndex(SplitLayoutId id) {
  AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::LocalVocabIndex);
  return std::string{id.getLocalVocabIndex()->getContent()};
}

// The number behind an index: the strong index types (e.g. `VocabIndex`) wrap a
// `uint64_t`, the other values are returned as they are.
auto indexValue = []<typename T>(const T& index) {
  if constexpr (std::is_integral_v<T> || std::is_same_v<T, std::string>) {
    return index;
  } else {
    return index.get();
  }
};

// The bit representation of an `Id` with the given datatype and payload.
constexpr SplitLayoutIdBitRepresentation makeBits(Datatype datatype,
                                                  uint64_t payload) {
  return {static_cast<uint8_t>(datatype), payload};
}

// Add `numElements` random values from the `generator`, each of them converted
// to an `Id` by `makeIds` and repeated 1 to 4 times.
auto addIdsFromGenerator = [](auto& generator, auto makeIds,
                              std::vector<SplitLayoutId>& ids) {
  ad_utility::SlowRandomIntGenerator<uint16_t> numRepetitionGenerator(1, 4);
  for (size_t i = 0; i < numElements; ++i) {
    auto randomValue = generator();
    auto numRepetitions = numRepetitionGenerator();
    for (size_t j = 0; j < numRepetitions; ++j) {
      ids.push_back(makeIds(randomValue));
    }
  }
};

// Random `Id`s of the datatype `Double`, including the special values like the
// infinities and NaNs, in random order.
auto makeRandomDoubleIds = []() {
  std::vector<SplitLayoutId> ids;
  addIdsFromGenerator(randomDouble, &SplitLayoutId::makeFromDouble, ids);

  using Limits = std::numeric_limits<double>;
  for (size_t i = 0; i < numElements; ++i) {
    for (double d :
         {0.0, -0.0, Limits::infinity(), -Limits::infinity(),
          Limits::quiet_NaN(), Limits::signaling_NaN(), Limits::max(),
          Limits::min(), Limits::denorm_min(), -Limits::denorm_min()}) {
      ids.push_back(SplitLayoutId::makeFromDouble(d));
    }
  }
  ad_utility::randomShuffle(ids.begin(), ids.end());
  return ids;
};

// Random `Id`s of the datatypes `Double`, `Int`, `Undefined` and all index
// types except `EncodedVal`, in random order.
auto makeRandomIds = []() {
  std::vector<SplitLayoutId> ids = makeRandomDoubleIds();
  addIdsFromGenerator(indexGenerator, VocabId, ids);
  addIdsFromGenerator(indexGenerator, LocalVocabId, ids);
  addIdsFromGenerator(indexGenerator, TextRecordId, ids);
  addIdsFromGenerator(indexGenerator, WordVocabId, ids);
  addIdsFromGenerator(indexGenerator, BlankNodeId, ids);
  addIdsFromGenerator(indexGenerator, &makeSecondaryVocabId, ids);
  addIdsFromGenerator(integerGenerator, &SplitLayoutId::makeFromInt, ids);

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(SplitLayoutId::makeUndefined());
  }

  ad_utility::randomShuffle(ids.begin(), ids.end());
  return ids;
};

// An `Id` together with its datatype and payload.
struct KnownId {
  SplitLayoutId id_;
  Datatype datatype_;
  uint64_t payload_;
};

std::vector<KnownId> makeKnownIds() {
  return {
      {SplitLayoutId::makeUndefined(), Datatype::Undefined, 0},
      {SplitLayoutId::makeFromBool(false), Datatype::Bool, 0},
      {SplitLayoutId::makeFromBool(true), Datatype::Bool, 1},
      {SplitLayoutId::makeFromInt(0), Datatype::Int, 0},
      {SplitLayoutId::makeFromInt(42), Datatype::Int, 42},
      // A negative integer has all payload bits set, and its sign must not
      // leak into the datatype.
      {SplitLayoutId::makeFromInt(-1), Datatype::Int, allBitsSet},
      {VocabId(42), Datatype::VocabIndex, 42},
      // The largest index fills the whole payload, but does not touch the
      // datatype.
      {VocabId(SplitLayoutId::maxIndex), Datatype::VocabIndex,
       SplitLayoutId::maxIndex},
  };
}

// The overloads of `fromBits` that take a datatype and a payload, as functions
// of a `Datatype` and a payload: all bits at once, the datatype as a number,
// and the `Datatype`.
auto fromBitRepresentation = [](Datatype datatype, uint64_t payload) {
  return SplitLayoutId::fromBits(makeBits(datatype, payload));
};
auto fromDatatypeBits = [](Datatype datatype, uint64_t payload) {
  return SplitLayoutId::fromBits(static_cast<uint8_t>(datatype), payload);
};
auto fromDatatype = [](Datatype datatype, uint64_t payload) {
  return SplitLayoutId::fromBits(datatype, payload);
};

// Test an overload of `fromBits` (see above) that accepts the first
// `numAcceptedDatatypes` values of the datatype byte.
template <typename FromBits>
void testFromBits(FromBits fromBits, size_t numAcceptedDatatypes) {
  for (const auto& [id, datatype, payload] : makeKnownIds()) {
    auto rebuilt = fromBits(datatype, payload);
    EXPECT_EQ(rebuilt, id);
    EXPECT_EQ(rebuilt.getBits(), id.getBits());
  }

  // Every datatype can be combined with every payload, and the two parts do
  // not influence each other.
  for (size_t datatype = 0; datatype < numAcceptedDatatypes; ++datatype) {
    for (uint64_t payload : examplePayloads) {
      auto id = fromBits(static_cast<Datatype>(datatype), payload);
      EXPECT_EQ(uint64_t{id.getDatatypeBits()}, datatype);
      EXPECT_EQ(id.getPayloadBits(), payload);
    }
  }
}
}  // namespace

namespace {

struct SplitLayoutIdTest : public ::testing::Test {
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
};

}  // namespace

// Test that a `double` is stored without any loss of precision, including the
// special values like the infinities, the NaNs, and the subnormal numbers.
TEST_F(SplitLayoutIdTest, makeFromDouble) {
  // The payload has 64 bits, so all bits of a `double` fit into it and the
  // conversion is lossless. (The legacy `ValueId` drops the lowest
  // `numDatatypeBits` bits of the mantissa, and rounds subnormal numbers that
  // are too small to zero.)
  auto testRoundtrip = [](double d) {
    auto id = SplitLayoutId::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // The payload is the bit pattern of the `double`.
    ASSERT_EQ(id.getPayloadBits(), absl::bit_cast<uint64_t>(d));
    if (std::isnan(d)) {
      ASSERT_TRUE(std::isnan(id.getDouble()));
    } else {
      ASSERT_EQ(absl::bit_cast<uint64_t>(id.getDouble()),
                absl::bit_cast<uint64_t>(d));
    }
  };
  for (size_t i = 0; i < numRandomChecks; ++i) {
    testRoundtrip(randomDouble());
    // Every bit pattern, also the ones of the NaNs.
    testRoundtrip(absl::bit_cast<double>(bitsGenerator()));
  }

  using Limits = std::numeric_limits<double>;
  for (double d :
       {0.0, -0.0, 1.0, -1.5, 13.37, Limits::infinity(), -Limits::infinity(),
        Limits::max(), Limits::lowest(), Limits::min(), -Limits::min(),
        Limits::denorm_min(), -Limits::denorm_min(), Limits::epsilon(),
        Limits::quiet_NaN(), Limits::signaling_NaN()}) {
    testRoundtrip(d);
  }

  // Positive and negative 0 are different bit patterns and hence different
  // `Id`s, although `0.0 == -0.0`.
  ASSERT_NE(absl::bit_cast<uint64_t>(0.0), absl::bit_cast<uint64_t>(-0.0));
  ASSERT_EQ(0.0, -0.0);
  ASSERT_NE(SplitLayoutId::makeFromDouble(0.0),
            SplitLayoutId::makeFromDouble(-0.0));
  ASSERT_FALSE(std::signbit(SplitLayoutId::makeFromDouble(0.0).getDouble()));
  ASSERT_TRUE(std::signbit(SplitLayoutId::makeFromDouble(-0.0).getDouble()));

  // Subnormal numbers are not rounded to zero.
  ASSERT_EQ(SplitLayoutId::makeFromDouble(Limits::denorm_min()).getDouble(),
            Limits::denorm_min());
  ASSERT_NE(SplitLayoutId::makeFromDouble(Limits::denorm_min()),
            SplitLayoutId::makeFromDouble(0.0));
  ASSERT_EQ(SplitLayoutId::makeFromDouble(-Limits::denorm_min()).getDouble(),
            -Limits::denorm_min());

  ASSERT_TRUE(std::isnan(
      SplitLayoutId::makeFromDouble(Limits::quiet_NaN()).getDouble()));
  ASSERT_TRUE(std::isnan(
      SplitLayoutId::makeFromDouble(Limits::signaling_NaN()).getDouble()));
}

// Test that every `int64_t` is stored without an overflow.
TEST_F(SplitLayoutIdTest, makeFromInt) {
  // The payload has 64 bits, so every `int64_t` can be represented and there
  // is no overflow. (The legacy `ValueId` has 60 bits for an integer.)
  static_assert(SplitLayoutId::IntegerType::min() ==
                std::numeric_limits<int64_t>::min());
  static_assert(SplitLayoutId::IntegerType::max() ==
                std::numeric_limits<int64_t>::max());
  static_assert(SplitLayoutId::maxInt == std::numeric_limits<int64_t>::max());

  auto testInt = [](int64_t value) {
    auto id = SplitLayoutId::makeFromInt(value);
    ASSERT_EQ(id.getDatatype(), Datatype::Int);
    ASSERT_EQ(id.getInt(), value);
  };
  for (size_t i = 0; i < numRandomChecks; ++i) {
    testInt(integerGenerator());
  }

  constexpr int64_t minInt = std::numeric_limits<int64_t>::min();
  constexpr int64_t maxInt = std::numeric_limits<int64_t>::max();
  for (int64_t value : {minInt, minInt + 1, int64_t{-1}, int64_t{0}, int64_t{1},
                        maxInt - 1, maxInt}) {
    testInt(value);
  }
}

// Test `makeFromBool` and `makeBoolFromZeroOrOne` together with `getBool` and
// `getBoolLiteral`.
TEST_F(SplitLayoutIdTest, makeFromBool) {
  EXPECT_TRUE(SplitLayoutId::makeBoolFromZeroOrOne(true).getBool());
  EXPECT_TRUE(SplitLayoutId::makeFromBool(true).getBool());
  EXPECT_FALSE(SplitLayoutId::makeBoolFromZeroOrOne(false).getBool());
  EXPECT_FALSE(SplitLayoutId::makeFromBool(false).getBool());

  EXPECT_EQ(SplitLayoutId::makeBoolFromZeroOrOne(true).getBoolLiteral(), "1");
  EXPECT_EQ(SplitLayoutId::makeFromBool(true).getBoolLiteral(), "true");
  EXPECT_EQ(SplitLayoutId::makeBoolFromZeroOrOne(false).getBoolLiteral(), "0");
  EXPECT_EQ(SplitLayoutId::makeFromBool(false).getBoolLiteral(), "false");
}

// Test that the index types store every 64-bit value and return it unchanged.
// Unlike for the legacy `ValueId`, no value is too large.
TEST_F(SplitLayoutIdTest, Indices) {
  // Random values and the boundaries of the range.
  auto testRandomIds = [&](auto makeId, auto getFromId, Datatype type) {
    auto testSingle = [&](uint64_t value) {
      auto id = makeId(value);
      ASSERT_EQ(id.getDatatype(), type);
      ASSERT_EQ(indexValue(std::invoke(getFromId, id)), value);
    };
    for (size_t idx = 0; idx < numRandomChecks; ++idx) {
      testSingle(indexGenerator());
    }
    testSingle(0);
    testSingle(SplitLayoutId::maxIndex);
  };

  static_assert(SplitLayoutId::maxIndex ==
                std::numeric_limits<uint64_t>::max());
  testRandomIds(TextRecordId, &SplitLayoutId::getTextRecordIndex,
                Datatype::TextRecordIndex);
  testRandomIds(VocabId, &SplitLayoutId::getVocabIndex, Datatype::VocabIndex);

  auto localVocabWordToInt = [](const auto& input) {
    return std::strtoull(getLocalVocabIndex(input).c_str(), nullptr, 10);
  };
  testRandomIds(LocalVocabId, localVocabWordToInt, Datatype::LocalVocabIndex);
  testRandomIds(WordVocabId, &SplitLayoutId::getWordVocabIndex,
                Datatype::WordVocabIndex);
  testRandomIds(BlankNodeId, &SplitLayoutId::getBlankNodeIndex,
                Datatype::BlankNodeIndex);
  testRandomIds(&SplitLayoutId::makeFromEncodedVal,
                &SplitLayoutId::getEncodedVal, Datatype::EncodedVal);
  testRandomIds(&makeSecondaryVocabId, &SplitLayoutId::getSecondaryVocabIndex,
                Datatype::SecondaryVocabIndex);
}

// Test `makeUndefined` and `getUndefined`.
TEST_F(SplitLayoutIdTest, Undefined) {
  auto id = SplitLayoutId::makeUndefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);

  // `getUndefined()` returns the single value of `UndefinedType`. Its main
  // purpose is the generic code in `visit`, which has to dispatch on the
  // datatype, so we also test it via that path.
  static_assert(std::is_same_v<decltype(id.getUndefined()),
                               SplitLayoutId::UndefinedType>);
  auto isUndefinedType = [](const auto& value) {
    return std::is_same_v<std::decay_t<decltype(value)>,
                          SplitLayoutId::UndefinedType>;
  };
  EXPECT_TRUE(isUndefinedType(id.getUndefined()));
  EXPECT_TRUE(id.visit(isUndefinedType));
  EXPECT_FALSE(SplitLayoutId::makeFromInt(42).visit(isUndefinedType));
}

// Test `getDatatypeBits` and `getPayloadBits`, the two parts of the bits of
// an `Id`.
TEST_F(SplitLayoutIdTest, getDatatypeBitsAndGetPayloadBits) {
  // The layout: a full 64 bit payload word and one byte for the datatype.
  static_assert(std::is_same_v<SplitLayoutId::PayloadType, uint64_t>);
  static_assert(std::is_same_v<SplitLayoutId::DatatypeType, uint8_t>);
  static_assert(SplitLayoutId::numDataBits == 64);
  static_assert(SplitLayoutId::numDatatypeBits == 8);
  static_assert(numDatatypes <= numDatatypeByteValues);

  // The datatype bits and the payload bits are the two parts of the bits of an
  // `Id`. Unlike for the legacy `ValueId`, they are not packed into one
  // integer, so `getBits()` is the pair of the two.
  auto checkParts = [](SplitLayoutId id) {
    EXPECT_EQ(id.getDatatypeBits(), static_cast<uint8_t>(id.getDatatype()));
    EXPECT_EQ(id.getBits(), (SplitLayoutIdBitRepresentation{
                                id.getDatatypeBits(), id.getPayloadBits()}));
  };
  for (auto id : makeRandomIds()) {
    checkParts(id);
  }
  for (const auto& [id, datatype, payload] : makeKnownIds()) {
    checkParts(id);
    EXPECT_EQ(id.getDatatypeBits(), static_cast<uint8_t>(datatype));
    EXPECT_EQ(id.getPayloadBits(), payload);
  }
}

// Test `fromBits(bits)`, the counterpart of `getBits()`. It accepts every byte
// as the datatype, see `fromBitsChecksThatTheDatatypeExists`.
TEST_F(SplitLayoutIdTest, fromBitsWithBitRepresentation) {
  for (auto id : makeRandomIds()) {
    EXPECT_EQ(SplitLayoutId::fromBits(id.getBits()).getBits(), id.getBits());
  }
  testFromBits(fromBitRepresentation, numDatatypeByteValues);
}

// Test `fromBits(datatype, payload)` with the number of the datatype, the
// counterpart of `getDatatypeBits` and `getPayloadBits`.
TEST_F(SplitLayoutIdTest, fromBitsWithDatatypeBitsAndPayload) {
  for (auto id : makeRandomIds()) {
    EXPECT_EQ(SplitLayoutId::fromBits(id.getDatatypeBits(), id.getPayloadBits())
                  .getBits(),
              id.getBits());
  }
  testFromBits(fromDatatypeBits, numDatatypes);

  // Plain integers select this overload and not the one for the `Datatype`.
  EXPECT_EQ(SplitLayoutId::fromBits(0, 0), SplitLayoutId::makeUndefined());
  EXPECT_EQ(SplitLayoutId::fromBits(static_cast<uint8_t>(Datatype::Int), 5),
            SplitLayoutId::fromBits(Datatype::Int, 5));
}

// Test the overload `fromBits(Datatype, payload)`, which takes the `Datatype`
// directly instead of its number.
TEST_F(SplitLayoutIdTest, fromBitsWithDatatype) {
  for (auto id : makeRandomIds()) {
    EXPECT_EQ(SplitLayoutId::fromBits(id.getDatatype(), id.getPayloadBits())
                  .getBits(),
              id.getBits());
  }
  testFromBits(fromDatatype, numDatatypes);
}

// Test the overload of `fromBits` that takes a single integer. It creates an
// `Id` of type `Undefined` with this payload, as for the special `Id`s (see
// `SpecialIds.h`), which are not `UNDEF`.
TEST_F(SplitLayoutIdTest, fromBitsWithSingleInteger) {
  auto special = SplitLayoutId::fromBits(uint64_t{3});
  EXPECT_EQ(special.getDatatype(), Datatype::Undefined);
  EXPECT_EQ(special.getPayloadBits(), 3u);
  EXPECT_FALSE(special.isUndefined());
  EXPECT_EQ(SplitLayoutId::fromBits(uint64_t{0}),
            SplitLayoutId::makeUndefined());
}

// Test that the overloads of `fromBits` that take a datatype reject one that
// does not exist (the datatype byte can hold more values than there are
// datatypes). Like all expensive checks, this only happens if they are enabled
// (in `Debug` builds or via `ENABLE_EXPENSIVE_CHECKS`). The functions are
// `noexcept`, so a failed check terminates the process.
TEST(SplitLayoutId, fromBitsChecksThatTheDatatypeExists) {
  const auto largest = static_cast<uint8_t>(Datatype::MaxValue);
  const auto invalid = static_cast<uint8_t>(largest + 1);

  // The largest existing datatype is always accepted.
  EXPECT_EQ(SplitLayoutId::fromBits(largest, 0).getDatatype(),
            Datatype::MaxValue);
  EXPECT_EQ(SplitLayoutId::fromBits(Datatype::MaxValue, 0).getDatatype(),
            Datatype::MaxValue);

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    EXPECT_DEATH(SplitLayoutId::fromBits(invalid, 0),
                 "The datatype does not exist");
    EXPECT_DEATH(SplitLayoutId::fromBits(uint8_t{0xFF}, 0),
                 "The datatype does not exist");
    EXPECT_DEATH(SplitLayoutId::fromBits(static_cast<Datatype>(invalid), 0),
                 "The datatype does not exist");
  } else {
    EXPECT_EQ(SplitLayoutId::fromBits(invalid, 0).getDatatypeBits(), invalid);
  }

  // The overload for the bits is not checked, so that every bit representation
  // of an `Id` can be turned into an `Id` again, also the one of the largest
  // `Id`, whose datatype byte is not a datatype.
  const auto maxBits = SplitLayoutId::max().getBits();
  EXPECT_EQ(SplitLayoutId::fromBits(maxBits).getBits(), maxBits);
}

// Test that sorted `Id`s are ordered by their datatype, except for the string
// types, whose `Id`s are ordered by their words.
TEST_F(SplitLayoutIdTest, OrderingDifferentDatatypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end());

  auto compareByDatatypeAndIndexTypes = [](SplitLayoutId a, SplitLayoutId b) {
    auto typeA = a.getDatatype();
    auto typeB = b.getDatatype();
    if (ad_utility::contains(SplitLayoutId::stringTypes_, typeA) &&
        ad_utility::contains(SplitLayoutId::stringTypes_, typeB)) {
      return false;
    }
    return a.getDatatype() < b.getDatatype();
  };
  ASSERT_TRUE(
      std::is_sorted(ids.begin(), ids.end(), compareByDatatypeAndIndexTypes));
}

// Test that the `Id`s of an index type are ordered like their indices. With the
// full 64-bit payload this includes the indices that have the highest bit set.
TEST_F(SplitLayoutIdTest, IndexOrdering) {
  auto testOrder = [](auto& generator, auto makeIdFromIndex,
                      auto getIndexFromId) {
    std::vector<SplitLayoutId> ids;
    addIdsFromGenerator(generator, makeIdFromIndex, ids);
    std::vector<std::invoke_result_t<decltype(getIndexFromId), SplitLayoutId> >
        indices;
    for (auto id : ids) {
      indices.push_back(std::invoke(getIndexFromId, id));
    }

    std::sort(ids.begin(), ids.end());
    std::sort(indices.begin(), indices.end());

    for (size_t i = 0; i < ids.size(); ++i) {
      ASSERT_EQ(std::invoke(getIndexFromId, ids[i]), indices[i]);
    }
  };

  testOrder(indexGenerator, VocabId, &SplitLayoutId::getVocabIndex);
  testOrder(indexGenerator, LocalVocabId, &getLocalVocabIndex);
  testOrder(indexGenerator, WordVocabId, &SplitLayoutId::getWordVocabIndex);
  testOrder(indexGenerator, &makeSecondaryVocabId,
            &SplitLayoutId::getSecondaryVocabIndex);
  testOrder(indexGenerator, TextRecordId, &SplitLayoutId::getTextRecordIndex);
  testOrder(indexGenerator, BlankNodeId, &SplitLayoutId::getBlankNodeIndex);
  testOrder(indexGenerator, &SplitLayoutId::makeFromEncodedVal,
            &SplitLayoutId::getEncodedVal);

  // The highest bit of the payload is a regular bit of the index: it makes the
  // `Id` larger and does not change the datatype.
  EXPECT_LT(VocabId((uint64_t{1} << 63) - 1), VocabId(uint64_t{1} << 63));
  EXPECT_LT(VocabId(uint64_t{1} << 63), VocabId(SplitLayoutId::maxIndex));
}

// Test that the `Id`s of `double`s are ordered like their bit patterns: first
// the positive `double`s in ascending order (followed by the NaNs), and then
// the negative ones in descending order.
TEST_F(SplitLayoutIdTest, DoubleOrdering) {
  auto ids = makeRandomDoubleIds();
  std::vector<double> doubles;
  doubles.reserve(ids.size());
  for (auto id : ids) {
    doubles.push_back(id.getDouble());
  }
  std::sort(ids.begin(), ids.end());

  // The sorting of `double`s is broken as soon as NaNs are present. We remove
  // the NaNs from the `double`s.
  ql::erase_if(doubles, [](double d) { return std::isnan(d); });
  std::sort(doubles.begin(), doubles.end());

  // When sorting `Id`s that hold doubles, the NaN values form a contiguous
  // range.
  auto beginOfNans = std::find_if(ids.begin(), ids.end(), [](const auto& id) {
    return std::isnan(id.getDouble());
  });
  auto endOfNans = std::find_if(ids.rbegin(), ids.rend(), [](const auto& id) {
                     return std::isnan(id.getDouble());
                   }).base();
  for (auto it = beginOfNans; it < endOfNans; ++it) {
    ASSERT_TRUE(std::isnan(it->getDouble()));
  }

  // The NaN values are sorted directly after positive infinity.
  ASSERT_EQ((beginOfNans - 1)->getDouble(),
            std::numeric_limits<double>::infinity());
  // Delete the NaN values without changing the order of all other types.
  ids.erase(beginOfNans, endOfNans);

  // In `ids` the negative numbers stand AFTER the positive numbers because of
  // the bit ordering. First rotate the negative numbers to the beginning.
  auto doubleIdIsNegative = [](SplitLayoutId id) {
    auto bits = absl::bit_cast<uint64_t>(id.getDouble());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };
  auto beginOfNegatives =
      std::find_if(ids.begin(), ids.end(), doubleIdIsNegative);
  auto endOfNegatives = std::rotate(ids.begin(), beginOfNegatives, ids.end());

  // The negative numbers now come before the positive numbers, but they are
  // ordered in descending instead of ascending order, reverse them.
  std::reverse(ids.begin(), endOfNegatives);

  // After these two transformations (switch positive and negative range,
  // reverse negative range) the `ids` are sorted in exactly the same order as
  // the `doubles`. The `Id`s store the `double`s exactly.
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSERT_EQ(ids[i].getDouble(), doubles[i]);
  }
}

// Test that the `Id`s of integers are ordered like their bit patterns: first
// the non-negative integers and then the negative ones, both in ascending
// order.
TEST_F(SplitLayoutIdTest, SignedIntegerOrdering) {
  std::vector<SplitLayoutId> ids;
  addIdsFromGenerator(integerGenerator, &SplitLayoutId::makeFromInt, ids);
  std::vector<int64_t> integers;
  integers.reserve(ids.size());
  for (auto id : ids) {
    integers.push_back(id.getInt());
  }

  std::sort(ids.begin(), ids.end());
  std::sort(integers.begin(), integers.end());

  // The negative integers stand after the positive integers, so we have to
  // switch these ranges.
  auto beginOfNegative = std::find_if(
      ids.begin(), ids.end(), [](SplitLayoutId id) { return id.getInt() < 0; });
  std::rotate(ids.begin(), beginOfNegative, ids.end());

  // Now `integers` and `ids` should be in the same order
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSERT_EQ(ids[i].getInt(), integers[i]);
  }
}

// Test that an `Id` is serialized by copying its bytes, and that it is the same
// `Id` again after the deserialization.
TEST_F(SplitLayoutIdTest, Serialization) {
  auto serializedBytes = [](SplitLayoutId id) {
    ad_utility::serialization::ByteBufferWriteSerializer writer;
    writer << id;
    return std::move(writer).data();
  };

  for (auto id : makeRandomIds()) {
    auto bytes = serializedBytes(id);
    // The `Id` is serialized by copying its bytes, including the padding.
    EXPECT_EQ(bytes.size(), sizeof(SplitLayoutId));
    ad_utility::serialization::ByteBufferReadSerializer reader{
        std::move(bytes)};
    SplitLayoutId deserialized;
    reader >> deserialized;
    ASSERT_EQ(id, deserialized);
    ASSERT_EQ(id.getBits(), deserialized.getBits());
  }

  // The serialization is deterministic: `Id`s with the same bits have the same
  // bytes, no matter how they were created (the padding is always zero).
  for (auto id :
       {SplitLayoutId::makeFromInt(-42), VocabId(7),
        SplitLayoutId::makeFromDouble(1.5), SplitLayoutId::makeUndefined()}) {
    EXPECT_EQ(serializedBytes(id),
              serializedBytes(SplitLayoutId::fromBits(id.getBits())));
    EXPECT_EQ(serializedBytes(id), serializedBytes(SplitLayoutId::fromBits(
                                       id.getDatatype(), id.getPayloadBits())));
  }
}

// Test that a `HashSet` of `Id`s contains each `Id` exactly once.
TEST_F(SplitLayoutIdTest, HashingInHashSet) {
  auto ids = makeRandomIds();
  ad_utility::HashSet<SplitLayoutId> idSet(ids.begin(), ids.end());
  std::vector<SplitLayoutId> idsFromSet(idSet.begin(), idSet.end());

  std::sort(idsFromSet.begin(), idsFromSet.end());
  std::sort(ids.begin(), ids.end());
  ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
  ASSERT_EQ(ids, idsFromSet);
}

// Test that the hash of an `Id` is consistent with `operator==`, also for an
// `Id` of type `LocalVocabIndex`, which is hashed by its word.
TEST_F(SplitLayoutIdTest, HashingIsConsistentWithEquality) {
  using namespace ad_utility::triple_component;
  using namespace ad_utility::testing;
  const Index& index = qec_->getIndex();
  auto mkId = makeGetId(index);
  LocalVocab lv1;
  LocalVocab lv2;
  Iri iri = Iri::fromIriref("<foo>");
  LocalVocabEntry lve1(iri, index.getLocalVocabContext());
  LocalVocabEntry lve2(iri, index.getLocalVocabContext());
  LocalVocabEntry lve3 = LocalVocabEntry::fromStringRepresentation(
      "\"foo\"", index.getLocalVocabContext());
  LocalVocabEntry lve4 =
      LocalVocabEntry::fromIriref("<x>", index.getLocalVocabContext());
  auto LVID = [](LocalVocabEntry& lve, LocalVocab& lv) {
    return SplitLayoutId::makeFromLocalVocabIndex(
        lv.getIndexAndAddIfNotContained(lve));
  };
  // Checks that hashing is implemented correctly using `==` for equality. The
  // hash expansion is the values added with `combine`.
  // - If two elements are equal, then their hash expansions must be equal.
  // - If two elements are not equal, then hash expansions must differ and
  // neither can be a suffix of the other.
  // NOTE: `Undefined`, `Bool(false)`, `Int(0)` and `Double(0)` have the same
  // payload, so their hashes must take the datatype into account.
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(
      {LVID(lve1, lv1), LVID(lve2, lv2), LVID(lve3, lv1), LVID(lve4, lv1),
       mkId("<x>"), SplitLayoutId::makeFromInt(0),
       SplitLayoutId::makeFromInt(42), SplitLayoutId::makeFromDouble(0),
       SplitLayoutId::makeFromDouble(1.56),
       SplitLayoutId::makeFromDouble(1e-10),
       SplitLayoutId::makeFromDouble(1e+100), SplitLayoutId::makeFromBool(true),
       SplitLayoutId::makeFromBool(false), SplitLayoutId::makeUndefined()}));
}

// Test the human-readable representation of an `Id` (see `operator<<`), for
// all datatypes.
TEST_F(SplitLayoutIdTest, toDebugString) {
  auto test = [](SplitLayoutId id, std::string_view expected) {
    std::stringstream stream;
    stream << id;
    ASSERT_EQ(stream.str(), expected);
  };
  test(SplitLayoutId::makeUndefined(), "U:0");
  // Values with type undefined can usually only have one value (all data bits
  // zero). Sometimes `Id`s with type undefined but non-zero data bits are
  // used. The following test tests some of these internal `Id`s.
  test(SplitLayoutId::fromBits(Datatype::Undefined, 100), "U:100");
  test(SplitLayoutId::fromBits(uint64_t{100}), "U:100");
  test(SplitLayoutId::makeFromDouble(42.0), "D:42.000000");
  test(SplitLayoutId::makeFromInt(-42), "I:-42");
  test(SplitLayoutId::makeFromBool(false), "B:false");
  test(SplitLayoutId::makeFromBool(true), "B:true");
  test(SplitLayoutId::makeBoolFromZeroOrOne(false), "B:false");
  test(SplitLayoutId::makeBoolFromZeroOrOne(true), "B:true");
  test(VocabId(15), "V:15");
  auto str = LocalVocabEntry::literalWithoutQuotes(
      "SomeValue", qec_->getLocalVocabContext());
  test(SplitLayoutId::makeFromLocalVocabIndex(&str), "L:\"SomeValue\"");
  test(TextRecordId(37), "T:37");
  test(WordVocabId(42), "W:42");
  test(BlankNodeId(27), "B:27");
  test(SplitLayoutId::makeFromEncodedVal(12), "E:12");
  test(makeSecondaryVocabId(13), "S:13");
  test(SplitLayoutId::makeFromDate(
           DateYearOrDuration{123456, DateYearOrDuration::Type::Year}),
       "D:123456");
  test(SplitLayoutId::makeFromGeoPoint(GeoPoint{50.0, 50.0}),
       "G:POINT(50.000000 50.000000)");
  // The payload is printed as an unsigned number, also if it uses all bits.
  test(VocabId(SplitLayoutId::maxIndex), "V:18446744073709551615");
  // The largest `Id` has a datatype that does not exist, which neither the
  // representation nor `visit` accept.
  ASSERT_ANY_THROW(test(SplitLayoutId::max(), "blim"));
  ASSERT_ANY_THROW(SplitLayoutId::max().visit([](const auto&) {}));
}

// Test that an `Id` is trivially copyable.
TEST_F(SplitLayoutIdTest, TriviallyCopyable) {
  static_assert(std::is_trivially_copyable_v<SplitLayoutId>);
}

// Test that the bytes of an `Id` are completely determined by its payload and
// datatype, because the raw bytes are serialized. The split layout stores the
// payload (8 bytes) and the datatype (1 byte) next to explicit padding bytes.
TEST_F(SplitLayoutIdTest, byteRepresentation) {
  // 8 bytes payload + 1 byte datatype + 7 bytes explicit padding.
  static_assert(sizeof(SplitLayoutId) == 16);
  // The compiler doesn't insert any further (uninitialized) padding.
  static_assert(std::has_unique_object_representations_v<SplitLayoutId>);

  auto bytesEqual = [](SplitLayoutId a, SplitLayoutId b) {
    return std::memcmp(&a, &b, sizeof(SplitLayoutId)) == 0;
  };
  // The same `Id`, created in different ways, has the same bytes.
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeFromInt(5),
                         SplitLayoutId::fromBits(Datatype::Int, 5)));
  EXPECT_TRUE(bytesEqual(
      SplitLayoutId::makeFromInt(5),
      SplitLayoutId::fromBits(static_cast<uint8_t>(Datatype::Int), 5)));
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeFromInt(5),
                         SplitLayoutId::fromBits(makeBits(Datatype::Int, 5))));
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeFromBool(true),
                         SplitLayoutId::fromBits(Datatype::Bool, 1)));
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeUndefined(),
                         SplitLayoutId::fromBits(uint64_t{0})));
  EXPECT_TRUE(bytesEqual(SplitLayoutId::makeFromInt(5).incremented(),
                         SplitLayoutId::makeFromInt(6)));
  EXPECT_TRUE(
      bytesEqual(VocabId(SplitLayoutId::maxIndex).incremented(),
                 SplitLayoutId::fromBits(Datatype::LocalVocabIndex, 0)));
  for (auto id : makeRandomIds()) {
    EXPECT_TRUE(bytesEqual(id, SplitLayoutId::fromBits(id.getBits())));
  }
  // Different `Id`s have different bytes, in particular if only the datatype
  // or only the payload differs.
  EXPECT_FALSE(bytesEqual(SplitLayoutId::makeFromInt(0),
                          SplitLayoutId::makeFromDouble(0.0)));
  EXPECT_FALSE(
      bytesEqual(SplitLayoutId::makeFromInt(0), SplitLayoutId::makeFromInt(1)));
}

// Test that `incremented` returns the next bit pattern, that is, the smallest
// `Id` that is greater than the original one, with the overflow of the payload
// carrying into the datatype.
TEST_F(SplitLayoutIdTest, incremented) {
  // Without an overflow, only the payload changes.
  EXPECT_EQ(VocabId(17).incremented(), VocabId(18));
  EXPECT_EQ(VocabId(17).incremented().getDatatype(), Datatype::VocabIndex);
  EXPECT_EQ(makeSecondaryVocabId(0).incremented(), makeSecondaryVocabId(1));
  EXPECT_EQ(BlankNodeId(0).incremented(), BlankNodeId(1));
  EXPECT_EQ(SplitLayoutId::makeFromInt(13).incremented(),
            SplitLayoutId::makeFromInt(14));
  EXPECT_EQ(SplitLayoutId::makeFromInt(41).incremented(),
            SplitLayoutId::makeFromInt(42));
  EXPECT_EQ(SplitLayoutId::makeFromInt(42).incremented(),
            SplitLayoutId::makeFromInt(43));

  // The `Id` after UNDEF has the datatype `Undefined` too, but it is not UNDEF
  // (its payload is 1). The join algorithms use it as the end of the range of
  // the UNDEF values, see `findSmallerUndefRanges...`.
  const auto afterUndefined = SplitLayoutId::makeUndefined().incremented();
  EXPECT_EQ(afterUndefined.getDatatype(), Datatype::Undefined);
  EXPECT_FALSE(afterUndefined.isUndefined());
  EXPECT_EQ(afterUndefined.getBits(), makeBits(Datatype::Undefined, 1));

  // The largest payload of a datatype carries into the next datatype, with the
  // payload 0.
  const auto nextDatatype =
      static_cast<Datatype>(static_cast<size_t>(Datatype::VocabIndex) + 1);
  const auto carried = VocabId(SplitLayoutId::maxIndex).incremented();
  EXPECT_EQ(carried.getDatatype(), nextDatatype);
  EXPECT_EQ(carried.getBits(), makeBits(nextDatatype, 0));
  // The integer -1 has all payload bits set, so the increment is the first
  // `Id` of the next datatype `Double`, which is the `Id` of the double 0.0.
  EXPECT_EQ(SplitLayoutId::makeFromInt(-1).incremented(),
            SplitLayoutId::makeFromDouble(0.0));
  EXPECT_EQ(SplitLayoutId::makeFromInt(std::numeric_limits<uint64_t>::max())
                .incremented(),
            SplitLayoutId::makeFromDouble(0.0));

  // The result is the smallest greater `Id`: Its bits are the successor of the
  // bits, so no other `Id` lies between the two.
  for (const SplitLayoutId id :
       {SplitLayoutId::makeUndefined(), SplitLayoutId::makeFromBool(true),
        SplitLayoutId::makeFromInt(-42), SplitLayoutId::makeFromDouble(1.5),
        VocabId(5), TextRecordId(3), WordVocabId(4), BlankNodeId(7),
        makeSecondaryVocabId(9), SplitLayoutId::makeFromEncodedVal(11)}) {
    SCOPED_TRACE(id);
    const auto next = id.incremented();
    EXPECT_EQ(next.getBits(), id.getBits().incremented());
    EXPECT_LT(id, next);
  }

  // There is no greater `Id` than the maximum. The increment wraps around, like
  // the integer of the bits.
  EXPECT_EQ(SplitLayoutId::max().incremented().getBits(),
            SplitLayoutId::min().getBits());
}

// Pin down that the `SplitLayoutId` functions that can be evaluated at compile
// time actually are `constexpr`. Note that several of them contain an
// `AD_CONTRACT_CHECK`/`AD_EXPENSIVE_CHECK`, which is only possible because
// those macros are `constexpr`-friendly, see the note on `constexpr` in
// `util/Exception.h`.
// NOTE: The functions that are only `QL_CONSTEXPR` (`constexpr` in C++20 mode
// only) are excluded in C++17 mode, see the notes in `SplitLayoutId.h`.
namespace constexprSplitLayoutId {
static_assert(SplitLayoutId::makeUndefined().getDatatype() ==
              Datatype::Undefined);
static_assert(SplitLayoutId::makeFromBool(true).getBool());
static_assert(!SplitLayoutId::makeBoolFromZeroOrOne(false).getBool());
static_assert(SplitLayoutId::makeFromInt(42).getDatatype() == Datatype::Int);
static_assert(SplitLayoutId::makeFromVocabIndex(VocabIndex::make(17))
                  .getVocabIndex() == VocabIndex::make(17));
static_assert(SplitLayoutId::makeFromEncodedVal(17).getEncodedVal() == 17);
static_assert(SplitLayoutId::makeFromTextRecordIndex(TextRecordIndex::make(17))
                  .getTextRecordIndex() == TextRecordIndex::make(17));
static_assert(SplitLayoutId::makeFromWordVocabIndex(WordVocabIndex::make(17))
                  .getWordVocabIndex() == WordVocabIndex::make(17));
static_assert(SplitLayoutId::makeFromBlankNodeIndex(BlankNodeIndex::make(17))
                  .getBlankNodeIndex() == BlankNodeIndex::make(17));
static_assert(
    SplitLayoutId::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(17))
        .getSecondaryVocabIndex() == SecondaryVocabIndex::make(17));
static_assert(SplitLayoutId::makeFromBool(true).getBoolLiteral() == "true");
static_assert(SplitLayoutId::makeBoolFromZeroOrOne(true).getBoolLiteral() ==
              "1");
static_assert(SplitLayoutId::makeFromVocabIndex(VocabIndex::make(17))
                  .incremented()
                  .getVocabIndex() == VocabIndex::make(18));
static_assert(SplitLayoutId::min().getBits() ==
              makeBits(Datatype::Undefined, 0));
static_assert(SplitLayoutId::makeFromInt(5).getPayloadBits() == 5);
static_assert(SplitLayoutId::makeFromInt(5).getDatatypeBits() ==
              static_cast<uint8_t>(Datatype::Int));
// The overloads of `fromBits` and the lambda `fromBitsL`.
static_assert(SplitLayoutId::fromBits(makeBits(Datatype::Int, 5)).getBits() ==
              SplitLayoutId::makeFromInt(5).getBits());
static_assert(SplitLayoutId::fromBits(static_cast<uint8_t>(Datatype::Int), 5)
                  .getBits() == SplitLayoutId::makeFromInt(5).getBits());
static_assert(SplitLayoutId::fromBits(Datatype::Int, 5).getBits() ==
              SplitLayoutId::makeFromInt(5).getBits());
static_assert(SplitLayoutId::fromBitsL(SplitLayoutId::makeFromInt(5).getBits())
                  .getBits() == SplitLayoutId::makeFromInt(5).getBits());
#ifndef QLEVER_CPP_17
// `getInt` is only a constant expression since C++20.
static_assert(SplitLayoutId::makeFromInt(-42).getInt() == -42);
static_assert(SplitLayoutId::fromBits(SplitLayoutId::makeFromInt(42).getBits())
                  .getInt() == 42);
static_assert(SplitLayoutId::fromBits(makeBits(Datatype::Int, 5)).getInt() ==
              5);
static_assert(SplitLayoutId::fromBits(static_cast<uint8_t>(Datatype::Int), 5)
                  .getInt() == 5);
static_assert(SplitLayoutId::fromBits(Datatype::Int, 5).getInt() == 5);
static_assert(SplitLayoutId::fromBitsL(SplitLayoutId::makeFromInt(5).getBits())
                  .getInt() == 5);
static_assert(SplitLayoutId::makeUndefined().isTrivial());
static_assert(SplitLayoutId::makeUndefined().isUndefined());
static_assert(!SplitLayoutId::makeFromBool(true).isUndefined());
static_assert(SplitLayoutId::makeFromDouble(0.5).getDouble() == 0.5);
// The `compareWithoutLocalVocab` contains two `AD_EXPENSIVE_CHECK`s.
static_assert(SplitLayoutId::makeFromBool(true).compareWithoutLocalVocab(
                  SplitLayoutId::makeUndefined()) > 0);
static_assert(SplitLayoutId::makeUndefined().compareWithoutLocalVocab(
                  SplitLayoutId::makeUndefined()) == 0);
#endif
}  // namespace constexprSplitLayoutId

// Test that an `Id` of an encoded IRI compares equal to an `Id` of a
// `LocalVocabEntry` with the same IRI, and that they are ordered consistently.
TEST_F(SplitLayoutIdTest, EncodedIriEqualityWithLocalVocabEntry) {
  // Create an EncodedIriManager with some test prefixes
  std::vector<std::string> prefixes = {"http://example.org/",
                                       "http://test.com/"};

  // Create a test index config with the encoded IRI manager and call getQec
  // to set up the global index state
  using namespace ad_utility::testing;
  TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = prefixes;
  qec_ = getQec(config);
  const auto& encodedIriManager = qec_->getIndex().encodedIriManager();

  // Test case 1: IRI that can be encoded
  std::string encodableIri = "<http://example.org/123>";
  auto encodedIdOpt = encodedIriManager.encode(encodableIri);
  ASSERT_TRUE(encodedIdOpt.has_value())
      << "Failed to encode IRI: " << encodableIri;

  auto encodedId = *encodedIdOpt;
  EXPECT_EQ(encodedId.getDatatype(), Datatype::EncodedVal);

  // Create a LocalVocabEntry with the same IRI
  auto iri = ad_utility::triple_component::Iri::fromIriref(encodableIri);
  LocalVocabEntry localVocabEntry{iri, qec_->getLocalVocabContext()};
  auto localVocabId = SplitLayoutId::makeFromLocalVocabIndex(&localVocabEntry);

  // The encoded ID should compare equal to the LocalVocabEntry ID
  EXPECT_EQ(encodedId, localVocabId)
      << "Encoded ID should equal LocalVocabEntry ID for IRI: " << encodableIri;

  // Test case 2: Another encodable IRI with different prefix
  std::string encodableIri2 = "<http://test.com/456>";
  auto encodedIdOpt2 = encodedIriManager.encode(encodableIri2);
  ASSERT_TRUE(encodedIdOpt2.has_value())
      << "Failed to encode IRI: " << encodableIri2;

  auto encodedId2 = *encodedIdOpt2;
  auto iri2 = ad_utility::triple_component::Iri::fromIriref(encodableIri2);
  LocalVocabEntry localVocabEntry2{iri2, qec_->getLocalVocabContext()};
  auto localVocabId2 =
      SplitLayoutId::makeFromLocalVocabIndex(&localVocabEntry2);

  EXPECT_EQ(encodedId2, localVocabId2)
      << "Encoded ID should equal LocalVocabEntry ID for IRI: "
      << encodableIri2;

  // Test case 3: Encoded IDs should not equal LocalVocabEntries with different
  // IRIs
  EXPECT_NE(encodedId, localVocabId2)
      << "Different encoded IRIs should not be equal";
  EXPECT_NE(encodedId2, localVocabId)
      << "Different encoded IRIs should not be equal";

  // Test case 4: Ordering should also work correctly

  auto inconsistentOrderingMessage =
      "Ordering should be consistent between encoded and local vocab IDs";
  if (encodableIri < encodableIri2) {
    EXPECT_LT(encodedId, localVocabId2) << inconsistentOrderingMessage;
    EXPECT_GT(localVocabId2, encodedId) << inconsistentOrderingMessage;
  } else {
    EXPECT_GT(encodedId, localVocabId2) << inconsistentOrderingMessage;
    EXPECT_LT(localVocabId2, encodedId) << inconsistentOrderingMessage;
  }
}

// Test `isTrivial` for a trivial and two non-trivial datatypes. It is also
// tested by `static_assert`s across the codebase, so it is not tested
// exhaustively here.
TEST(SplitLayoutId, isTrivial) {
  EXPECT_TRUE(SplitLayoutId::makeUndefined().isTrivial());
  EXPECT_FALSE(SplitLayoutId::makeFromBlankNodeIndex(BlankNodeIndex::make(17))
                   .isTrivial());
  EXPECT_FALSE(SplitLayoutId::makeFromEncodedVal(738).isTrivial());
}

// Test that the `Id`s of all datatypes except `LocalVocabIndex` can be compared
// bitwise.
TEST(SplitLayoutId, canBeComparedBitwise) {
  EXPECT_TRUE(SplitLayoutId::makeUndefined().canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromBool(true).canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromInt(1337).canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromDouble(3.14).canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromVocabIndex(VocabIndex::make(0))
                  .canBeComparedBitwise());
  EXPECT_FALSE(
      SplitLayoutId::makeFromLocalVocabIndex(nullptr).canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromTextRecordIndex(TextRecordIndex::make(0))
                  .canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromDate(DateYearOrDuration{Date{0, 0, 0}})
                  .canBeComparedBitwise());
  EXPECT_TRUE(
      SplitLayoutId::makeFromGeoPoint(GeoPoint{0, 0}).canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromWordVocabIndex(WordVocabIndex::make(0))
                  .canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromBlankNodeIndex(BlankNodeIndex::make(17))
                  .canBeComparedBitwise());
  EXPECT_TRUE(SplitLayoutId::makeFromEncodedVal(738).canBeComparedBitwise());
}

// Test that `compareWithoutLocalVocab` yields the same order as
// `compareThreeWay` for `Id`s of all datatypes except `LocalVocabIndex`, and
// that it rejects an `Id` of type `LocalVocabIndex` if the expensive checks are
// enabled.
TEST_F(SplitLayoutIdTest, compareWithoutLocalVocab) {
  auto ids = makeRandomIds();
  ql::erase_if(ids, [](SplitLayoutId id) {
    return id.getDatatype() == Datatype::LocalVocabIndex;
  });
  ad_utility::SlowRandomIntGenerator<size_t> indexOfId(0, ids.size() - 1);
  for (size_t i = 0; i < numRandomChecks; ++i) {
    auto a = ids.at(indexOfId());
    auto b = ids.at(indexOfId());
    EXPECT_TRUE(a.compareWithoutLocalVocab(b) == a.compareThreeWay(b))
        << a << ' ' << b;
  }
  EXPECT_TRUE(SplitLayoutId::makeFromInt(-1).compareWithoutLocalVocab(
                  SplitLayoutId::makeFromDouble(0.0)) < 0);
  EXPECT_TRUE(VocabId(3).compareWithoutLocalVocab(VocabId(3)) == 0);

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    auto localVocabId = LocalVocabId(3);
    EXPECT_ANY_THROW(localVocabId.compareWithoutLocalVocab(VocabId(3)));
    EXPECT_ANY_THROW(VocabId(3).compareWithoutLocalVocab(localVocabId));
  }
}

// Test all the branches of `compareThreeWay` in which an `Id` of type
// `LocalVocabIndex` is compared to an `Id` of another datatype.
TEST(SplitLayoutId, compareThreeWayWithLocalVocabIndex) {
  using namespace ad_utility::testing;
  // Use a fresh index (and not the shared one of `getQec`), because the
  // secondary vocabulary must not leak into other tests.
  TestIndexConfig config{"<a> <b> <c> ."};
  config.secondaryVocabWords = std::vector<std::string>{"<zzz>"};
  Index index = makeTestIndex(gtestCurrentTestName(), std::move(config));
  auto mkId = makeGetId(index);
  const auto& ctx = index.getLocalVocabContext();

  // `<b>` is stored in the vocabulary of the main index, so the position of
  // `entryInVocab` is of type `VocabIndex`, and `<zzz>` is stored in the
  // secondary vocabulary, so the position of `entryInSecondaryVocab` is of type
  // `SecondaryVocabIndex`.
  LocalVocabEntry entryInVocab = LocalVocabEntry::fromIriref("<b>", ctx);
  LocalVocabEntry entryInSecondaryVocab =
      LocalVocabEntry::fromIriref("<zzz>", ctx);
  SplitLayoutId localVocabId =
      SplitLayoutId::makeFromLocalVocabIndex(&entryInVocab);
  SplitLayoutId localVocabIdSecondary =
      SplitLayoutId::makeFromLocalVocabIndex(&entryInSecondaryVocab);
  SplitLayoutId secondaryVocabId =
      SplitLayoutId::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(0));
  // `Int` is a datatype that is smaller than `LocalVocabIndex` and `Date` is
  // one that is greater, and neither of them is a datatype that a position in
  // the vocabularies can have.
  SplitLayoutId intId = SplitLayoutId::makeFromInt(42);
  SplitLayoutId dateId =
      SplitLayoutId::makeFromDate(DateYearOrDuration{Date{2026, 8, 19}});

  // `isDatatypeOfPositionInVocab(type) == true` and
  // `otherType == LocalVocabIndex`: the position of the entry is compared to
  // the left-hand `Id`.
  EXPECT_LT(mkId("<a>"), localVocabId);
  EXPECT_EQ(mkId("<b>"), localVocabId);
  EXPECT_GT(mkId("<c>"), localVocabId);
  // The same combination, but with `type == Datatype::SecondaryVocabIndex`.
  // Note that every word of the secondary vocabulary is positioned after every
  // word of the vocabulary of the main index.
  EXPECT_EQ(secondaryVocabId, localVocabIdSecondary);
  EXPECT_GT(secondaryVocabId, localVocabId);
  // NOTE: The combination `isDatatypeOfPositionInVocab(type) == true` and
  // `otherType != LocalVocabIndex` is unreachable. This point is only reached
  // if exactly one of the two datatypes is `LocalVocabIndex`, so
  // `otherType != LocalVocabIndex` implies `type == LocalVocabIndex`, which is
  // none of the datatypes that a position can have.

  // `isDatatypeOfPositionInVocab(type) == false`, but
  // `otherType == LocalVocabIndex`: the position is not looked up at all, and
  // the bits are compared instead.
  EXPECT_LT(intId, localVocabId);
  EXPECT_GT(dateId, localVocabId);

  // `isDatatypeOfPositionInVocab(type) == false` and
  // `otherType != LocalVocabIndex`, because `type == LocalVocabIndex`. This is
  // the mirrored case, which the second condition of `compareThreeWay` handles.
  // There, `type == LocalVocabIndex` is true and
  // `isDatatypeOfPositionInVocab(otherType)` is true as well.
  EXPECT_GT(localVocabId, mkId("<a>"));
  EXPECT_EQ(localVocabId, mkId("<b>"));
  EXPECT_LT(localVocabId, mkId("<c>"));
  EXPECT_EQ(localVocabIdSecondary, secondaryVocabId);
  EXPECT_LT(localVocabId, secondaryVocabId);
  // Both operands of the first condition are false, and in the second condition
  // `type == LocalVocabIndex` is true, but
  // `isDatatypeOfPositionInVocab(otherType)` is false, so the bits are
  // compared.
  EXPECT_GT(localVocabId, intId);
  EXPECT_LT(localVocabId, dateId);
  // NOTE: For the second condition, the combination
  // `type != LocalVocabIndex` and `isDatatypeOfPositionInVocab(otherType) ==
  // true` is unreachable by the same argument as above: `type !=
  // LocalVocabIndex` implies `otherType == LocalVocabIndex`, which is none of
  // the datatypes that a position can have. Its remaining combination
  // (`type != LocalVocabIndex` and `otherType == LocalVocabIndex`) is the case
  // of `intId` and `dateId` above.
}

// Test that the forwarding lambdas `Id::isUndefinedL`, `Id::isDefinedL`,
// `Id::getBitsL` and `Id::getDatatypeL` behave like the member functions they
// forward to, both for an `Id` and for a proxy type that is not an `Id` but
// provides the same member functions.
TEST(SplitLayoutId, forwardingLambdas) {
  // A proxy type with the member functions, like the elements of a column view
  // that does not store `Id`s.
  struct Proxy {
    SplitLayoutId id_;
    bool isUndefined() const { return id_.isUndefined(); }
    SplitLayoutId::T getBits() const { return id_.getBits(); }
    Datatype getDatatype() const { return id_.getDatatype(); }
  };

  // Each lambda returns what the corresponding member function returns.
  for (SplitLayoutId id :
       {SplitLayoutId::makeUndefined(), SplitLayoutId::makeFromInt(42),
        SplitLayoutId::makeFromInt(-42), SplitLayoutId::makeFromDouble(13.37),
        SplitLayoutId::makeFromBool(true)}) {
    EXPECT_EQ(SplitLayoutId::isUndefinedL(id), id.isUndefined());
    EXPECT_EQ(SplitLayoutId::isDefinedL(id), !id.isUndefined());
    EXPECT_EQ(SplitLayoutId::getBitsL(id), id.getBits());
    EXPECT_EQ(SplitLayoutId::getDatatypeL(id), id.getDatatype());
    EXPECT_EQ(SplitLayoutId::isUndefinedL(Proxy{id}), id.isUndefined());
    EXPECT_EQ(SplitLayoutId::isDefinedL(Proxy{id}), !id.isUndefined());
    EXPECT_EQ(SplitLayoutId::getBitsL(Proxy{id}), id.getBits());
    EXPECT_EQ(SplitLayoutId::getDatatypeL(Proxy{id}), id.getDatatype());
  }

  // The lambdas work as predicates and projections of generic algorithms, also
  // over a range of proxies.
  std::vector ids{SplitLayoutId::makeFromInt(1), SplitLayoutId::makeUndefined(),
                  SplitLayoutId::makeFromDouble(3.5)};
  std::vector<Proxy> proxies{{ids[0]}, {ids[1]}, {ids[2]}};
  EXPECT_TRUE(ql::ranges::any_of(ids, SplitLayoutId::isUndefinedL));
  EXPECT_TRUE(ql::ranges::any_of(proxies, SplitLayoutId::isUndefinedL));
  EXPECT_EQ(
      ql::ranges::find(ids, Datatype::Double, SplitLayoutId::getDatatypeL),
      ids.begin() + 2);
  EXPECT_EQ(
      ql::ranges::find(proxies, Datatype::Double, SplitLayoutId::getDatatypeL),
      proxies.begin() + 2);
}

// Test that the lambda `Id::fromBitsL` forwards to the `fromBits` overload that
// takes all bits at once. In contrast to `fromBits`, which is overloaded, it
// can be passed to generic algorithms.
TEST(SplitLayoutId, fromBitsL) {
  std::vector<SplitLayoutId> ids{
      SplitLayoutId::makeUndefined(),
      SplitLayoutId::makeFromBool(true),
      SplitLayoutId::makeFromInt(42),
      SplitLayoutId::makeFromInt(-42),
      SplitLayoutId::makeFromDouble(13.37),
      SplitLayoutId::makeFromVocabIndex(VocabIndex::make(123)),
      SplitLayoutId::makeFromBlankNodeIndex(BlankNodeIndex::make(7))};

  // The lambda returns what `fromBits` returns, and is the inverse of
  // `getBitsL`.
  for (SplitLayoutId id : ids) {
    EXPECT_EQ(SplitLayoutId::fromBitsL(id.getBits()).getBits(),
              SplitLayoutId::fromBits(id.getBits()).getBits());
    EXPECT_EQ(SplitLayoutId::fromBitsL(SplitLayoutId::getBitsL(id)), id);
  }

  // The lambda works as the transformation of a generic algorithm.
  std::vector<SplitLayoutId::T> bits;
  ql::ranges::transform(ids, std::back_inserter(bits), SplitLayoutId::getBitsL);
  std::vector<SplitLayoutId> rebuilt(bits.size());
  ql::ranges::transform(bits, rebuilt.begin(), SplitLayoutId::fromBitsL);
  EXPECT_EQ(rebuilt, ids);
}
}  // namespace columnBasedIdTable::splitLayout
