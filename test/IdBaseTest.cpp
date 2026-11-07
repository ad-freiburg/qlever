// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for everything that `IdBase` implements, for the `Id` of the current
// build: the legacy `ValueId`, or the `SplitLayoutId` if `USE_SPLIT_LAYOUT` is
// set. Where the two layouts differ
// (the number of payload bits), the expectations are derived from the
// constants of the `Id`. The tests that only make sense for one of the layouts
// are in `FoldedValueIdTest.cpp` and
// `engine/idTable/splitLayout/SplitLayoutIdTest.cpp`.

#include <absl/hash/hash_testing.h>
#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "./ValueIdTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "global/Id.h"
#include "index/LocalVocabEntry.h"
#include "util/BitUtils.h"
#include "util/HashSet.h"
#include "util/LocalVocabEntryTestHelpers.h"
#include "util/Random.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

using ad_utility::testing::iriLocalVocabEntry;
using ad_utility::testing::localVocabEntry;
using ad_utility::testing::localVocabEntryFromStringRepresentation;

namespace {
// The number of lowest bits of a `double` that do not fit into the payload: 4
// for the legacy `ValueId`, 0 for the `SplitLayoutId`.
constexpr int numStrippedBits = 64 - static_cast<int>(Id::numDataBits);

// A payload with all bits set: the largest index and the integer -1.
constexpr uint64_t payloadMask =
    ad_utility::bitMaskForLowerBits(Id::numDataBits);

// The number of values that the datatype bits can hold.
constexpr uint64_t numDatatypeValues = uint64_t{1} << Id::numDatatypeBits;

uint64_t getBlankNodeIndex(Id id) { return id.getBlankNodeIndex().get(); }
uint64_t getEncodedVal(Id id) { return id.getEncodedVal(); }
Id makeEncodedId(uint64_t value) { return Id::makeFromEncodedVal(value); }

struct IdBaseTest : public ::testing::Test {
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
};
}  // namespace

// Test that a `double` keeps all but the lowest `numStrippedBits` bits of its
// mantissa, and that subnormal numbers that are too small are rounded to zero.
TEST_F(IdBaseTest, makeFromDouble) {
  auto testRepresentableDouble = [](double d) {
    auto id = Id::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    ASSERT_FLOAT_EQ(id.getDouble(), d);
    // This check expresses the precision more exactly.
    if (id.getDouble() != d) {
      // The `if` is needed for the case of += infinity.
      ASSERT_NEAR(id.getDouble(), d,
                  std::abs(d / (uint64_t{1} << (52 - numStrippedBits))));
    }
  };

  auto testNonRepresentableSubnormal = [](double d) {
    auto id = Id::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // Subnormal numbers with a too small fraction are rounded to zero.
    ASSERT_EQ(id.getDouble(), 0.0);
  };
  for (size_t i = 0; i < numElements; ++i) {
    testRepresentableDouble(positiveRepresentableDoubleGenerator());
    testRepresentableDouble(negativeRepresentableDoubleGenerator());
    auto nonRepresentable = nonRepresentableDoubleGenerator();
    // The random number generator includes the edge cases which would make the
    // tests fail.
    if (nonRepresentable != Id::minPositiveDouble &&
        nonRepresentable != -Id::minPositiveDouble) {
      testNonRepresentableSubnormal(nonRepresentable);
    }
  }

  testRepresentableDouble(std::numeric_limits<double>::infinity());
  testRepresentableDouble(-std::numeric_limits<double>::infinity());

  // Test positive and negative 0.
  ASSERT_NE(absl::bit_cast<uint64_t>(0.0), absl::bit_cast<uint64_t>(-0.0));
  ASSERT_EQ(0.0, -0.0);
  testRepresentableDouble(0.0);
  testRepresentableDouble(-0.0);
  testNonRepresentableSubnormal(0.0);

  auto quietNan = std::numeric_limits<double>::quiet_NaN();
  auto signalingNan = std::numeric_limits<double>::signaling_NaN();
  ASSERT_TRUE(std::isnan(Id::makeFromDouble(quietNan).getDouble()));
  ASSERT_TRUE(std::isnan(Id::makeFromDouble(signalingNan).getDouble()));

  // Test that the value of `minPositiveDouble` is correct: it is stored
  // exactly, and half of it is rounded to zero. With stripped bits, half of it
  // is still a `double` other than zero.
  auto testSmallestNumber = [](double d) {
    ASSERT_EQ(Id::makeFromDouble(d).getDouble(), d);
    ASSERT_EQ(d / 2 != 0.0, numStrippedBits > 0);
    ASSERT_EQ(Id::makeFromDouble(d / 2).getDouble(), 0.0);
  };
  testSmallestNumber(Id::minPositiveDouble);
  testSmallestNumber(-Id::minPositiveDouble);
}

// Test that the integers of the `IntegerType` of the `Id` are stored exactly.
TEST_F(IdBaseTest, makeFromInt) {
  auto testInt = [](int64_t value) {
    auto id = Id::makeFromInt(value);
    ASSERT_EQ(id.getDatatype(), Datatype::Int);
    ASSERT_EQ(id.getInt(), value);
  };
  for (size_t i = 0; i < numElements; ++i) {
    testInt(nonOverflowingNBitGenerator());
  }
  for (int64_t value : {Id::IntegerType::min(), int64_t{-1}, int64_t{0},
                        int64_t{1}, Id::IntegerType::max()}) {
    testInt(value);
  }
}

// _____________________________________________________________________________
TEST_F(IdBaseTest, makeFromBool) {
  EXPECT_TRUE(Id::makeBoolFromZeroOrOne(true).getBool());
  EXPECT_TRUE(Id::makeFromBool(true).getBool());
  EXPECT_FALSE(Id::makeBoolFromZeroOrOne(false).getBool());
  EXPECT_FALSE(Id::makeFromBool(false).getBool());

  EXPECT_EQ(Id::makeBoolFromZeroOrOne(true).getBoolLiteral(), "1");
  EXPECT_EQ(Id::makeFromBool(true).getBoolLiteral(), "true");
  EXPECT_EQ(Id::makeBoolFromZeroOrOne(false).getBoolLiteral(), "0");
  EXPECT_EQ(Id::makeFromBool(false).getBoolLiteral(), "false");
}

// Test that the index types store every value up to `maxIndex` and return it
// unchanged, and that a larger value is rejected (there is none for the
// `SplitLayoutId`, whose `maxIndex` is the largest `uint64_t`). The
// `LocalVocabIndex` is a pointer and has no `maxIndex`.
TEST_F(IdBaseTest, Indices) {
  auto testRandomIds = [&](auto makeId, auto getFromId, Datatype type) {
    auto testSingle = [&](auto value) {
      auto id = makeId(value);
      ASSERT_EQ(id.getDatatype(), type);
      ASSERT_EQ(std::invoke(getFromId, id), value);
    };
    for (size_t idx = 0; idx < numElements; ++idx) {
      testSingle(indexGenerator());
    }
    testSingle(0);
    testSingle(Id::maxIndex);

    if (type != Datatype::LocalVocabIndex &&
        Id::maxIndex < std::numeric_limits<uint64_t>::max()) {
      for (size_t idx = 0; idx < numElements; ++idx) {
        auto value = invalidIndexGenerator();
        if (value == Id::maxIndex) {
          continue;
        }
        AD_EXPECT_THROW_WITH_MESSAGE(
            makeId(value), ::testing::ContainsRegex("is bigger than"));
      }
    }
  };

  testRandomIds(&makeTextRecordId, &getTextRecordIndex,
                Datatype::TextRecordIndex);
  testRandomIds(&makeVocabId, &getVocabIndex, Datatype::VocabIndex);
  auto localVocabWordToInt = [](const auto& input) {
    return std::strtoull(getLocalVocabIndex(input).c_str(), nullptr, 10);
  };
  testRandomIds(&makeLocalVocabId, localVocabWordToInt,
                Datatype::LocalVocabIndex);
  testRandomIds(&makeWordVocabId, &getWordVocabIndex, Datatype::WordVocabIndex);
  testRandomIds(&makeSecondaryVocabId, &getSecondaryVocabIndex,
                Datatype::SecondaryVocabIndex);
  testRandomIds(&makeBlankNodeId, &getBlankNodeIndex, Datatype::BlankNodeIndex);
  testRandomIds(&makeEncodedId, &getEncodedVal, Datatype::EncodedVal);
}

// Test `makeUndefined` and `getUndefined`.
TEST_F(IdBaseTest, Undefined) {
  auto id = Id::makeUndefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);

  // `getUndefined()` returns the single value of `UndefinedType`. Its main
  // purpose is the generic code in `visit`, which has to dispatch on the
  // datatype, so we also test it via that path.
  static_assert(std::is_same_v<decltype(id.getUndefined()), Id::UndefinedType>);
  auto isUndefinedType = [](const auto& value) {
    return std::is_same_v<std::decay_t<decltype(value)>, Id::UndefinedType>;
  };
  EXPECT_TRUE(isUndefinedType(id.getUndefined()));
  EXPECT_TRUE(id.visit(isUndefinedType));
  EXPECT_FALSE(Id::makeFromInt(42).visit(isUndefinedType));
}

// Test `getDatatypeBits` and `getPayloadBits`, the two parts of the bits of an
// `Id`, together with `fromBits(datatype, payload)` and `fromBits(bits)`, which
// put them together again.
TEST_F(IdBaseTest, datatypeBitsAndPayloadBits) {
  auto checkParts = [](Id id) {
    EXPECT_EQ(id.getDatatypeBits(), static_cast<uint8_t>(id.getDatatype()));
    EXPECT_LE(id.getPayloadBits(), payloadMask);
    auto rebuilt = Id::fromBits(id.getDatatypeBits(), id.getPayloadBits());
    EXPECT_EQ(rebuilt.getBits(), id.getBits());
    EXPECT_EQ(Id::fromBits(id.getDatatype(), id.getPayloadBits()).getBits(),
              id.getBits());
    EXPECT_EQ(Id::fromBits(id.getBits()).getBits(), id.getBits());
  };
  for (auto id : makeRandomIds()) {
    checkParts(id);
  }

  // Some known values.
  auto expectParts = [&checkParts](Id id, Datatype datatype, uint64_t payload) {
    checkParts(id);
    EXPECT_EQ(id.getDatatypeBits(), static_cast<uint8_t>(datatype));
    EXPECT_EQ(id.getPayloadBits(), payload);
    EXPECT_EQ(Id::fromBits(datatype, payload), id);
    EXPECT_EQ(Id::fromBits(static_cast<uint8_t>(datatype), payload), id);
  };
  expectParts(Id::makeUndefined(), Datatype::Undefined, 0);
  expectParts(Id::makeFromBool(false), Datatype::Bool, 0);
  expectParts(Id::makeFromBool(true), Datatype::Bool, 1);
  expectParts(Id::makeFromInt(0), Datatype::Int, 0);
  expectParts(Id::makeFromInt(42), Datatype::Int, 42);
  expectParts(makeVocabId(42), Datatype::VocabIndex, 42);
  // The largest index fills the whole payload, but does not touch the datatype
  // bits.
  expectParts(makeVocabId(Id::maxIndex), Datatype::VocabIndex, Id::maxIndex);
  // A negative integer has all payload bits set, and its sign must not leak
  // into the datatype bits.
  expectParts(Id::makeFromInt(-1), Datatype::Int, payloadMask);

  // Every value of the datatype bits can be combined with every payload, and
  // the two parts do not influence each other.
  for (uint64_t datatype = 0; datatype < numDatatypeValues; ++datatype) {
    for (uint64_t payload :
         {uint64_t{0}, uint64_t{1}, uint64_t{123456789}, payloadMask}) {
      auto id = Id::fromBits(static_cast<uint8_t>(datatype), payload);
      EXPECT_EQ(uint64_t{id.getDatatypeBits()}, datatype);
      EXPECT_EQ(id.getPayloadBits(), payload);
      // NOTE: The bits are compared and not the `Id`s, because an `Id` of type
      // `LocalVocabIndex` with an arbitrary payload is no valid pointer.
      EXPECT_EQ(
          Id::fromBits(static_cast<Datatype>(datatype), payload).getBits(),
          id.getBits());
    }
  }
}

// Test that sorted `Id`s are ordered by their datatype, except for the string
// types, whose `Id`s are ordered by their words.
TEST_F(IdBaseTest, OrderingDifferentDatatypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end());

  auto compareByDatatypeAndIndexTypes = [](Id a, Id b) {
    auto typeA = a.getDatatype();
    auto typeB = b.getDatatype();
    if (ad_utility::contains(Id::stringTypes_, typeA) &&
        ad_utility::contains(Id::stringTypes_, typeB)) {
      return false;
    }
    return a.getDatatype() < b.getDatatype();
  };
  ASSERT_TRUE(
      std::is_sorted(ids.begin(), ids.end(), compareByDatatypeAndIndexTypes));
}

// Test that the `Id`s of an index type are ordered like their indices.
TEST_F(IdBaseTest, IndexOrdering) {
  auto testOrder = [](auto makeIdFromIndex, auto getIndexFromId) {
    std::vector<Id> ids;
    addIdsFromGenerator(indexGenerator, makeIdFromIndex, ids);
    std::vector<std::invoke_result_t<decltype(getIndexFromId), Id>> indices;
    for (auto id : ids) {
      indices.push_back(std::invoke(getIndexFromId, id));
    }

    std::sort(ids.begin(), ids.end());
    std::sort(indices.begin(), indices.end());

    for (size_t i = 0; i < ids.size(); ++i) {
      ASSERT_EQ(std::invoke(getIndexFromId, ids[i]), indices[i]);
    }
  };

  testOrder(&makeVocabId, &getVocabIndex);
  testOrder(&makeLocalVocabId, &getLocalVocabIndex);
  testOrder(&makeWordVocabId, &getWordVocabIndex);
  testOrder(&makeSecondaryVocabId, &getSecondaryVocabIndex);
  testOrder(&makeTextRecordId, &getTextRecordIndex);
  testOrder(&makeBlankNodeId, &getBlankNodeIndex);
  testOrder(&makeEncodedId, &getEncodedVal);
}

// Test that the `Id`s of `double`s are ordered like their bit patterns: first
// the positive `double`s in ascending order (followed by the NaNs), and then
// the negative ones in descending order.
TEST_F(IdBaseTest, DoubleOrdering) {
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
  auto doubleIdIsNegative = [](Id id) {
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
  // the `doubles`.
  for (size_t i = 0; i < ids.size(); ++i) {
    auto doubleTruncated = Id::makeFromDouble(doubles[i]).getDouble();
    ASSERT_EQ(ids[i].getDouble(), doubleTruncated);
  }
}

// Test that the `Id`s of integers are ordered like their bit patterns: first
// the non-negative integers and then the negative ones, both in ascending
// order.
TEST_F(IdBaseTest, SignedIntegerOrdering) {
  std::vector<Id> ids;
  addIdsFromGenerator(nonOverflowingNBitGenerator, &Id::makeFromInt, ids);
  std::vector<int64_t> integers;
  integers.reserve(ids.size());
  for (auto id : ids) {
    integers.push_back(id.getInt());
  }

  std::sort(ids.begin(), ids.end());
  std::sort(integers.begin(), integers.end());

  // The negative integers stand after the positive integers, so we have to
  // switch these ranges.
  auto beginOfNegative = std::find_if(ids.begin(), ids.end(),
                                      [](Id id) { return id.getInt() < 0; });
  std::rotate(ids.begin(), beginOfNegative, ids.end());

  // Now `integers` and `ids` should be in the same order.
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSERT_EQ(ids[i].getInt(), integers[i]);
  }
}

// Test that an `Id` is the same `Id` again after the serialization.
TEST_F(IdBaseTest, Serialization) {
  for (auto id : makeRandomIds()) {
    ad_utility::serialization::ByteBufferWriteSerializer writer;
    writer << id;
    ad_utility::serialization::ByteBufferReadSerializer reader{
        std::move(writer).data()};
    Id serializedId;
    reader >> serializedId;
    ASSERT_EQ(id, serializedId);
    ASSERT_EQ(id.getBits(), serializedId.getBits());
  }
}

// Test that a `HashSet` of `Id`s contains each `Id` exactly once, and that the
// hash is consistent with `operator==`, also for an `Id` of type
// `LocalVocabIndex`, which is hashed by its word.
TEST_F(IdBaseTest, Hashing) {
  {
    auto ids = makeRandomIds();
    ad_utility::HashSet<Id> idSet(ids.begin(), ids.end());
    std::vector<Id> idsFromSet(idSet.begin(), idSet.end());

    std::sort(idsFromSet.begin(), idsFromSet.end());
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    ASSERT_EQ(ids, idsFromSet);
  }
  {
    using namespace ad_utility::triple_component;
    using namespace ad_utility::testing;
    const Index& index = qec_->getIndex();
    auto mkId = makeGetId(index);
    LocalVocab lv1;
    LocalVocab lv2;
    Iri iri = Iri::fromIriref("<foo>");
    LocalVocabEntry lve1 = localVocabEntry(iri, index.getLocalVocabContext());
    LocalVocabEntry lve2 = localVocabEntry(iri, index.getLocalVocabContext());
    LocalVocabEntry lve3 = localVocabEntryFromStringRepresentation(
        "\"foo\"", index.getLocalVocabContext());
    LocalVocabEntry lve4 =
        iriLocalVocabEntry("<x>", index.getLocalVocabContext());
    auto LVID = [](LocalVocabEntry& lve, LocalVocab& lv) {
      return Id::makeFromLocalVocabIndex(lv.getIndexAndAddIfNotContained(lve));
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
         mkId("<x>"), Id::makeFromInt(0), Id::makeFromInt(42),
         Id::makeFromDouble(0), Id::makeFromDouble(1.56),
         Id::makeFromDouble(1e-10), Id::makeFromDouble(1e+100),
         Id::makeFromBool(true), Id::makeFromBool(false),
         Id::makeUndefined()}));
  }
}

// Test the human-readable representation of an `Id` (see `operator<<`), for
// all datatypes.
TEST_F(IdBaseTest, toDebugString) {
  auto test = [](Id id, std::string_view expected) {
    std::stringstream stream;
    stream << id;
    ASSERT_EQ(stream.str(), expected);
  };
  test(Id::makeUndefined(), "U:0");
  // Values with type undefined can usually only have one value (all data bits
  // zero). Sometimes `Id`s with type undefined but non-zero data bits are used.
  // The following test tests one of these internal `Id`s.
  test(Id::fromBits(Datatype::Undefined, 100), "U:100");
  test(Id::makeFromDouble(42.0), "D:42.000000");
  test(Id::makeFromInt(-42), "I:-42");
  test(Id::makeFromBool(false), "B:false");
  test(Id::makeFromBool(true), "B:true");
  test(Id::makeBoolFromZeroOrOne(false), "B:false");
  test(Id::makeBoolFromZeroOrOne(true), "B:true");
  test(makeVocabId(15), "V:15");
  auto str = LocalVocabEntry::literalWithoutQuotes(
      "SomeValue", qec_->getLocalVocabContext());
  test(Id::makeFromLocalVocabIndex(&str), "L:\"SomeValue\"");
  test(makeTextRecordId(37), "T:37");
  test(makeWordVocabId(42), "W:42");
  test(makeBlankNodeId(27), "B:27");
  test(Id::makeFromEncodedVal(12), "E:12");
  test(makeSecondaryVocabId(13), "S:13");
  test(Id::makeFromDate(
           DateYearOrDuration{123456, DateYearOrDuration::Type::Year}),
       "D:123456");
  test(Id::makeFromGeoPoint(GeoPoint{50.0, 50.0}),
       "G:POINT(50.000000 50.000000)");
  // The payload is printed as an unsigned number, also if it uses all bits.
  test(makeVocabId(Id::maxIndex), "V:" + std::to_string(Id::maxIndex));
  // The largest `Id` has a datatype that does not exist, which neither the
  // representation nor `visit` accept.
  ASSERT_ANY_THROW(test(Id::max(), "blim"));
  ASSERT_ANY_THROW(Id::max().visit([](const auto&) {}));
}

// _____________________________________________________________________________
TEST_F(IdBaseTest, TriviallyCopyable) {
  static_assert(std::is_trivially_copyable_v<Id>);
}

// Test that `incremented` returns the smallest `Id` that is greater than the
// original one, with the overflow of the payload carrying into the datatype.
TEST_F(IdBaseTest, incremented) {
  // The `Id` after UNDEF has the datatype `Undefined` too, but it is not UNDEF
  // (its payload is 1). The join algorithms use it as the end of the range of
  // the UNDEF values, see `findSmallerUndefRanges...`.
  const auto afterUndefined = Id::makeUndefined().incremented();
  EXPECT_EQ(afterUndefined, Id::fromBits(Datatype::Undefined, 1));
  EXPECT_FALSE(afterUndefined.isUndefined());

  // The largest payload of a datatype carries into the next datatype, with the
  // payload 0.
  const auto nextDatatype =
      static_cast<Datatype>(static_cast<size_t>(Datatype::VocabIndex) + 1);
  // NOTE: The next datatype is `LocalVocabIndex`, and an `Id` of this type with
  // the payload 0 is no valid pointer, so the bits are compared.
  EXPECT_EQ(makeVocabId(Id::maxIndex).incremented().getBits(),
            Id::fromBits(nextDatatype, 0).getBits());
  // The integer -1 has all payload bits set, so the increment is the first `Id`
  // of the next datatype `Double`, which is the `Id` of the double 0.0.
  EXPECT_EQ(Id::makeFromInt(-1).incremented(), Id::makeFromDouble(0.0));

  // The result is the smallest greater `Id`: Its payload is the successor of
  // the payload, so no other `Id` lies between the two.
  for (const Id id :
       {Id::makeUndefined(), Id::makeFromBool(true), Id::makeFromInt(-42),
        Id::makeFromDouble(1.5), makeVocabId(5), makeTextRecordId(3),
        makeWordVocabId(4), makeBlankNodeId(7), makeSecondaryVocabId(9),
        Id::makeFromEncodedVal(11)}) {
    SCOPED_TRACE(id);
    const auto next = id.incremented();
    EXPECT_EQ(next, Id::fromBits(id.getDatatype(), id.getPayloadBits() + 1));
    EXPECT_LT(id, next);
  }

  // There is no greater `Id` than the maximum. The increment wraps around, like
  // the integer of the bits.
  EXPECT_EQ(Id::max().incremented().getBits(), Id::min().getBits());
}

// Pin down that the `Id` functions that can be evaluated at compile time
// actually are `constexpr`. Note that several of them contain an
// `AD_CONTRACT_CHECK`/`AD_EXPENSIVE_CHECK`, which is only possible because
// those macros are `constexpr`-friendly, see the note on `constexpr` in
// `util/Exception.h`.
// NOTE: The functions that are only `QL_CONSTEXPR` (`constexpr` in C++20 mode
// only) are excluded in C++17 mode, see the notes in `global/IdBase.h`.
namespace constexprId {
static_assert(Id::makeUndefined().getDatatype() == Datatype::Undefined);
static_assert(Id::makeFromBool(true).getBool());
static_assert(!Id::makeBoolFromZeroOrOne(false).getBool());
static_assert(Id::makeFromInt(42).getDatatype() == Datatype::Int);
static_assert(Id::makeFromVocabIndex(VocabIndex::make(17)).getVocabIndex() ==
              VocabIndex::make(17));
static_assert(Id::makeFromEncodedVal(17).getEncodedVal() == 17);
static_assert(Id::makeFromTextRecordIndex(TextRecordIndex::make(17))
                  .getTextRecordIndex() == TextRecordIndex::make(17));
static_assert(Id::makeFromWordVocabIndex(WordVocabIndex::make(17))
                  .getWordVocabIndex() == WordVocabIndex::make(17));
static_assert(Id::makeFromBlankNodeIndex(BlankNodeIndex::make(17))
                  .getBlankNodeIndex() == BlankNodeIndex::make(17));
static_assert(Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(17))
                  .getSecondaryVocabIndex() == SecondaryVocabIndex::make(17));
static_assert(Id::makeFromBool(true).getBoolLiteral() == "true");
static_assert(Id::makeBoolFromZeroOrOne(true).getBoolLiteral() == "1");
static_assert(Id::makeFromVocabIndex(VocabIndex::make(17))
                  .incremented()
                  .getVocabIndex() == VocabIndex::make(18));
static_assert(Id::makeFromInt(5).getPayloadBits() == 5);
static_assert(Id::makeFromInt(5).getDatatypeBits() ==
              static_cast<uint8_t>(Datatype::Int));
// The overloads of `fromBits` and the lambda `fromBitsL`.
static_assert(Id::fromBits(Id::makeFromInt(5).getBits()).getBits() ==
              Id::makeFromInt(5).getBits());
static_assert(Id::fromBits(static_cast<uint8_t>(Datatype::Int), 5).getBits() ==
              Id::makeFromInt(5).getBits());
static_assert(Id::fromBits(Datatype::Int, 5).getBits() ==
              Id::makeFromInt(5).getBits());
static_assert(Id::fromBitsL(Id::makeFromInt(5).getBits()).getBits() ==
              Id::makeFromInt(5).getBits());
#ifndef QLEVER_CPP_17
// `getInt` performs a signed left shift for the legacy `ValueId`, which is only
// a constant expression since C++20.
static_assert(Id::makeFromInt(-42).getInt() == -42);
static_assert(Id::fromBits(Id::makeFromInt(42).getBits()).getInt() == 42);
static_assert(Id::makeUndefined().isTrivial());
static_assert(Id::makeUndefined().isUndefined());
static_assert(!Id::makeFromBool(true).isUndefined());
static_assert(Id::makeFromDouble(0.5).getDouble() == 0.5);
// The `compareWithoutLocalVocab` contains two `AD_EXPENSIVE_CHECK`s.
static_assert(
    Id::makeFromBool(true).compareWithoutLocalVocab(Id::makeUndefined()) > 0);
static_assert(
    Id::makeUndefined().compareWithoutLocalVocab(Id::makeUndefined()) == 0);
#endif
}  // namespace constexprId

// Note: the `isTrivial` functionality is also tested using `static_assert`s
// across the codebase, hence we don't test it exhaustively here, but only
// please the coverage tool.
TEST(IdBase, isTrivial) {
  EXPECT_TRUE(Id::makeUndefined().isTrivial());
  EXPECT_TRUE(Id::makeFromBool(true).isTrivial());
  EXPECT_TRUE(Id::makeFromInt(42).isTrivial());
  EXPECT_TRUE(Id::makeFromDouble(3.14).isTrivial());
  EXPECT_TRUE(Id::makeFromDate(DateYearOrDuration::parseXsdDate("2000-01-01"))
                  .isTrivial());
  EXPECT_FALSE(Id::makeFromVocabIndex(VocabIndex::make(0)).isTrivial());
  EXPECT_FALSE(
      Id::makeFromLocalVocabIndex(reinterpret_cast<LocalVocabIndex>(0x100))
          .isTrivial());
  EXPECT_FALSE(
      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(17)).isTrivial());
  EXPECT_FALSE(Id::makeFromEncodedVal(738).isTrivial());
}

// Test that the `Id`s of all datatypes except `LocalVocabIndex` can be compared
// bitwise.
TEST(IdBase, canBeComparedBitwise) {
  EXPECT_TRUE(Id::makeUndefined().canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromBool(true).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromInt(1337).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromDouble(3.14).canBeComparedBitwise());
  EXPECT_TRUE(
      Id::makeFromVocabIndex(VocabIndex::make(0)).canBeComparedBitwise());
  EXPECT_FALSE(Id::makeFromLocalVocabIndex(nullptr).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromTextRecordIndex(TextRecordIndex::make(0))
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromDate(DateYearOrDuration{Date{0, 0, 0}})
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromGeoPoint(GeoPoint{0, 0}).canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromWordVocabIndex(WordVocabIndex::make(0))
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromBlankNodeIndex(BlankNodeIndex::make(17))
                  .canBeComparedBitwise());
  EXPECT_TRUE(Id::makeFromEncodedVal(738).canBeComparedBitwise());
}

// Test that `compareWithoutLocalVocab` yields the same order as
// `compareThreeWay` for `Id`s of all datatypes except `LocalVocabIndex`, and
// that it rejects an `Id` of type `LocalVocabIndex` if the expensive checks are
// enabled.
TEST_F(IdBaseTest, compareWithoutLocalVocab) {
  auto ids = makeRandomIds();
  ql::erase_if(
      ids, [](Id id) { return id.getDatatype() == Datatype::LocalVocabIndex; });
  ad_utility::SlowRandomIntGenerator<size_t> indexOfId(0, ids.size() - 1);
  for (size_t i = 0; i < numElements; ++i) {
    auto a = ids.at(indexOfId());
    auto b = ids.at(indexOfId());
    EXPECT_TRUE(a.compareWithoutLocalVocab(b) == a.compareThreeWay(b))
        << a << ' ' << b;
  }
  EXPECT_TRUE(Id::makeFromInt(-1).compareWithoutLocalVocab(
                  Id::makeFromDouble(0.0)) < 0);
  EXPECT_TRUE(makeVocabId(3).compareWithoutLocalVocab(makeVocabId(3)) == 0);

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    auto localVocabId = makeLocalVocabId(3);
    EXPECT_ANY_THROW(localVocabId.compareWithoutLocalVocab(makeVocabId(3)));
    EXPECT_ANY_THROW(makeVocabId(3).compareWithoutLocalVocab(localVocabId));
  }
}

// Test all the branches of `compareThreeWay` in which an `Id` of type
// `LocalVocabIndex` is compared to an `Id` of another datatype.
TEST(IdBase, compareThreeWayWithLocalVocabIndex) {
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
  LocalVocabEntry entryInVocab = iriLocalVocabEntry("<b>", ctx);
  LocalVocabEntry entryInSecondaryVocab = iriLocalVocabEntry("<zzz>", ctx);
  Id localVocabId = Id::makeFromLocalVocabIndex(&entryInVocab);
  Id localVocabIdSecondary =
      Id::makeFromLocalVocabIndex(&entryInSecondaryVocab);
  Id secondaryVocabId =
      Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(0));
  // `Int` is a datatype that is smaller than `LocalVocabIndex` and `Date` is
  // one that is greater, and neither of them is a datatype that a position in
  // the vocabularies can have. The same holds for `EncodedVal`, because an
  // encoded IRI is never stored in a `LocalVocabEntry`.
  Id intId = Id::makeFromInt(42);
  Id dateId = Id::makeFromDate(DateYearOrDuration{Date{2026, 8, 19}});
  Id encodedId = Id::makeFromEncodedVal(42);

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
  EXPECT_GT(encodedId, localVocabId);

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
  EXPECT_LT(localVocabId, encodedId);
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
TEST(IdBase, forwardingLambdas) {
  // A proxy type with the member functions, like the elements of a column view
  // that does not store `Id`s.
  struct Proxy {
    Id id_;
    bool isUndefined() const { return id_.isUndefined(); }
    Id::T getBits() const { return id_.getBits(); }
    Datatype getDatatype() const { return id_.getDatatype(); }
  };

  // Each lambda returns what the corresponding member function returns.
  for (Id id : {Id::makeUndefined(), Id::makeFromInt(42), Id::makeFromInt(-42),
                Id::makeFromDouble(13.37), Id::makeFromBool(true)}) {
    EXPECT_EQ(Id::isUndefinedL(id), id.isUndefined());
    EXPECT_EQ(Id::isDefinedL(id), !id.isUndefined());
    EXPECT_EQ(Id::getBitsL(id), id.getBits());
    EXPECT_EQ(Id::getDatatypeL(id), id.getDatatype());
    EXPECT_EQ(Id::isUndefinedL(Proxy{id}), id.isUndefined());
    EXPECT_EQ(Id::isDefinedL(Proxy{id}), !id.isUndefined());
    EXPECT_EQ(Id::getBitsL(Proxy{id}), id.getBits());
    EXPECT_EQ(Id::getDatatypeL(Proxy{id}), id.getDatatype());
  }

  // The lambdas work as predicates and projections of generic algorithms, also
  // over a range of proxies.
  std::vector ids{Id::makeFromInt(1), Id::makeUndefined(),
                  Id::makeFromDouble(3.5)};
  std::vector<Proxy> proxies{{ids[0]}, {ids[1]}, {ids[2]}};
  EXPECT_TRUE(ql::ranges::any_of(ids, Id::isUndefinedL));
  EXPECT_TRUE(ql::ranges::any_of(proxies, Id::isUndefinedL));
  EXPECT_EQ(ql::ranges::find(ids, Datatype::Double, Id::getDatatypeL),
            ids.begin() + 2);
  EXPECT_EQ(ql::ranges::find(proxies, Datatype::Double, Id::getDatatypeL),
            proxies.begin() + 2);
}

// Test that the lambda `Id::fromBitsL` forwards to the `fromBits` overload that
// takes all bits at once. In contrast to `fromBits`, which is overloaded, it
// can be passed to generic algorithms.
TEST(IdBase, fromBitsL) {
  std::vector<Id> ids{Id::makeUndefined(),
                      Id::makeFromBool(true),
                      Id::makeFromInt(42),
                      Id::makeFromInt(-42),
                      Id::makeFromDouble(13.37),
                      Id::makeFromVocabIndex(VocabIndex::make(123)),
                      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(7))};

  // The lambda returns what `fromBits` returns, and is the inverse of
  // `getBitsL`.
  for (Id id : ids) {
    EXPECT_EQ(Id::fromBitsL(id.getBits()).getBits(),
              Id::fromBits(id.getBits()).getBits());
    EXPECT_EQ(Id::fromBitsL(Id::getBitsL(id)), id);
  }

  // The lambda works as the transformation of a generic algorithm.
  std::vector<Id::T> bits;
  ql::ranges::transform(ids, std::back_inserter(bits), Id::getBitsL);
  std::vector<Id> rebuilt(bits.size());
  ql::ranges::transform(bits, rebuilt.begin(), Id::fromBitsL);
  EXPECT_EQ(rebuilt, ids);
}
