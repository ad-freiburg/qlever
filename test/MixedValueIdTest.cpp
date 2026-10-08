//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/hash/hash_testing.h>
#include <gtest/gtest.h>

#include <bitset>
#include <string>
#include <vector>

#include "./ValueIdTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "backports/algorithm.h"
#include "global/MixedValueId.h"
#include "index/LocalVocabEntry.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "util/HashSet.h"
#include "util/Random.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

// The helpers from `ValueIdTestHelpers.h` work with `Id`, so this test only
// makes sense if `Id` is the `MixedValueId` (it is only built in that case, see
// `test/CMakeLists.txt`).
static_assert(std::is_same_v<Id, MixedValueId>);

struct MixedValueIdTest : public ::testing::Test {
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
};

TEST_F(MixedValueIdTest, makeFromDouble) {
  auto testRepresentableDouble = [](double d) {
    auto id = MixedValueId::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // We lose `numDatatypeBits` bits of precision, so `ASSERT_DOUBLE_EQ` would
    // fail.
    ASSERT_FLOAT_EQ(id.getDouble(), d);
    // This check expresses the precision more exactly
    if (id.getDouble() != d) {
      // The if is needed for the case of += infinity.
      ASSERT_NEAR(
          id.getDouble(), d,
          std::abs(d / (uint64_t{1} << (52 - MixedValueId::numDatatypeBits))));
    }
  };

  auto testNonRepresentableSubnormal = [](double d) {
    auto id = MixedValueId::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // Subnormal numbers with a too small fraction are rounded to zero.
    ASSERT_EQ(id.getDouble(), 0.0);
  };
  for (size_t i = 0; i < 10'000; ++i) {
    testRepresentableDouble(positiveRepresentableDoubleGenerator());
    testRepresentableDouble(negativeRepresentableDoubleGenerator());
    auto nonRepresentable = nonRepresentableDoubleGenerator();
    // The random number generator includes the edge cases which would make the
    // tests fail.
    if (nonRepresentable != MixedValueId::minPositiveDouble &&
        nonRepresentable != -MixedValueId::minPositiveDouble) {
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
  testNonRepresentableSubnormal(0.0);

  auto quietNan = std::numeric_limits<double>::quiet_NaN();
  auto signalingNan = std::numeric_limits<double>::signaling_NaN();
  ASSERT_TRUE(std::isnan(MixedValueId::makeFromDouble(quietNan).getDouble()));
  ASSERT_TRUE(
      std::isnan(MixedValueId::makeFromDouble(signalingNan).getDouble()));

  // Test that the value of `minPositiveDouble` is correct.
  auto testSmallestNumber = [](double d) {
    ASSERT_EQ(MixedValueId::makeFromDouble(d).getDouble(), d);
    ASSERT_NE(d / 2, 0.0);
    ASSERT_EQ(MixedValueId::makeFromDouble(d / 2).getDouble(), 0.0);
  };
  testSmallestNumber(MixedValueId::minPositiveDouble);
  testSmallestNumber(-MixedValueId::minPositiveDouble);
}

TEST_F(MixedValueIdTest, makeFromInt) {
  for (size_t i = 0; i < 10'000; ++i) {
    auto value = nonOverflowingNBitGenerator();
    auto id = MixedValueId::makeFromInt(value);
    ASSERT_EQ(id.getDatatype(), Datatype::Int);
    ASSERT_EQ(id.getInt(), value);
  }

  auto testOverflow = [](auto generator) {
    using I = MixedValueId::IntegerType;
    for (size_t i = 0; i < 10'000; ++i) {
      auto value = generator();
      auto id = MixedValueId::makeFromInt(value);
      ASSERT_EQ(id.getDatatype(), Datatype::Int);
      ASSERT_EQ(id.getInt(), I::fromNBit(I::toNBit(value)));
      ASSERT_NE(id.getInt(), value);
    }
  };

  testOverflow(overflowingNBitGenerator);
  testOverflow(underflowingNBitGenerator);
}

// _____________________________________________________________________________
TEST_F(MixedValueIdTest, makeFromBool) {
  EXPECT_TRUE(MixedValueId::makeBoolFromZeroOrOne(true).getBool());
  EXPECT_TRUE(MixedValueId::makeFromBool(true).getBool());
  EXPECT_FALSE(MixedValueId::makeBoolFromZeroOrOne(false).getBool());
  EXPECT_FALSE(MixedValueId::makeFromBool(false).getBool());

  EXPECT_EQ(MixedValueId::makeBoolFromZeroOrOne(true).getBoolLiteral(), "1");
  EXPECT_EQ(MixedValueId::makeFromBool(true).getBoolLiteral(), "true");
  EXPECT_EQ(MixedValueId::makeBoolFromZeroOrOne(false).getBoolLiteral(), "0");
  EXPECT_EQ(MixedValueId::makeFromBool(false).getBoolLiteral(), "false");
}

TEST_F(MixedValueIdTest, Indices) {
  auto testRandomIds = [&](auto makeId, auto getFromId, Datatype type) {
    auto testSingle = [&](auto value) {
      auto id = makeId(value);
      ASSERT_EQ(id.getDatatype(), type);
      ASSERT_EQ(std::invoke(getFromId, id), value);
    };
    for (size_t idx = 0; idx < 10'000; ++idx) {
      testSingle(indexGenerator());
    }
    testSingle(0);
    testSingle(MixedValueId::maxIndex);

    if (type != Datatype::LocalVocabIndex) {
      for (size_t idx = 0; idx < 10'000; ++idx) {
        auto value = invalidIndexGenerator();
        ASSERT_THROW(makeId(value), MixedValueId::IndexTooLargeException);
        AD_EXPECT_THROW_WITH_MESSAGE(
            makeId(value), ::testing::ContainsRegex("is bigger than"));
      }
    }
  };

  testRandomIds(&makeTextRecordId, &getTextRecordIndex,
                Datatype::TextRecordIndex);
  testRandomIds(&makeVocabId, &getVocabIndex, Datatype::VocabIndex);

  auto localVocabWordToInt = [](const auto& input) {
    return std::atoll(getLocalVocabIndex(input).c_str());
  };
  testRandomIds(&makeLocalVocabId, localVocabWordToInt,
                Datatype::LocalVocabIndex);
  testRandomIds(&makeWordVocabId, &getWordVocabIndex, Datatype::WordVocabIndex);
  testRandomIds(&makeSecondaryVocabId, &getSecondaryVocabIndex,
                Datatype::SecondaryVocabIndex);
}

TEST_F(MixedValueIdTest, Undefined) {
  auto id = MixedValueId::makeUndefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);

  // `getUndefined()` returns the single value of `UndefinedType`. Its main
  // purpose is the generic code in `visit`, which has to dispatch on the
  // datatype, so we also test it via that path.
  static_assert(
      std::is_same_v<decltype(id.getUndefined()), MixedValueId::UndefinedType>);
  auto isUndefinedType = [](const auto& value) {
    return std::is_same_v<std::decay_t<decltype(value)>,
                          MixedValueId::UndefinedType>;
  };
  EXPECT_TRUE(isUndefinedType(id.getUndefined()));
  EXPECT_TRUE(id.visit(isUndefinedType));
  EXPECT_FALSE(MixedValueId::makeFromInt(42).visit(isUndefinedType));
}

TEST_F(MixedValueIdTest, OrderingDifferentDatatypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end());

  auto compareByDatatypeAndIndexTypes = [](MixedValueId a, MixedValueId b) {
    auto typeA = a.getDatatype();
    auto typeB = b.getDatatype();
    if (ad_utility::contains(MixedValueId::stringTypes_, typeA) &&
        ad_utility::contains(MixedValueId::stringTypes_, typeB)) {
      return false;
    }
    return a.getDatatype() < b.getDatatype();
  };
  ASSERT_TRUE(
      std::is_sorted(ids.begin(), ids.end(), compareByDatatypeAndIndexTypes));
}

TEST_F(MixedValueIdTest, IndexOrdering) {
  auto testOrder = [](auto makeIdFromIndex, auto getIndexFromId) {
    std::vector<MixedValueId> ids;
    addIdsFromGenerator(indexGenerator, makeIdFromIndex, ids);
    std::vector<std::invoke_result_t<decltype(getIndexFromId), MixedValueId>>
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

  testOrder(&makeVocabId, &getVocabIndex);
  testOrder(&makeLocalVocabId, &getLocalVocabIndex);
  testOrder(&makeWordVocabId, &getWordVocabIndex);
  testOrder(&makeSecondaryVocabId, &getSecondaryVocabIndex);
  testOrder(&makeTextRecordId, &getTextRecordIndex);
}

TEST_F(MixedValueIdTest, DoubleOrdering) {
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

  // When sorting MixedValueIds that hold doubles, the NaN values form a
  // contiguous range.
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

  // In `ids` the negative number stand AFTER the positive numbers because of
  // the bitOrdering. First rotate the negative numbers to the beginning.
  auto doubleIdIsNegative = [](MixedValueId id) {
    auto bits = absl::bit_cast<uint64_t>(id.getDouble());
    return bits & ad_utility::bitMaskForHigherBits(1);
  };
  auto beginOfNegatives =
      std::find_if(ids.begin(), ids.end(), doubleIdIsNegative);
  auto endOfNegatives = std::rotate(ids.begin(), beginOfNegatives, ids.end());

  // The negative numbers now come before the positive numbers, but the are
  // ordered in descending instead of ascending order, reverse them.
  std::reverse(ids.begin(), endOfNegatives);

  // After these two transformations (switch positive and negative range,
  // reverse negative range) the `ids` are sorted in exactly the same order as
  // the `doubles`.
  for (size_t i = 0; i < ids.size(); ++i) {
    auto doubleTruncated = MixedValueId::makeFromDouble(doubles[i]).getDouble();
    ASSERT_EQ(ids[i].getDouble(), doubleTruncated);
  }
}

TEST_F(MixedValueIdTest, SignedIntegerOrdering) {
  std::vector<MixedValueId> ids;
  addIdsFromGenerator(nonOverflowingNBitGenerator, &MixedValueId::makeFromInt,
                      ids);
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
      ids.begin(), ids.end(), [](MixedValueId id) { return id.getInt() < 0; });
  std::rotate(ids.begin(), beginOfNegative, ids.end());

  // Now `integers` and `ids` should be in the same order
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSERT_EQ(ids[i].getInt(), integers[i]);
  }
}

TEST_F(MixedValueIdTest, Serialization) {
  auto ids = makeRandomIds();

  for (auto id : ids) {
    ad_utility::serialization::ByteBufferWriteSerializer writer;
    writer << id;
    ad_utility::serialization::ByteBufferReadSerializer reader{
        std::move(writer).data()};
    MixedValueId serializedId;
    reader >> serializedId;
    ASSERT_EQ(id, serializedId);
  }
}

TEST_F(MixedValueIdTest, Hashing) {
  {
    auto ids = makeRandomIds();
    ad_utility::HashSet<MixedValueId> idsWithoutDuplicates;
    for (size_t i = 0; i < 2; ++i) {
      for (auto id : ids) {
        idsWithoutDuplicates.insert(id);
      }
    }
    std::vector<MixedValueId> idsWithoutDuplicatesAsVector(
        idsWithoutDuplicates.begin(), idsWithoutDuplicates.end());

    std::sort(idsWithoutDuplicatesAsVector.begin(),
              idsWithoutDuplicatesAsVector.end());
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    ASSERT_EQ(ids, idsWithoutDuplicatesAsVector);
  }
  {
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
      return Id::makeFromLocalVocabIndex(lv.getIndexAndAddIfNotContained(lve));
    };
    // Checks that hashing is implemented correctly using `==` for equality. The
    // hash expansion is the values added with `combine`.
    // - If two elements are equal, then their hash expansions must be equal.
    // - If two elements are not equal, then hash expansions must differ and
    // neither can be a suffix of the other.
    EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(
        {LVID(lve1, lv1), LVID(lve2, lv2), LVID(lve3, lv1), LVID(lve4, lv1),
         mkId("<x>"), Id::makeFromInt(0), Id::makeFromInt(42),
         Id::makeFromDouble(0), Id::makeFromDouble(1.56),
         Id::makeFromDouble(1e-10), Id::makeFromDouble(1e+100),
         Id::makeFromBool(true), Id::makeFromBool(false),
         Id::makeUndefined()}));
  }
}

TEST_F(MixedValueIdTest, toDebugString) {
  auto test = [](MixedValueId id, std::string_view expected) {
    std::stringstream stream;
    stream << id;
    ASSERT_EQ(stream.str(), expected);
  };
  test(MixedValueId::makeUndefined(), "U:0");
  // Values with type undefined can usually only have one value (all data bits
  // zero). Sometimes MixedValueIds with type undefined but non-zero data bits
  // are used. The following test tests one of these internal MixedValueIds.
  MixedValueId customUndefined =
      MixedValueId::fromBits(MixedValueId::IntegerType::fromNBit(100) |
                             (static_cast<MixedValueId::T>(Datatype::Undefined)
                              << MixedValueId::numDataBits));
  test(customUndefined, "U:100");
  test(MixedValueId::makeFromDouble(42.0), "D:42.000000");
  test(MixedValueId::makeFromBool(false), "B:false");
  test(MixedValueId::makeFromBool(true), "B:true");
  test(MixedValueId::makeBoolFromZeroOrOne(false), "B:false");
  test(MixedValueId::makeBoolFromZeroOrOne(true), "B:true");
  test(makeVocabId(15), "V:15");
  auto str = LocalVocabEntry::literalWithoutQuotes(
      "SomeValue", qec_->getLocalVocabContext());
  test(MixedValueId::makeFromLocalVocabIndex(&str), "L:\"SomeValue\"");
  test(makeTextRecordId(37), "T:37");
  test(makeWordVocabId(42), "W:42");
  test(makeBlankNodeId(27), "B:27");
  test(MixedValueId::makeFromDate(
           DateYearOrDuration{123456, DateYearOrDuration::Type::Year}),
       "D:123456");
  test(MixedValueId::makeFromGeoPoint(GeoPoint{50.0, 50.0}),
       "G:POINT(50.000000 50.000000)");
  // make an ID with an invalid datatype
  ASSERT_ANY_THROW(test(MixedValueId::max(), "blim"));
}

TEST_F(MixedValueIdTest, InvalidDatatypeEnumValue) {
  ASSERT_ANY_THROW(toString(static_cast<Datatype>(2345)));
}

TEST_F(MixedValueIdTest, TriviallyCopyable) {
  static_assert(std::is_trivially_copyable_v<MixedValueId>);
}

// Pin down that the `MixedValueId` functions that can be evaluated at compile
// time actually are `constexpr`. Note that several of them contain an
// `AD_CONTRACT_CHECK`/`AD_EXPENSIVE_CHECK`, which is only possible because
// those macros are `constexpr`-friendly, see the note on `constexpr` in
// `util/Exception.h`.
// NOTE: The functions that are only `QL_CONSTEXPR` (`constexpr` in C++20 mode
// only) are excluded in C++17 mode, see the notes in `global/MixedValueId.h`.
namespace constexprMixedValueId {
static_assert(MixedValueId::makeUndefined().getDatatype() ==
              Datatype::Undefined);
static_assert(MixedValueId::makeFromBool(true).getBool());
static_assert(!MixedValueId::makeBoolFromZeroOrOne(false).getBool());
static_assert(MixedValueId::makeFromInt(42).getDatatype() == Datatype::Int);
static_assert(MixedValueId::makeFromVocabIndex(VocabIndex::make(17))
                  .getVocabIndex() == VocabIndex::make(17));
static_assert(MixedValueId::makeFromEncodedVal(17).getEncodedVal() == 17);
static_assert(MixedValueId::makeFromTextRecordIndex(TextRecordIndex::make(17))
                  .getTextRecordIndex() == TextRecordIndex::make(17));
static_assert(MixedValueId::makeFromWordVocabIndex(WordVocabIndex::make(17))
                  .getWordVocabIndex() == WordVocabIndex::make(17));
static_assert(MixedValueId::makeFromBlankNodeIndex(BlankNodeIndex::make(17))
                  .getBlankNodeIndex() == BlankNodeIndex::make(17));
static_assert(
    MixedValueId::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(17))
        .getSecondaryVocabIndex() == SecondaryVocabIndex::make(17));
static_assert(MixedValueId::makeFromBool(true).getBoolLiteral() == "true");
static_assert(MixedValueId::makeBoolFromZeroOrOne(true).getBoolLiteral() ==
              "1");
#ifndef QLEVER_CPP_17
// `getInt` performs a signed left shift, which is only a constant expression
// since C++20.
static_assert(MixedValueId::makeFromInt(-42).getInt() == -42);
static_assert(MixedValueId::fromBits(MixedValueId::makeFromInt(42).getBits())
                  .getInt() == 42);
static_assert(MixedValueId::makeUndefined().isTrivial());
static_assert(MixedValueId::makeUndefined().isUndefined());
static_assert(!MixedValueId::makeFromBool(true).isUndefined());
static_assert(MixedValueId::makeFromDouble(0.5).getDouble() == 0.5);
// The `compareWithoutLocalVocab` contains two `AD_EXPENSIVE_CHECK`s.
static_assert(MixedValueId::makeFromBool(true).compareWithoutLocalVocab(
                  MixedValueId::makeUndefined()) > 0);
static_assert(MixedValueId::makeUndefined().compareWithoutLocalVocab(
                  MixedValueId::makeUndefined()) == 0);
#endif
}  // namespace constexprMixedValueId

// _____________________________________________________________________________
TEST_F(MixedValueIdTest, EncodedIriEqualityWithLocalVocabEntry) {
  // Test that an ID storing an encoded IRI compares equal to a LocalVocabEntry
  // with the same IRI value.

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
  auto localVocabId = MixedValueId::makeFromLocalVocabIndex(&localVocabEntry);

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
  auto localVocabId2 = MixedValueId::makeFromLocalVocabIndex(&localVocabEntry2);

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

// Note: the `isTrivial` functionality is also tested using `static_assert`s
// across the codebase, hence we don't test it exhaustively here, but only
// please the coverage tool.
TEST(MixedValueId, isTrivial) {
  EXPECT_TRUE(Id::makeUndefined().isTrivial());
  EXPECT_FALSE(
      Id::makeFromBlankNodeIndex(BlankNodeIndex::make(17)).isTrivial());
  EXPECT_FALSE(Id::makeFromEncodedVal(738).isTrivial());
}

// _____________________________________________________________________________
TEST(MixedValueId, canBeComparedBitwise) {
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

// _____________________________________________________________________________
TEST(MixedValueId, compareThreeWayWithLocalVocabIndex) {
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
  Id localVocabId = Id::makeFromLocalVocabIndex(&entryInVocab);
  Id localVocabIdSecondary =
      Id::makeFromLocalVocabIndex(&entryInSecondaryVocab);
  Id secondaryVocabId =
      Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(0));
  // `Int` is a datatype that is smaller than `LocalVocabIndex` and `Date` is
  // one that is greater, and neither of them is a datatype that a position in
  // the vocabularies can have.
  Id intId = Id::makeFromInt(42);
  Id dateId = Id::makeFromDate(DateYearOrDuration{Date{2026, 8, 19}});

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
TEST(ValueId, forwardingLambdas) {
  // A proxy type with the member functions, like the elements of a column view
  // that does not store `Id`s.
  struct Proxy {
    Id id_;
    bool isUndefined() const { return id_.isUndefined(); }
    uint64_t getBits() const { return id_.getBits(); }
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
