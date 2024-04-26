//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>

#include "./ValueIdTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "global/ValueId.h"
#include "util/HashSet.h"
#include "util/Random.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/Serializer.h"

TEST(ValueId, makeFromDouble) {
  auto testRepresentableDouble = [](double d) {
    auto id = ValueId::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // We lose `numDatatypeBits` bits of precision, so `ASSERT_DOUBLE_EQ` would
    // fail.
    ASSERT_FLOAT_EQ(id.getDouble(), d);
    // This check expresses the precision more exactly
    if (id.getDouble() != d) {
      // The if is needed for the case of += infinity.
      ASSERT_NEAR(id.getDouble(), d,
                  std::abs(d / (1ul << (52 - ValueId::numDatatypeBits))));
    }
  };

  auto testNonRepresentableSubnormal = [](double d) {
    auto id = ValueId::makeFromDouble(d);
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
    if (nonRepresentable != ValueId::minPositiveDouble &&
        nonRepresentable != -ValueId::minPositiveDouble) {
      testNonRepresentableSubnormal(nonRepresentable);
    }
  }

  testRepresentableDouble(std::numeric_limits<double>::infinity());
  testRepresentableDouble(-std::numeric_limits<double>::infinity());

  // Test positive and negative 0.
  ASSERT_NE(std::bit_cast<uint64_t>(0.0), std::bit_cast<uint64_t>(-0.0));
  ASSERT_EQ(0.0, -0.0);
  testRepresentableDouble(0.0);
  testRepresentableDouble(-0.0);
  testNonRepresentableSubnormal(0.0);
  testNonRepresentableSubnormal(0.0);

  auto quietNan = std::numeric_limits<double>::quiet_NaN();
  auto signalingNan = std::numeric_limits<double>::signaling_NaN();
  ASSERT_TRUE(std::isnan(ValueId::makeFromDouble(quietNan).getDouble()));
  ASSERT_TRUE(std::isnan(ValueId::makeFromDouble(signalingNan).getDouble()));

  // Test that the value of `minPositiveDouble` is correct.
  auto testSmallestNumber = [](double d) {
    ASSERT_EQ(ValueId::makeFromDouble(d).getDouble(), d);
    ASSERT_NE(d / 2, 0.0);
    ASSERT_EQ(ValueId::makeFromDouble(d / 2).getDouble(), 0.0);
  };
  testSmallestNumber(ValueId::minPositiveDouble);
  testSmallestNumber(-ValueId::minPositiveDouble);
}

TEST(ValueId, makeFromInt) {
  for (size_t i = 0; i < 10'000; ++i) {
    auto value = nonOverflowingNBitGenerator();
    auto id = ValueId::makeFromInt(value);
    ASSERT_EQ(id.getDatatype(), Datatype::Int);
    ASSERT_EQ(id.getInt(), value);
  }

  auto testOverflow = [](auto generator) {
    using I = ValueId::IntegerType;
    for (size_t i = 0; i < 10'000; ++i) {
      auto value = generator();
      auto id = ValueId::makeFromInt(value);
      ASSERT_EQ(id.getDatatype(), Datatype::Int);
      ASSERT_EQ(id.getInt(), I::fromNBit(I::toNBit(value)));
      ASSERT_NE(id.getInt(), value);
    }
  };

  testOverflow(overflowingNBitGenerator);
  testOverflow(underflowingNBitGenerator);
}

TEST(ValueId, Indices) {
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
    testSingle(ValueId::maxIndex);

    if (type != Datatype::LocalVocabIndex) {
      for (size_t idx = 0; idx < 10'000; ++idx) {
        auto value = invalidIndexGenerator();
        ASSERT_THROW(makeId(value), ValueId::IndexTooLargeException);
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
}

TEST(ValueId, Undefined) {
  auto id = ValueId::makeUndefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);
}

TEST(ValueId, OrderingDifferentDatatypes) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end());

  auto compareByDatatypeAndIndexTypes = [](ValueId a, ValueId b) {
    return a.getDatatype() < b.getDatatype();
  };
  ASSERT_TRUE(
      std::is_sorted(ids.begin(), ids.end(), compareByDatatypeAndIndexTypes));
}

TEST(ValueId, IndexOrdering) {
  auto testOrder = [](auto makeIdFromIndex, auto getIndexFromId) {
    std::vector<ValueId> ids;
    addIdsFromGenerator(indexGenerator, makeIdFromIndex, ids);
    std::vector<std::invoke_result_t<decltype(getIndexFromId), ValueId>>
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
  testOrder(&makeTextRecordId, &getTextRecordIndex);
}

TEST(ValueId, DoubleOrdering) {
  auto ids = makeRandomDoubleIds();
  std::vector<double> doubles;
  doubles.reserve(ids.size());
  for (auto id : ids) {
    doubles.push_back(id.getDouble());
  }
  std::sort(ids.begin(), ids.end());

  // The sorting of `double`s is broken as soon as NaNs are present. We remove
  // the NaNs from the `double`s.
  std::erase_if(doubles, [](double d) { return std::isnan(d); });
  std::sort(doubles.begin(), doubles.end());

  // When sorting ValueIds that hold doubles, the NaN values form a contiguous
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

  // In `ids` the negative number stand AFTER the positive numbers because of
  // the bitOrdering. First rotate the negative numbers to the beginning.
  auto doubleIdIsNegative = [](ValueId id) {
    auto bits = std::bit_cast<uint64_t>(id.getDouble());
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
    auto doubleTruncated = ValueId::makeFromDouble(doubles[i]).getDouble();
    ASSERT_EQ(ids[i].getDouble(), doubleTruncated);
  }
}

TEST(ValueId, SignedIntegerOrdering) {
  std::vector<ValueId> ids;
  addIdsFromGenerator(nonOverflowingNBitGenerator, &ValueId::makeFromInt, ids);
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
      ids.begin(), ids.end(), [](ValueId id) { return id.getInt() < 0; });
  std::rotate(ids.begin(), beginOfNegative, ids.end());

  // Now `integers` and `ids` should be in the same order
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSERT_EQ(ids[i].getInt(), integers[i]);
  }
}

TEST(ValueId, Serialization) {
  auto ids = makeRandomIds();

  for (auto id : ids) {
    ad_utility::serialization::ByteBufferWriteSerializer writer;
    writer << id;
    ad_utility::serialization::ByteBufferReadSerializer reader{
        std::move(writer).data()};
    ValueId serializedId;
    reader >> serializedId;
    ASSERT_EQ(id, serializedId);
  }
}

TEST(ValueId, Hashing) {
  auto ids = makeRandomIds();
  ad_utility::HashSet<ValueId> idsWithoutDuplicates;
  for (size_t i = 0; i < 2; ++i) {
    for (auto id : ids) {
      idsWithoutDuplicates.insert(id);
    }
  }
  std::vector<ValueId> idsWithoutDuplicatesAsVector(
      idsWithoutDuplicates.begin(), idsWithoutDuplicates.end());

  std::sort(idsWithoutDuplicatesAsVector.begin(),
            idsWithoutDuplicatesAsVector.end());
  std::sort(ids.begin(), ids.end());
  ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

  ASSERT_EQ(ids, idsWithoutDuplicatesAsVector);
}

TEST(ValueId, toDebugString) {
  auto test = [](ValueId id, std::string_view expected) {
    std::stringstream stream;
    stream << id;
    ASSERT_EQ(stream.str(), expected);
  };
  test(ValueId::makeUndefined(), "Undefined:Undefined");
  test(ValueId::makeFromInt(-42), "Int:-42");
  test(ValueId::makeFromDouble(42.0), "Double:42.000000");
  test(ValueId::makeFromBool(false), "Bool:false");
  test(ValueId::makeFromBool(true), "Bool:true");
  test(makeVocabId(15), "VocabIndex:15");
  auto str = ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(
      "SomeValue");
  test(ValueId::makeFromLocalVocabIndex(&str), "LocalVocabIndex:\"SomeValue\"");
  test(makeTextRecordId(37), "TextRecordIndex:37");
  test(makeWordVocabId(42), "WordVocabIndex:42");
  test(makeBlankNodeId(27), "BlankNodeIndex:27");
  test(ValueId::makeFromDate(
           DateOrLargeYear{123456, DateOrLargeYear::Type::Year}),
       "Date:123456");
  // make an ID with an invalid datatype
  ASSERT_ANY_THROW(test(ValueId::max(), "blim"));
}

TEST(ValueId, InvalidDatatypeEnumValue) {
  ASSERT_ANY_THROW(toString(static_cast<Datatype>(2345)));
}

TEST(ValueId, TriviallyCopyable) {
  static_assert(std::is_trivially_copyable_v<ValueId>);
}
