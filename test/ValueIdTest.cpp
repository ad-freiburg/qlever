//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>

#include "../src/global/ValueId.h"
#include "../src/util/HashSet.h"
#include "../src/util/Random.h"
#include "../src/util/Serializer/Serializer.h"

auto positiveRepresentableDoubleGenerator = RandomDoubleGenerator(
    ValueId::minPositiveDouble, std::numeric_limits<double>::max());
auto negativeRepresentableDoubleGenerator = RandomDoubleGenerator(
    -std::numeric_limits<double>::max(), -ValueId::minPositiveDouble);
auto nonRepresentableDoubleGenerator = RandomDoubleGenerator(
    -ValueId::minPositiveDouble, ValueId::minPositiveDouble);
auto indexGenerator = SlowRandomIntGenerator<uint64_t>(0, ValueId::maxIndex);
auto invalidIndexGenerator = SlowRandomIntGenerator<uint64_t>(
    ValueId::maxIndex, std::numeric_limits<uint64_t>::max());

auto nonOverflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    ValueId::IntegerType::min(), ValueId::IntegerType::max());
auto overflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    ValueId::IntegerType::max() + 1, std::numeric_limits<int64_t>::max());
auto underflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    std::numeric_limits<int64_t>::min(), ValueId::IntegerType::min() - 1);

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

    for (size_t idx = 0; idx < 10'000; ++idx) {
      auto value = invalidIndexGenerator();
      ASSERT_THROW(makeId(value), ValueId::IndexTooLargeException);
    }
  };

  testRandomIds(&ValueId::makeFromTextIndex, &ValueId::getTextIndex,
                Datatype::TextIndex);
  testRandomIds(&ValueId::makeFromVocabIndex, &ValueId::getVocabIndex,
                Datatype::VocabIndex);
  testRandomIds(&ValueId::makeFromLocalVocabIndex, &ValueId::getLocalVocabIndex,
                Datatype::LocalVocabIndex);
}

TEST(ValueId, Undefined) {
  auto id = ValueId::makeUndefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);
}

auto addIdsFromGenerator = [](auto& generator, auto makeIds,
                              std::vector<ValueId>& ids) {
  for (size_t i = 0; i < 10'000; ++i) {
    ids.push_back(makeIds(generator()));
  }
};

auto makeRandomDoubleIds = []() {
  std::vector<ValueId> ids;
  addIdsFromGenerator(positiveRepresentableDoubleGenerator,
                      &ValueId::makeFromDouble, ids);
  addIdsFromGenerator(negativeRepresentableDoubleGenerator,
                      &ValueId::makeFromDouble, ids);

  for (size_t i = 0; i < 1000; ++i) {
    ids.push_back(ValueId::makeFromDouble(0.0));
    ids.push_back(ValueId::makeFromDouble(-0.0));
    auto inf = std::numeric_limits<double>::infinity();
    ids.push_back(ValueId::makeFromDouble(inf));
    ids.push_back(ValueId::makeFromDouble(-inf));
    auto nan = std::numeric_limits<double>::quiet_NaN();
    ids.push_back(ValueId::makeFromDouble(nan));
    auto max = std::numeric_limits<double>::max();
    auto min = std::numeric_limits<double>::min();
    ids.push_back(ValueId::makeFromDouble(max));
    ids.push_back(ValueId::makeFromDouble(min));
  }
  randomShuffle(ids.begin(), ids.end());
  return ids;
};
auto makeRandomIds = []() {
  std::vector<ValueId> ids = makeRandomDoubleIds();
  addIdsFromGenerator(indexGenerator, &ValueId::makeFromVocabIndex, ids);
  addIdsFromGenerator(indexGenerator, &ValueId::makeFromLocalVocabIndex, ids);
  addIdsFromGenerator(indexGenerator, &ValueId::makeFromTextIndex, ids);
  addIdsFromGenerator(nonOverflowingNBitGenerator, &ValueId::makeFromInt, ids);
  addIdsFromGenerator(overflowingNBitGenerator, &ValueId::makeFromInt, ids);
  addIdsFromGenerator(underflowingNBitGenerator, &ValueId::makeFromInt, ids);

  for (size_t i = 0; i < 10'000; ++i) {
    ids.push_back(ValueId::makeUndefined());
  }

  randomShuffle(ids.begin(), ids.end());
  return ids;
};

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

  testOrder(&ValueId::makeFromVocabIndex, &ValueId::getVocabIndex);
  testOrder(&ValueId::makeFromLocalVocabIndex, &ValueId::getLocalVocabIndex);
  testOrder(&ValueId::makeFromTextIndex, &ValueId::getTextIndex);
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
  test(ValueId::makeFromVocabIndex(15), "VocabIndex:15");
  test(ValueId::makeFromLocalVocabIndex(25), "LocalVocabIndex:25");
  test(ValueId::makeFromTextIndex(37), "TextIndex:37");
}

TEST(ValueId, TriviallyCopyable) {
  static_assert(std::is_trivially_copyable_v<ValueId>);
}
