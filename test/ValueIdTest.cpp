//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/global/ValueId.h"
#include "../src/util/HashSet.h"
#include "../src/util/Random.h"
#include "../src/util/Serializer/Serializer.h"

auto doubleGenerator = RandomDoubleGenerator();
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
  for (size_t i = 0; i < 10'000; ++i) {
    double d = doubleGenerator();
    auto id = ValueId::makeFromDouble(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // We lose 4 bits of precision, so `ASSERT_DOUBLE_EQ` would fail.
    ASSERT_FLOAT_EQ(id.getDouble(), d);
  }
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
  testRandomIds(&ValueId::makeFromLocalVocabIndex, &ValueId::getLocalVocab,
                Datatype::LocalVocabIndex);
}

TEST(ValueId, Undefined) {
  auto id = ValueId::makeUndefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);
}

auto makeRandomIds = []() {
  std::vector<ValueId> ids;

  auto addIdsFromGenerator = [&ids](auto& generator, auto makeIds) {
    for (size_t i = 0; i < 10'000; ++i) {
      ids.push_back(makeIds(generator()));
    }
  };

  addIdsFromGenerator(doubleGenerator, &ValueId::makeFromDouble);
  addIdsFromGenerator(indexGenerator, &ValueId::makeFromVocabIndex);
  addIdsFromGenerator(indexGenerator, &ValueId::makeFromLocalVocabIndex);
  addIdsFromGenerator(indexGenerator, &ValueId::makeFromTextIndex);
  addIdsFromGenerator(nonOverflowingNBitGenerator, &ValueId::makeFromInt);
  addIdsFromGenerator(overflowingNBitGenerator, &ValueId::makeFromInt);
  addIdsFromGenerator(underflowingNBitGenerator, &ValueId::makeFromInt);

  for (size_t i = 0; i < 10'000; ++i) {
    ids.push_back(ValueId::makeUndefined());
  }

  randomShuffle(ids.begin(), ids.end());
  return ids;
};

TEST(ValueId, Ordering) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end());

  auto compareByDatatypeAndIndexTypes = [](ValueId a, ValueId b) {
    if (a.getDatatype() != b.getDatatype()) {
      return a.getDatatype() < b.getDatatype();
    }
    static ad_utility::HashSet<Datatype> indexTypes{
        Datatype::LocalVocabIndex, Datatype::VocabIndex, Datatype::TextIndex};
    if (indexTypes.contains(a.getDatatype())) {
      return a.getVocabIndex() < b.getVocabIndex();
    }
    return false;
  };
  ASSERT_TRUE(
      std::is_sorted(ids.begin(), ids.end(), compareByDatatypeAndIndexTypes));
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
