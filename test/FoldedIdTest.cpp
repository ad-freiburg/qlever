//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/global/FoldedId.h"
#include "../src/util/HashSet.h"
#include "../src/util/Random.h"
#include "../src/util/Serializer/Serializer.h"

auto doubleGenerator = RandomDoubleGenerator();
auto idGenerator = SlowRandomIntGenerator<uint64_t>(0, FoldedId::maxIndex);
auto invalidIdGenerator = SlowRandomIntGenerator<uint64_t>(
    FoldedId::maxIndex, std::numeric_limits<uint64_t>::max());

auto nonOverflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    FoldedId::IntegerType::min(), FoldedId::IntegerType::max());
auto overflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    FoldedId::IntegerType::max() + 1, std::numeric_limits<int64_t>::max());
auto underflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    std::numeric_limits<int64_t>::min(), FoldedId::IntegerType::min() - 1);

TEST(FoldedId, Double) {
  for (size_t i = 0; i < 10000; ++i) {
    double d = doubleGenerator();
    auto id = FoldedId::Double(d);
    ASSERT_EQ(id.getDatatype(), Datatype::Double);
    // We lose 4 bits of precision, so `ASSERT_DOUBLE_EQ` would fail.
    ASSERT_FLOAT_EQ(id.getDouble(), d);
  }
}

TEST(FoldedId, Int) {
  for (size_t i = 0; i < 10000; ++i) {
    auto value = nonOverflowingNBitGenerator();
    auto id = FoldedId::Int(value);
    ASSERT_EQ(id.getDatatype(), Datatype::Int);
    ASSERT_EQ(id.getInt(), value);
  }

  auto testOverflow = [](auto generator) {
    using I = FoldedId::IntegerType;
    for (size_t i = 0; i < 10000; ++i) {
      auto value = generator();
      auto id = FoldedId::Int(value);
      ASSERT_EQ(id.getDatatype(), Datatype::Int);
      ASSERT_EQ(id.getInt(), I::fromNBit(I::toNBit(value)));
      ASSERT_NE(id.getInt(), value);
    }
  };

  testOverflow(overflowingNBitGenerator);
  testOverflow(underflowingNBitGenerator);
}

TEST(FoldedId, Indices) {
  auto testRandomIds = [&](auto makeId, auto getFromId, Datatype type) {
    auto testSingle = [&](auto value) {
      auto id = makeId(value);
      ASSERT_EQ(id.getDatatype(), type);
      ASSERT_EQ(std::invoke(getFromId, id), value);
    };
    for (size_t idx = 0; idx < 10000; ++idx) {
      testSingle(idGenerator());
    }
    testSingle(0);
    testSingle(FoldedId::maxIndex);

    for (size_t idx = 0; idx < 10000; ++idx) {
      auto value = invalidIdGenerator();
      ASSERT_THROW(makeId(value), FoldedId::IndexTooLargeException);
    }
  };

  testRandomIds(&FoldedId::Text, &FoldedId::getText, Datatype::Text);
  testRandomIds(&FoldedId::Vocab, &FoldedId::getVocab, Datatype::Vocab);
  testRandomIds(&FoldedId::LocalVocab, &FoldedId::getLocalVocab,
                Datatype::LocalVocab);
}

TEST(FoldedId, Undefined) {
  auto id = FoldedId::Undefined();
  ASSERT_EQ(id.getDatatype(), Datatype::Undefined);
}

auto makeRandomIds = []() {
  std::vector<FoldedId> ids;

  auto makeIdsFromGenerator = [&ids](auto& generator, auto makeIds) {
    for (size_t i = 0; i < 10000; ++i) {
      ids.push_back(makeIds(generator()));
    }
  };

  makeIdsFromGenerator(doubleGenerator, &FoldedId::Double);
  makeIdsFromGenerator(idGenerator, &FoldedId::Vocab);
  makeIdsFromGenerator(idGenerator, &FoldedId::Text);
  makeIdsFromGenerator(idGenerator, &FoldedId::LocalVocab);
  makeIdsFromGenerator(nonOverflowingNBitGenerator, &FoldedId::Int);
  makeIdsFromGenerator(overflowingNBitGenerator, &FoldedId::Int);
  makeIdsFromGenerator(underflowingNBitGenerator, &FoldedId::Int);

  for (size_t i = 0; i < 10000; ++i) {
    ids.push_back(FoldedId::Undefined());
  }

  randomShuffle(ids.begin(), ids.end());
  return ids;
};

TEST(FoldedId, Ordering) {
  auto ids = makeRandomIds();
  std::sort(ids.begin(), ids.end());

  auto sortedByDatatypeAndIndexTypes = [](FoldedId a, FoldedId b) {
    if (a.getDatatype() != b.getDatatype()) {
      return a.getDatatype() < b.getDatatype();
    }
    static ad_utility::HashSet<Datatype> indexTypes{
        Datatype::LocalVocab, Datatype::Vocab, Datatype::Text};
    if (indexTypes.contains(a.getDatatype())) {
      return a.getVocab() < b.getVocab();
    }
    return false;
  };
  ASSERT_TRUE(
      std::is_sorted(ids.begin(), ids.end(), sortedByDatatypeAndIndexTypes));
}

TEST(FoldedId, Serialization) {
  auto ids = makeRandomIds();

  for (auto id : ids) {
    ad_utility::serialization::ByteBufferWriteSerializer writer;
    writer << id;
    ad_utility::serialization::ByteBufferReadSerializer reader{
        std::move(writer).data()};
    FoldedId serializedId;
    reader >> serializedId;
    ASSERT_EQ(id, serializedId);
  }
}

TEST(FoldedId, Hashing) {
  auto ids = makeRandomIds();
  ad_utility::HashSet<FoldedId> duplicates;
  duplicates.reserve(2 * ids.size());
  for (size_t i = 0; i < 2; ++i) {
    for (auto id : ids) {
      duplicates.insert(id);
    }
  }
  std::vector<FoldedId> duplicatesAsVector;
  duplicatesAsVector.reserve(duplicates.size());
  for (auto id : duplicates) {
    duplicatesAsVector.push_back(id);
  }

  std::sort(duplicatesAsVector.begin(), duplicatesAsVector.end());
  std::sort(ids.begin(), ids.end());
  ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

  ASSERT_EQ(ids, duplicatesAsVector);
}

TEST(FoldedId, toDebugString) {
  auto test = [](FoldedId id, std::string_view expected) {
    std::stringstream stream;
    stream << id;
    ASSERT_EQ(stream.str(), expected);
  };
  test(FoldedId::Undefined(), "Undefined:Undefined");
  test(FoldedId::Int(-42), "Int:-42");
  test(FoldedId::Double(42.0), "Double:42.000000");
  test(FoldedId::Vocab(15), "Vocab:15");
  test(FoldedId::LocalVocab(25), "LocalVocab:25");
  test(FoldedId::Text(37), "Text:37");
}

TEST(FoldedId, TriviallyCopyable) {
  static_assert(std::is_trivially_copyable_v<FoldedId>);
}
