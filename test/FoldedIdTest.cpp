//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/global/FoldedId.h"
#include "../src/util/Random.h"

auto doubleGenerator = RandomDoubleGenerator();
auto idGenerator = SlowRandomIntGenerator<uint64_t>(0, FoldedId::maxId);
auto invalidIdGenerator = SlowRandomIntGenerator<uint64_t>(
    FoldedId::maxId, std::numeric_limits<uint64_t>::max());

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
    testSingle(FoldedId::maxId);

    for (size_t idx = 0; idx < 10000; ++idx) {
      auto value = invalidIdGenerator();
      ASSERT_THROW(makeId(value), FoldedId::IdTooLargeException);
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

TEST(FoldedId, Ordering) {
  // TODO<joka921> implement tests.
}
