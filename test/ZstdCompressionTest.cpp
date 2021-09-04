// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include "../src/util/CompressionUsingZstd/ZstdWrapper.h"

TEST(CompressionTest, Basic) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp;
  ZstdWrapper::compress(x.data(), x.size() * sizeof(int),
                        [&comp](auto result) { comp = std::move(result); });
  auto decomp = ZstdWrapper::decompress<int>(comp.data(), comp.size(), 4);
  ASSERT_EQ(x, decomp);
}

// TODO: refactor to separate test
struct KeyLhs {
  size_t _firstId;
  size_t _lastId;
  size_t _firstLhs;
  size_t _lastLhs;
};

auto comp2 = [](const auto& a, const auto& b) {
  bool endBeforeBegin = a._lastId < b._firstId;
  endBeforeBegin |= (a._lastId == b._firstId && a._lastLhs < b._firstLhs);
  return endBeforeBegin;
};

auto assertL = [](const auto& a, const auto& b) {
  ASSERT_TRUE(comp2(a, b));
  ASSERT_FALSE(comp2(b, a));
};

auto assertEq = [](const auto& a, const auto& b) {
  ASSERT_FALSE(comp2(a, b));
  ASSERT_FALSE(comp2(b, a));
};

TEST(CompressedRelationTest, CorrectBounds) {
  KeyLhs a{3, 3, 5, 5};  // look for blocks with Id 3 and firstLhs 5

  KeyLhs b{1, 2, 0, 1000};
  assertL(b, a);
  b = KeyLhs{4, 7, 0, 1000};
  assertL(a, b);
  b = KeyLhs{1, 5, 0, 1000};
  assertEq(a, b);
  b = KeyLhs{3, 5, 0, 1000};
  assertEq(a, b);
  b = KeyLhs{3, 5, 5, 1000};
  assertEq(a, b);
  b = KeyLhs{3, 5, 6, 1000};
  assertL(a, b);
}
