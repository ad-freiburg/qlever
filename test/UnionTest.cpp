// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "../src/engine/CallFixedSize.h"
#include "../src/engine/Union.h"
#include "../src/global/Id.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

TEST(UnionTest, computeUnion) {
  IdTable left(1, allocator());
  left.push_back({I(1)});
  left.push_back({I(2)});
  left.push_back({I(3)});

  IdTable right(2, allocator());
  right.push_back({I(4), I(5)});
  right.push_back({I(6), I(7)});

  IdTable result(2, allocator());

  const std::vector<std::array<size_t, 2>> columnOrigins = {
      {0, 1}, {Union::NO_COLUMN, 0}};

  int leftWidth = left.numColumns();
  int rightWidth = right.numColumns();
  int outWidth = result.numColumns();
  Union U{Union::InvalidUnionOnlyUseForTestinTag{}};
  CALL_FIXED_SIZE((std::array{leftWidth, rightWidth, outWidth}),
                  &Union::computeUnion, &U, &result, left, right,
                  columnOrigins);

  ASSERT_EQ(5u, result.size());
  for (size_t i = 0; i < left.size(); i++) {
    ASSERT_EQ(left(i, 0), result(i, 0));
    ASSERT_EQ(ID_NO_VALUE, result(i, 1));
  }
  for (size_t i = 0; i < right.size(); i++) {
    ASSERT_EQ(right(i, 0), result(i + left.size(), 1));
    ASSERT_EQ(right(i, 1), result(i + left.size(), 0));
  }
}

TEST(UnionTest, computeUnionOptimized) {
  // the left and right data vectors will be deleted by the result tables
  IdTable left(2, allocator());
  left.push_back({I(1), I(2)});
  left.push_back({I(2), I(3)});
  left.push_back({I(3), I(4)});

  IdTable right(2, allocator());
  right.push_back({I(4), I(5)});
  right.push_back({I(6), I(7)});

  IdTable result(2, allocator());

  const std::vector<std::array<size_t, 2>> columnOrigins = {{0, 0}, {1, 1}};
  int leftWidth = left.numColumns();
  int rightWidth = right.numColumns();
  int outWidth = result.numColumns();
  Union U{Union::InvalidUnionOnlyUseForTestinTag{}};
  CALL_FIXED_SIZE((std::array{leftWidth, rightWidth, outWidth}),
                  &Union::computeUnion, &U, &result, left, right,
                  columnOrigins);

  ASSERT_EQ(5u, result.size());
  for (size_t i = 0; i < left.size(); i++) {
    ASSERT_EQ(left(i, 0), result(i, 0));
    ASSERT_EQ(left(i, 1), result(i, 1));
  }
  for (size_t i = 0; i < right.size(); i++) {
    ASSERT_EQ(right(i, 0), result(i + left.size(), 0));
    ASSERT_EQ(right(i, 1), result(i + left.size(), 1));
  }
}
