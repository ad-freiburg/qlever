// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "../src/engine/Union.h"
#include "../src/global/Id.h"

TEST(UnionTest, computeUnion) {
  IdTable left(1);
  left.push_back({1});
  left.push_back({2});
  left.push_back({3});

  IdTable right(2);
  right.push_back({4, 5});
  right.push_back({6, 7});

  IdTable result(2);

  const std::vector<std::array<size_t, 2>> columnOrigins = {
      {0, 1}, {Union::NO_COLUMN, 0}};
  Union::computeUnion(&result, left, right, columnOrigins);

  ASSERT_EQ(5u, result.size());
  for (size_t i = 0; i < left.size(); i++) {
    ASSERT_EQ(left[i][0], result(i, 0));
    ASSERT_EQ(ID_NO_VALUE, result(i, 1));
  }
  for (size_t i = 0; i < right.size(); i++) {
    ASSERT_EQ(right[i][0], result(i + left.size(), 1));
    ASSERT_EQ(right(i, 1), result(i + left.size(), 0));
  }
}

TEST(UnionTest, computeUnionOptimized) {
  // the left and right data vectors will be deleted by the result tables
  IdTable left(2);
  left.push_back({1, 2});
  left.push_back({2, 3});
  left.push_back({3, 4});

  IdTable right(2);
  right.push_back({4, 5});
  right.push_back({6, 7});

  IdTable result(2);

  const std::vector<std::array<size_t, 2>> columnOrigins = {{0, 0}, {1, 1}};
  Union::computeUnion(&result, left, right, columnOrigins);

  ASSERT_EQ(5u, result.size());
  for (size_t i = 0; i < left.size(); i++) {
    ASSERT_EQ(left[i][0], result(i, 0));
    ASSERT_EQ(left[i][1], result(i, 1));
  }
  for (size_t i = 0; i < right.size(); i++) {
    ASSERT_EQ(right[i][0], result(i + left.size(), 0));
    ASSERT_EQ(right(i, 1), result(i + left.size(), 1));
  }
}
