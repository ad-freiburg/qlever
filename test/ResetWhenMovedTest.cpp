//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "gtest/gtest.h"
#include "util/ResetWhenMoved.h"

TEST(ResetWhenMoved, IntegerZero) {
  using R = ad_utility::ResetWhenMoved<int, 0>;
  R r;
  ASSERT_EQ(r, 0);
  r = 24;
  ASSERT_EQ(r, 24);

  R r2 = r;
  ASSERT_EQ(r, 24);
  ASSERT_EQ(r2, 24);

  R r3{std::move(r)};
  ASSERT_EQ(r, 0);
  ASSERT_EQ(r3, 24);

  r2 = std::move(r3);
  ASSERT_EQ(r3, 0);
  ASSERT_EQ(r2, 24);

  int x = r2;
  ASSERT_EQ(x, 24);
  ASSERT_EQ(r2, 24);
  r2 = 42;
  x = std::move(r2);
  ASSERT_EQ(r2, 0);
  ASSERT_EQ(x, 42);
}

TEST(ResetWhenMoved, IntegerFortyTwo) {
  using R = ad_utility::ResetWhenMoved<int, 42>;
  R r;
  ASSERT_EQ(r, 42);
  r = 24;
  ASSERT_EQ(r, 24);

  R r2 = r;
  ASSERT_EQ(r, 24);
  ASSERT_EQ(r2, 24);

  R r3{std::move(r)};
  ASSERT_EQ(r, 42);
  ASSERT_EQ(r3, 24);

  r2 = std::move(r3);
  ASSERT_EQ(r3, 42);
  ASSERT_EQ(r2, 24);

  int x = r2;
  ASSERT_EQ(x, 24);
  ASSERT_EQ(r2, 24);
  r2 = 43;
  x = std::move(r2);
  ASSERT_EQ(r2, 42);
  ASSERT_EQ(x, 43);
}
