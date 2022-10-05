//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author:
//   2022 -    Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>
#include <util/TransparentFunctors.h>

#include <string>
#include <vector>

TEST(TransparentFunctors, FirstOfPair) {
  std::pair<std::string, std::vector<int>> pair{"hello", {2}};
  auto first = ad_utility::first(pair);
  ASSERT_EQ("hello", first);
  ASSERT_EQ("hello", pair.first);

  auto firstMoved = ad_utility::first(std::move(pair));
  ASSERT_EQ("hello", firstMoved);
  ASSERT_TRUE(pair.first.empty());
}

TEST(TransparentFunctors, SecondOfPair) {
  std::pair<std::string, std::vector<int>> pair{"hello", {2}};
  auto second = ad_utility::second(pair);
  ASSERT_EQ(std::vector{2}, second);
  ASSERT_EQ(std::vector{2}, pair.second);

  auto secondMoved = ad_utility::second(std::move(pair));
  ASSERT_EQ(std::vector{2}, secondMoved);
  ASSERT_TRUE(pair.second.empty());
}
