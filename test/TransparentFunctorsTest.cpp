//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author:
//   2022 -    Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "util/GTestHelpers.h"
#include "util/TransparentFunctors.h"
#include "util/Views.h"

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

TEST(TransparentFunctors, OptionalHandling) {
  {
    std::optional<std::string> null = std::nullopt;
    std::optional<std::string> sth = "sth";
    EXPECT_FALSE(ad_utility::hasValue(null));
    EXPECT_TRUE(ad_utility::hasValue(sth));
    AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::value(null), testing::_);
    EXPECT_THAT(ad_utility::value(sth), testing::StrEq("sth"));
    AD_EXPECT_NULLOPT(null);
    EXPECT_THAT(sth.value(), testing::StrEq("sth"));
    AD_EXPECT_NULLOPT(ad_utility::exchange(null));
    EXPECT_THAT(ad_utility::exchange(sth),
                testing::Optional(testing::StrEq("sth")));
    AD_EXPECT_NULLOPT(null);
    AD_EXPECT_NULLOPT(sth);
  }
  {
    std::vector<std::optional<std::string>> s{"foo", std::nullopt, std::nullopt,
                                              "bar"};
    auto filtered = s | ql::views::filter(ad_utility::hasValue) |
                    ql::views::transform(ad_utility::value) |
                    ::ranges::to<std::vector>();
    EXPECT_THAT(filtered, testing::ElementsAre(testing::StrEq("foo"),
                                               testing::StrEq("bar")));
    auto moved = s | ql::views::filter(ad_utility::hasValue) |
                 ql::views::transform(ad_utility::exchange) |
                 ql::views::transform(ad_utility::value) |
                 ::ranges::to<std::vector>();
    EXPECT_THAT(s, testing::Each(testing::Eq(std::nullopt)));
    EXPECT_THAT(moved, testing::ElementsAre(testing::StrEq("foo"),
                                            testing::StrEq("bar")));
  }
}
