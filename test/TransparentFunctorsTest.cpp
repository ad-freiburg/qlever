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

TEST(TransparentFunctors, MemberProj) {
  using Pair = std::pair<int, std::string>;
  ad_utility::MemberFieldProj<&Pair::first> first;
  ad_utility::MemberFieldProj<&Pair::second> second;

  {
    Pair p{42, "hello"};

    EXPECT_EQ(first(p), 42);
    EXPECT_EQ(second(p), "hello");
    EXPECT_EQ(p.second, "hello");

    auto movedLabel = second(std::move(p));
    EXPECT_EQ(movedLabel, "hello");
    EXPECT_TRUE(p.second.empty());
  }
  {
    using Pairs = std::vector<Pair>;
    Pairs points{{3, "four"}, {1, "two"}, {5, "six"}};
    ql::ranges::sort(points, {}, first);
    EXPECT_THAT(points, testing::ElementsAreArray(
                            Pairs{{1, "two"}, {3, "four"}, {5, "six"}}));
  }
  {
    using Pair = std::pair<int, int>;
    static constexpr Pair cp{10, 11};
    static constexpr ad_utility::MemberFieldProj<&Pair::second> second;
    static_assert(second(cp) == 11);
  }
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
  }
  {
    std::vector<std::optional<std::string>> s{"foo", std::nullopt, std::nullopt,
                                              "bar"};
    auto filtered = s | ql::views::filter(ad_utility::hasValue) |
                    ql::views::transform(ad_utility::value) |
                    ::ranges::to<std::vector>();
    EXPECT_THAT(filtered, testing::ElementsAre(testing::StrEq("foo"),
                                               testing::StrEq("bar")));
  }
}
