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

// _____________________________________________________________________________
TEST(TransparentFunctors, FirstOfPair) {
  std::pair<std::string, std::vector<int>> pair{"hello", {2}};
  auto first = ad_utility::first(pair);
  ASSERT_EQ("hello", first);
  ASSERT_EQ("hello", pair.first);

  auto firstMoved = ad_utility::first(std::move(pair));
  ASSERT_EQ("hello", firstMoved);
  ASSERT_TRUE(pair.first.empty());
}

// _____________________________________________________________________________
TEST(TransparentFunctors, SecondOfPair) {
  std::pair<std::string, std::vector<int>> pair{"hello", {2}};
  auto second = ad_utility::second(pair);
  ASSERT_EQ(std::vector{2}, second);
  ASSERT_EQ(std::vector{2}, pair.second);

  auto secondMoved = ad_utility::second(std::move(pair));
  ASSERT_EQ(std::vector{2}, secondMoved);
  ASSERT_TRUE(pair.second.empty());
}

// _____________________________________________________________________________
TEST(TransparentFunctors, MemberProjection) {
  using Pair = std::pair<int, std::string>;
  ad_utility::MemberProjection<&Pair::first> first;
  ad_utility::MemberProjection<&Pair::second> second;

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
    static constexpr ad_utility::MemberProjection<&Pair::second> second;
    static_assert(second(cp) == 11);
  }
}

// _____________________________________________________________________________
TEST(TransparentFunctors, Dereference) {
  {
    std::string s = "hello";
    std::string* ptr = &s;
    EXPECT_EQ(ad_utility::dereference(ptr), "hello");
    ad_utility::dereference(ptr) = "world";
    EXPECT_EQ(s, "world");
  }
  {
    std::vector<std::string> s{"foo", "bar", "baz"};
    std::vector<std::string*> pointers{&s[0], &s[1], &s[2]};
    auto values = pointers | ql::views::transform(ad_utility::dereference) |
                  ::ranges::to<std::vector>();
    EXPECT_THAT(values, testing::ElementsAre("foo", "bar", "baz"));
  }
}

// _____________________________________________________________________________
TEST(TransparentFunctors, ToBool) {
  EXPECT_TRUE(ad_utility::toBool(1));
  EXPECT_FALSE(ad_utility::toBool(0));

  std::optional<int> sth = 42;
  std::optional<int> null = std::nullopt;
  EXPECT_TRUE(ad_utility::toBool(sth));
  EXPECT_FALSE(ad_utility::toBool(null));

  std::vector<int> values{0, 1, 2, 0, 3};
  auto truthy = values | ql::views::filter(ad_utility::toBool) |
                ::ranges::to<std::vector>();
  EXPECT_THAT(truthy, testing::ElementsAre(1, 2, 3));
}

// _____________________________________________________________________________
TEST(TransparentFunctors, AddressOf) {
  {
    std::string s = "hello";
    EXPECT_EQ(ad_utility::addressOf(s), &s);
  }
  {
    std::vector<std::string> s{"foo", "bar", "baz"};
    auto pointers = s | ql::views::transform(ad_utility::addressOf) |
                    ::ranges::to<std::vector>();
    EXPECT_THAT(pointers, testing::ElementsAre(&s[0], &s[1], &s[2]));
  }
}

// _____________________________________________________________________________
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
