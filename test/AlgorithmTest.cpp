// Copyright 2022 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>

#include "util/Algorithm.h"

using namespace ad_utility;

// _____________________________________________________________________________
TEST(Algorithm, Contains) {
  std::vector v{1, 42, 5, 3};
  ASSERT_TRUE(
      std::ranges::all_of(v, [&v](const auto& el) { return contains(v, el); }));
  ASSERT_TRUE(std::ranges::none_of(
      std::vector{
          28,
          2,
          7,
      },
      [&v](const auto& el) { return contains(v, el); }));

  auto testStringLike = []<typename StringLike>() {
    StringLike s{"hal"};
    {
      std::vector<std::string> substrings{"h", "a", "l", "ha", "al", "hal"};
      ASSERT_TRUE(std::ranges::all_of(
          substrings, [&s](const auto& el) { return contains(s, el); }));
      std::vector<std::string> noSubstrings{"x", "hl", "hel"};
      ASSERT_TRUE(std::ranges::none_of(
          noSubstrings, [&s](const auto& el) { return contains(s, el); }));
    }
    {
      std::vector<std::string_view> substrings{"h",  "a",  "l",
                                               "ha", "al", "hal"};
      ASSERT_TRUE(std::ranges::all_of(
          substrings, [&s](const auto& el) { return contains(s, el); }));
      std::vector<std::string_view> noSubstrings{"x", "hl", "hel"};
      ASSERT_TRUE(std::ranges::none_of(
          noSubstrings, [&s](const auto& el) { return contains(s, el); }));
    }

    std::vector<char> subchars{'h', 'a', 'l'};
    ASSERT_TRUE(std::ranges::all_of(
        subchars, [&s](const auto& el) { return contains(s, el); }));

    std::vector<char> noSubchars{'i', 'b', 'm'};
    ASSERT_TRUE(std::ranges::none_of(
        noSubchars, [&s](const auto& el) { return contains(s, el); }));
  };
  testStringLike.template operator()<std::string>();
  testStringLike.template operator()<std::string_view>();
}

// _____________________________________________________________________________
TEST(Algorithm, ContainsIF) {
  std::vector v{1, 3, 42};
  ASSERT_TRUE(contains_if(v, [](const auto& el) { return el > 5; }));
  ASSERT_TRUE(contains_if(v, [](const auto& el) { return el == 42; }));

  ASSERT_FALSE(contains_if(v, [](const auto& el) { return el == 5; }));
  ASSERT_FALSE(contains_if(v, [](const auto& el) { return el < 0; }));
}

// _____________________________________________________________________________
TEST(Algorithm, AppendVector) {
  using V = std::vector<std::string>;
  V v{"1", "2", "7"};
  V v2{"3", "9", "16"};
  auto v2Copy = v2;
  appendVector(v, v2);
  ASSERT_EQ(v, (V{"1", "2", "7", "3", "9", "16"}));
  // `v2` was copied from.
  ASSERT_EQ(v2, v2Copy);

  V v3{"-18", "0"};
  appendVector(v, std::move(v3));
  ASSERT_EQ(v, (V{"1", "2", "7", "3", "9", "16", "-18", "0"}));
  // The individual elements from `v3` were moved from.
  ASSERT_EQ(v3.size(), 2u);
  ASSERT_TRUE(v3[0].empty());
  ASSERT_TRUE(v3[1].empty());
}

// _____________________________________________________________________________
TEST(Algorithm, Transform) {
  std::vector<std::string> v{"hi", "bye", "why"};
  auto vCopy = v;
  auto v2 = transform(v, [](const std::string& s) { return s.substr(1); });
  ASSERT_EQ((std::vector<std::string>{"i", "ye", "hy"}), v2);
  ASSERT_EQ(vCopy, v);
  // Transform using the moved values from `v`
  auto v3 = transform(std::move(v), [](std::string s) {
    s.push_back('x');
    return s;
  });
  ASSERT_EQ((std::vector<std::string>{"hix", "byex", "whyx"}), v3);
  ASSERT_EQ(3u, v.size());
  // The individual elements of `v` were moved from.
  ASSERT_TRUE(std::ranges::all_of(v, &std::string::empty));
}

// _____________________________________________________________________________
TEST(Algorithm, Flatten) {
  std::vector<std::vector<std::string>> v{{"hi"}, {"bye", "why"}, {"me"}};
  auto v3 = flatten(std::move(v));
  ASSERT_EQ((std::vector<std::string>{"hi", "bye", "why", "me"}), v3);
  ASSERT_EQ(3u, v.size());
  // The individual elements of `v` were moved from.
  ASSERT_TRUE(std::ranges::all_of(v, [](const auto& inner) {
    return std::ranges::all_of(inner, &std::string::empty);
  }));
}

// _____________________________________________________________________________
TEST(AlgorithmTest, removeDuplicates) {
  // Test with ints.
  ASSERT_EQ(ad_utility::removeDuplicates(std::vector<int>{4, 6, 6, 2, 2, 4, 2}),
            (std::vector<int>{4, 6, 2}));
  // Test with strings.
  std::string s1 = "four";
  std::string s2 = "six";
  std::string s3 = "abcdefghijklmnopqrstuvwxzy";
  ASSERT_EQ(ad_utility::removeDuplicates(
                std::vector<std::string>{s1, s2, s1, s1, s3, s1, s3}),
            (std::vector<std::string>{s1, s2, s3}));
  // Test with empty input.
  ASSERT_EQ(ad_utility::removeDuplicates(std::vector<int>{}),
            (std::vector<int>{}));
};

// _____________________________________________________________________________
TEST(AlgorithmTest, ZipVectors) {
  // Vectors of different size are not allowed.
  ASSERT_ANY_THROW(
      zipVectors(std::vector<size_t>{1}, std::vector<size_t>{1, 2}));

  // Do a simple test.
  std::vector<char> charVector{'a', 'b', 'c'};
  std::vector<float> floatVector{4.0, 4.1, 4.2};
  const auto& combinedVector = zipVectors(charVector, floatVector);

  for (size_t i = 0; i < charVector.size(); i++) {
    ASSERT_EQ(charVector.at(i), combinedVector.at(i).first);
  }
  for (size_t i = 0; i < charVector.size(); i++) {
    ASSERT_FLOAT_EQ(floatVector.at(i), combinedVector.at(i).second);
  }
};
