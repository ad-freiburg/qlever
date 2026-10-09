// Copyright 2022 - 2023, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <numeric>
#include <vector>

#include "util/Algorithm.h"
#include "util/GTestHelpers.h"
#include "util/HashMap.h"
#include "util/Random.h"
#include "util/TransparentFunctors.h"

using namespace ad_utility;

namespace {
template <typename StringLike>
void testStringLike() {
  StringLike s{"hal"};
  {
    std::vector<std::string> substrings{"h", "a", "l", "ha", "al", "hal"};
    ASSERT_TRUE(ql::ranges::all_of(
        substrings, [&s](const auto& el) { return contains(s, el); }));
    std::vector<std::string> noSubstrings{"x", "hl",
                                          "hel"};  // codespell-ignore
    ASSERT_TRUE(ql::ranges::none_of(
        noSubstrings, [&s](const auto& el) { return contains(s, el); }));
  }
  {
    std::vector<std::string_view> substrings{"h", "a", "l", "ha", "al", "hal"};
    ASSERT_TRUE(ql::ranges::all_of(
        substrings, [&s](const auto& el) { return contains(s, el); }));
    std::vector<std::string_view> noSubstrings{"x", "hl",
                                               "hel"};  // codespell-ignore
    ASSERT_TRUE(ql::ranges::none_of(
        noSubstrings, [&s](const auto& el) { return contains(s, el); }));
  }

  std::vector<char> subchars{'h', 'a', 'l'};
  ASSERT_TRUE(ql::ranges::all_of(
      subchars, [&s](const auto& el) { return contains(s, el); }));

  std::vector<char> noSubchars{'i', 'b', 'm'};
  ASSERT_TRUE(ql::ranges::none_of(
      noSubchars, [&s](const auto& el) { return contains(s, el); }));
}
}  // namespace

// _____________________________________________________________________________
TEST(Algorithm, Contains) {
  std::vector v{1, 42, 5, 3};
  ASSERT_TRUE(
      ql::ranges::all_of(v, [&v](const auto& el) { return contains(v, el); }));
  ASSERT_TRUE(ql::ranges::none_of(
      std::vector{
          28,
          2,
          7,
      },
      [&v](const auto& el) { return contains(v, el); }));

  EXPECT_TRUE(contains((std::unordered_set{3, 4, 5}), 5));
  EXPECT_TRUE(contains((ad_utility::HashSet<int>{3, 4, 5}), 5));

  testStringLike<std::string>();
  testStringLike<std::string_view>();
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
TEST(Algorithm, FindOptional) {
  // Key found in a HashMap returns the mapped value.
  HashMap<std::string, int> map{{"foo", 42}, {"bar", 7}};
  {
    auto result = findOptional(map, std::string{"foo"});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 42);
  }
  {
    auto result = findOptional(map, std::string{"bar"});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 7);
  }

  // Key not found returns boost::none.
  {
    auto result = findOptional(map, std::string{"missing"});
    ASSERT_FALSE(result.has_value());
  }

  // Empty map always returns boost::none.
  {
    HashMap<std::string, int> emptyMap;
    auto result = findOptional(emptyMap, std::string{"any"});
    ASSERT_FALSE(result.has_value());
  }

  // The returned optional holds a reference to the value in the map.
  {
    HashMap<std::string, std::string> strMap{{"key", "value"}};
    auto result = findOptional(strMap, std::string{"key"});
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(&result.value(), &strMap["key"]);
  }

  // Works with std::map as well.
  {
    std::map<int, std::string> stdMap{{1, "one"}, {2, "two"}};
    auto found = findOptional(stdMap, 2);
    ASSERT_TRUE(found.has_value());
    ASSERT_EQ(found.value(), "two");

    auto notFound = findOptional(stdMap, 3);
    ASSERT_FALSE(notFound.has_value());
  }
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
  ASSERT_TRUE(ql::ranges::all_of(v, &std::string::empty));
}

// _____________________________________________________________________________
TEST(Algorithm, Flatten) {
  std::vector<std::vector<std::string>> v{{"hi"}, {"bye", "why"}, {"me"}};
  auto v3 = flatten(std::move(v));
  ASSERT_EQ((std::vector<std::string>{"hi", "bye", "why", "me"}), v3);
  ASSERT_EQ(3u, v.size());
  // The individual elements of `v` were moved from.
  ASSERT_TRUE(ql::ranges::all_of(v, [](const auto& inner) {
    return ql::ranges::all_of(inner, &std::string::empty);
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

// ___________________________________________________________________________
TEST(AlgorithmTest, transformArray) {
  using namespace std::string_literals;
  auto inc = [](int x) { return x + 2; };
  ASSERT_EQ(transformArray(std::array{1, 3, 5}, inc), (std::array{3, 5, 7}));
  auto str = [](size_t x) { return std::string(x, 'a'); };
  ASSERT_EQ(transformArray(std::array{1, 3, 5}, str),
            (std::array{"a"s, "aaa"s, "aaaaa"s}));
}

TEST(AlgorithmTest, lowerUpperBoundIterator) {
  std::vector<size_t> input;
  FastRandomIntGenerator<size_t> randomGenerator;
  ql::ranges::generate_n(std::back_inserter(input), 1000,
                         std::ref(randomGenerator));

  auto compForLowerBound = [](auto iterator, size_t value) {
    return *iterator < value;
  };
  auto compForUpperBound = [](size_t value, auto iterator) {
    return value < *iterator;
  };
  for (auto value : input) {
    EXPECT_EQ(ad_utility::lower_bound_iterator(input.begin(), input.end(),
                                               value, compForLowerBound),
              ql::ranges::lower_bound(input, value));
    EXPECT_EQ(ad_utility::upper_bound_iterator(input.begin(), input.end(),
                                               value, compForUpperBound),
              ql::ranges::upper_bound(input, value));
  }
}

// ____________________________________________________________________________
TEST(AlgorithmTest, SetDifference) {
  using Vec = std::vector<int>;

  // Helper: run setDifference in-place (output = begin(r1)) and check result.
  auto testInplace = [](Vec r1, Vec r2, const Vec& expected,
                        source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(loc);
    auto newEnd = inplace_set_difference(r1, r2);
    r1.erase(newEnd, r1.end());
    EXPECT_EQ(r1, expected);
  };

  testInplace({}, {}, {});
  testInplace({}, {1, 2, 3}, {});
  testInplace({1, 2, 3}, {}, {1, 2, 3});
  testInplace({1, 2, 3}, {1, 2, 3}, {});
  testInplace({1, 2, 3}, {2}, {1, 3});
  testInplace({1, 2, 3}, {1}, {2, 3});
  testInplace({1, 2, 3}, {3}, {1, 2});
  testInplace({1, 2, 3}, {1, 3}, {2});
  testInplace({2, 3}, {1, 3}, {2});
  testInplace({1, 2}, {1, 4}, {2});
  testInplace({2, 3, 4}, {1, 3, 5}, {2, 4});
  testInplace({2, 3, 4}, {1, 3, 3, 3, 5}, {2, 4});
  testInplace({2, 3, 4}, {1, 5}, {2, 3, 4});
  testInplace({1, 2, 3, 4, 5}, {1}, {2, 3, 4, 5});
  testInplace({1, 2, 3, 4, 5}, {2, 3}, {1, 4, 5});
  // Duplicates in `r1`: `std::set_difference` semantics, each `r2` element
  // cancels one equivalent `r1` element.
  testInplace({1, 1, 2}, {1}, {1, 2});
  testInplace({1, 1, 2}, {1, 1}, {2});

  if (areExpensiveChecksEnabled) {
    auto assertionFailed = [](const std::string& assertion) {
      return ::testing::HasSubstr("Assertion `" + assertion + "` failed");
    };
    // r1 unsorted triggers the first AD_EXPENSIVE_CHECK.
    AD_EXPECT_THROW_WITH_MESSAGE(
        inplace_set_difference(Vec{3, 1, 2}, Vec{1}),
        assertionFailed("ql::ranges::is_sorted(r1, comp, proj1)"));
    // r2 unsorted triggers the second AD_EXPENSIVE_CHECK.
    AD_EXPECT_THROW_WITH_MESSAGE(
        inplace_set_difference(Vec{1, 2, 3}, Vec{3, 1}),
        assertionFailed("ql::ranges::is_sorted(r2, comp, proj2)"));
  }

  // With projection: compare only the first element of pairs.
  using Pair = std::pair<int, int>;
  using PVec = std::vector<Pair>;
  using Proj = MemberProjection<&Pair::first>;

  auto testProj = [&](PVec r1, PVec r2, const PVec& expected,
                      source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(loc);
    auto newEnd = inplace_set_difference(r1, r2, std::less{}, Proj{}, Proj{});
    r1.erase(newEnd, r1.end());
    EXPECT_EQ(r1, expected);
  };

  testProj({{1, 0}, {2, 0}, {3, 0}}, {}, {{1, 0}, {2, 0}, {3, 0}});
  testProj({{1, 0}, {2, 0}, {3, 0}}, {{2, 9}}, {{1, 0}, {3, 0}});
  testProj({{1, 0}, {2, 0}, {3, 0}}, {{1, 9}, {2, 9}, {3, 9}}, {});
  // Output contains only elements from `r1`, never from `r2`. Each `r2` element
  // cancels one equivalent `r1` element.
  testProj({{9, 1}, {9, 2}}, {}, {{9, 1}, {9, 2}});
  testProj({{9, 1}, {9, 2}}, {{9, 3}}, {{9, 2}});
  testProj({{9, 1}, {9, 2}}, {{9, 3}, {9, 4}}, {});
}

// _____________________________________________________________________________
TEST(AlgorithmTest, gallopingPartitionPoint) {
  // Compare against `std::partition_point` for all the partition points of
  // ranges of different sizes (the sizes cover all the cases in which the
  // galloping steps exactly hit or overshoot the end of the range).
  for (size_t size = 0; size < 70; ++size) {
    for (size_t partitionPoint = 0; partitionPoint <= size; ++partitionPoint) {
      std::vector<size_t> values(size);
      std::iota(values.begin(), values.end(), 0);
      auto pred = [partitionPoint](size_t value) {
        return value < partitionPoint;
      };
      auto it = gallopingPartitionPoint(values.begin(), values.end(), pred);
      EXPECT_EQ(it - values.begin(), static_cast<ptrdiff_t>(partitionPoint))
          << "size: " << size << ", partitionPoint: " << partitionPoint;
      EXPECT_EQ(it, std::partition_point(values.begin(), values.end(), pred));
    }
  }

  // A partition point close to the beginning of a large range is found with
  // only a few calls to the predicate.
  std::vector<int> large(1'000'000, 1);
  large.at(0) = 0;
  large.at(1) = 0;
  size_t numCalls = 0;
  auto isZero = [&numCalls](int value) {
    ++numCalls;
    return value == 0;
  };
  auto it = gallopingPartitionPoint(large.begin(), large.end(), isZero);
  EXPECT_EQ(it - large.begin(), 2);
  EXPECT_LE(numCalls, 4u);

  // A partition point at the end of a large range only needs a logarithmic
  // number of calls.
  ql::ranges::fill(large, 0);
  numCalls = 0;
  it = gallopingPartitionPoint(large.begin(), large.end(), isZero);
  EXPECT_EQ(it, large.end());
  EXPECT_LE(numCalls, 45u);

  // The predicate is also applied correctly if it is a member pointer.
  std::vector<std::pair<bool, int>> pairs{{true, 0}, {true, 1}, {false, 2}};
  EXPECT_EQ(gallopingPartitionPoint(pairs.begin(), pairs.end(),
                                    &std::pair<bool, int>::first) -
                pairs.begin(),
            2);
}
