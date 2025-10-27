// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstring>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "util/ConstexprSmallString.h"

using ad_utility::ConstexprSmallString;

// _____________________________________________________________________________
// Compile-time tests using static_assert
// _____________________________________________________________________________

// Test compile-time construction from string literal
static constexpr ConstexprSmallString<10> compileTimeString = "hello";
static_assert(compileTimeString.size() == 5);
static_assert(compileTimeString[0] == 'h');
static_assert(compileTimeString[1] == 'e');
static_assert(compileTimeString[2] == 'l');
static_assert(compileTimeString[3] == 'l');
static_assert(compileTimeString[4] == 'o');

// Test compile-time comparison
static constexpr ConstexprSmallString<10> str1 = "abc";
static constexpr ConstexprSmallString<10> str2 = "abc";
static constexpr ConstexprSmallString<10> str3 = "abd";  // codespell-ignore
static_assert(str1 == str2);
static_assert(!(str1 == str3));
static_assert(str1 < str3);
static_assert(!(str3 < str1));

// Test empty string at compile time
static constexpr ConstexprSmallString<10> emptyStr = "";
static_assert(emptyStr.size() == 0);

// _____________________________________________________________________________
// Runtime tests
// _____________________________________________________________________________

// Test construction from string literal
TEST(ConstexprSmallString, ConstructionFromLiteral) {
  ConstexprSmallString<20> str = "test string";
  EXPECT_EQ(str.size(), 11);
  EXPECT_EQ(std::string_view(str), "test string");
}

// Test construction from empty string
TEST(ConstexprSmallString, ConstructionFromEmptyLiteral) {
  ConstexprSmallString<10> str = "";
  EXPECT_EQ(str.size(), 0);
  EXPECT_EQ(std::string_view(str), "");
}

// Test construction from string_view
TEST(ConstexprSmallString, ConstructionFromStringView) {
  std::string_view sv = "view";
  ConstexprSmallString<10> str(sv);
  EXPECT_EQ(str.size(), 4);
  EXPECT_EQ(std::string_view(str), "view");
}

// Test construction from empty string_view
TEST(ConstexprSmallString, ConstructionFromEmptyStringView) {
  std::string_view sv = "";
  ConstexprSmallString<10> str(sv);
  EXPECT_EQ(str.size(), 0);
  EXPECT_EQ(std::string_view(str), "");
}

// Test exception when string literal is too large
TEST(ConstexprSmallString, ThrowsOnTooLargeLiteral) {
  EXPECT_THROW((ConstexprSmallString<5>("toolong")), std::runtime_error);
}

// Test exception when string_view is too large
TEST(ConstexprSmallString, ThrowsOnTooLargeStringView) {
  std::string_view sv = "this is way too long";
  EXPECT_THROW((ConstexprSmallString<10>(sv)), std::runtime_error);
}

// Test operator[] with valid indices
TEST(ConstexprSmallString, OperatorBracketValid) {
  ConstexprSmallString<10> str = "abc";
  EXPECT_EQ(str[0], 'a');
  EXPECT_EQ(str[1], 'b');
  EXPECT_EQ(str[2], 'c');
}

// Test operator[] throws on out of range
TEST(ConstexprSmallString, OperatorBracketThrows) {
  ConstexprSmallString<10> str = "abc";
  EXPECT_THROW(str[3], std::out_of_range);
  EXPECT_THROW(str[10], std::out_of_range);
}

// Test size() method
TEST(ConstexprSmallString, Size) {
  ConstexprSmallString<20> str1 = "short";
  EXPECT_EQ(str1.size(), 5);

  ConstexprSmallString<20> str2 = "a bit longer";
  EXPECT_EQ(str2.size(), 12);

  ConstexprSmallString<10> empty = "";
  EXPECT_EQ(empty.size(), 0);
}

// Test equality operator
TEST(ConstexprSmallString, EqualityOperator) {
  ConstexprSmallString<10> str1 = "test";
  ConstexprSmallString<10> str2 = "test";
  ConstexprSmallString<10> str3 = "different";

  EXPECT_TRUE(str1 == str2);
  EXPECT_FALSE(str1 == str3);
}

// Test equality with different MaxSize
TEST(ConstexprSmallString, EqualityDifferentMaxSize) {
  ConstexprSmallString<10> str1 = "test";
  ConstexprSmallString<20> str2 = "test";

  // These have different types, so can't be compared with ==
  // But we can compare their string_view representations
  EXPECT_EQ(std::string_view(str1), std::string_view(str2));
}

// Test less-than operator
TEST(ConstexprSmallString, LessThanOperator) {
  ConstexprSmallString<10> str1 = "abc";
  ConstexprSmallString<10> str2 = "abd";  // codespell-ignore
  ConstexprSmallString<10> str3 = "ab";
  ConstexprSmallString<10> str4 = "abc";

  EXPECT_TRUE(str1 < str2);
  EXPECT_FALSE(str2 < str1);
  EXPECT_FALSE(str1 < str3);  // "abc" > "ab"
  EXPECT_TRUE(str3 < str1);
  EXPECT_FALSE(str1 < str4);  // equal strings
}

// Test conversion to string_view
TEST(ConstexprSmallString, ConversionToStringView) {
  ConstexprSmallString<15> str = "convert me";
  std::string_view sv = str;
  EXPECT_EQ(sv, "convert me");
  EXPECT_EQ(sv.size(), 10);
}

// Test with standard string functions via string_view
TEST(ConstexprSmallString, UseWithStringFunctions) {
  ConstexprSmallString<20> str = "hello world";
  std::string_view sv = str;

  EXPECT_TRUE(sv.starts_with("hello"));
  EXPECT_TRUE(sv.ends_with("world"));
  EXPECT_EQ(sv.find("world"), 6);
}

// Test ostream operator
TEST(ConstexprSmallString, OstreamOperator) {
  ConstexprSmallString<20> str = "output test";
  std::ostringstream oss;
  oss << str;
  EXPECT_EQ(oss.str(), "output test");
}

// Test hash consistency - equal strings should have equal hashes
TEST(ConstexprSmallString, HashConsistency) {
  ConstexprSmallString<10> str1 = "test";
  ConstexprSmallString<10> str2 = "test";

  std::hash<ConstexprSmallString<10>> hasher;
  EXPECT_EQ(hasher(str1), hasher(str2));
}

// Test hash with unordered_map
TEST(ConstexprSmallString, HashWithUnorderedMap) {
  std::unordered_map<ConstexprSmallString<10>, int> map;
  map[ConstexprSmallString<10>("key1")] = 1;
  map[ConstexprSmallString<10>("key2")] = 2;
  map[ConstexprSmallString<10>("key3")] = 3;

  EXPECT_EQ(map[ConstexprSmallString<10>("key1")], 1);
  EXPECT_EQ(map[ConstexprSmallString<10>("key2")], 2);
  EXPECT_EQ(map[ConstexprSmallString<10>("key3")], 3);
  EXPECT_EQ(map.size(), 3);
}

// Test hash with unordered_set
TEST(ConstexprSmallString, HashWithUnorderedSet) {
  std::unordered_set<ConstexprSmallString<15>> set;
  set.insert(ConstexprSmallString<15>("hello"));
  set.insert(ConstexprSmallString<15>("world"));
  set.insert(ConstexprSmallString<15>("hello"));  // duplicate

  EXPECT_EQ(set.size(), 2);
  EXPECT_TRUE(set.contains(ConstexprSmallString<15>("hello")));
  EXPECT_TRUE(set.contains(ConstexprSmallString<15>("world")));
  EXPECT_FALSE(set.contains(ConstexprSmallString<15>("foo")));
}

// Test that hash values match string_view hashes
TEST(ConstexprSmallString, HashMatchesStringView) {
  ConstexprSmallString<20> str = "test string";
  std::string_view sv = "test string";

  std::hash<ConstexprSmallString<20>> hasher1;
  std::hash<std::string_view> hasher2;

  EXPECT_EQ(hasher1(str), hasher2(sv));
}

// Test with special characters
TEST(ConstexprSmallString, SpecialCharacters) {
  // Note: String literals with \0 get terminated at that point,
  // so we test the characters we can
  ConstexprSmallString<20> str = "a\nb\tc";
  EXPECT_EQ(str.size(), 5);
  EXPECT_EQ(str[0], 'a');
  EXPECT_EQ(str[1], '\n');
  EXPECT_EQ(str[2], 'b');
  EXPECT_EQ(str[3], '\t');
  EXPECT_EQ(str[4], 'c');

  // Test tab and newline separately
  ConstexprSmallString<10> tab = "\t";
  EXPECT_EQ(tab.size(), 1);
  EXPECT_EQ(tab[0], '\t');

  ConstexprSmallString<10> newline = "\n";
  EXPECT_EQ(newline.size(), 1);
  EXPECT_EQ(newline[0], '\n');
}

// Test with UTF-8 characters (multi-byte sequences)
TEST(ConstexprSmallString, UTF8Characters) {
  ConstexprSmallString<20> str = "café";
  // "café" is 5 bytes: c(1) a(1) f(1) é(2)
  EXPECT_EQ(str.size(), 5);
  std::string_view sv = str;
  EXPECT_EQ(sv, "café");
}

// Test maximum size capacity
TEST(ConstexprSmallString, MaximumCapacity) {
  // MaxSize includes the null terminator, so we can store MaxSize-1 chars
  ConstexprSmallString<8> str = "1234567";  // 7 chars + '\0'
  EXPECT_EQ(str.size(), 7);
  EXPECT_EQ(std::string_view(str), "1234567");
}

// Test exactly at boundary
TEST(ConstexprSmallString, ExactlyAtBoundary) {
  // This should work: 5 chars + 1 null = 6 total, MaxSize = 6
  ConstexprSmallString<6> str = "12345";
  EXPECT_EQ(str.size(), 5);
}

// Test copy construction and assignment
TEST(ConstexprSmallString, CopyConstructionAndAssignment) {
  ConstexprSmallString<20> str1 = "original";
  ConstexprSmallString<20> str2 = str1;  // copy construction

  EXPECT_EQ(str1, str2);
  EXPECT_EQ(std::string_view(str2), "original");

  ConstexprSmallString<20> str3 = "different";
  str3 = str1;  // copy assignment
  EXPECT_EQ(str3, str1);
}

// Test null-termination
TEST(ConstexprSmallString, NullTermination) {
  ConstexprSmallString<10> str = "test";
  // Verify that we can safely use the underlying data as a C string
  EXPECT_EQ(std::strlen(str._characters.data()), 4);
  EXPECT_EQ(str._characters[4], '\0');
}

// Test constexpr evaluation in runtime context
TEST(ConstexprSmallString, ConstexprInRuntime) {
  constexpr ConstexprSmallString<10> str = "constexpr";
  EXPECT_EQ(str.size(), 9);
  EXPECT_EQ(std::string_view(str), "constexpr");
}

// Test comparison with lexicographical ordering
TEST(ConstexprSmallString, LexicographicalOrdering) {
  ConstexprSmallString<10> a = "a";
  ConstexprSmallString<10> aa = "aa";
  ConstexprSmallString<10> b = "b";
  ConstexprSmallString<10> ab = "ab";

  EXPECT_TRUE(a < aa);
  EXPECT_TRUE(a < b);
  EXPECT_TRUE(aa < b);
  EXPECT_TRUE(a < ab);
  EXPECT_TRUE(ab < b);
}
