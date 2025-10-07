// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"

class EndsWithTest : public ::testing::Test {
 protected:
  void SetUp() override {
    str = "Hello, World!";
    sv = str;
    empty_str = "";
    empty_sv = empty_str;
    single_char_str = "A";
    single_char_sv = single_char_str;
  }

  std::string str;
  std::string_view sv;
  std::string empty_str;
  std::string_view empty_sv;
  std::string single_char_str;
  std::string_view single_char_sv;
};

// _____________________________________________________________________________
// Tests for string_view with string_view suffix
TEST_F(EndsWithTest, StringViewWithStringViewSuffix) {
  EXPECT_TRUE(ql::ends_with(sv, std::string_view("World!")));
  EXPECT_TRUE(ql::ends_with(sv, std::string_view("Hello, World!")));
  EXPECT_TRUE(ql::ends_with(sv, std::string_view("")));
  EXPECT_FALSE(ql::ends_with(sv, std::string_view("Hello")));
  EXPECT_FALSE(ql::ends_with(sv, std::string_view("Extra Hello, World!")));

  // Edge cases
  EXPECT_TRUE(ql::ends_with(empty_sv, std::string_view("")));
  EXPECT_FALSE(ql::ends_with(empty_sv, std::string_view("a")));
  EXPECT_TRUE(ql::ends_with(single_char_sv, std::string_view("A")));
  EXPECT_FALSE(ql::ends_with(single_char_sv, std::string_view("B")));
}

// _____________________________________________________________________________
// Tests for string_view with char suffix
TEST_F(EndsWithTest, StringViewWithCharSuffix) {
  EXPECT_TRUE(ql::ends_with(sv, '!'));
  EXPECT_FALSE(ql::ends_with(sv, 'H'));
  EXPECT_FALSE(ql::ends_with(sv, '.'));

  // Edge cases
  EXPECT_FALSE(ql::ends_with(empty_sv, 'a'));
  EXPECT_TRUE(ql::ends_with(single_char_sv, 'A'));
  EXPECT_FALSE(ql::ends_with(single_char_sv, 'B'));
}

// _____________________________________________________________________________
// Tests for string_view with C-string suffix
TEST_F(EndsWithTest, StringViewWithCStringSuffix) {
  EXPECT_TRUE(ql::ends_with(sv, "World!"));
  EXPECT_TRUE(ql::ends_with(sv, "Hello, World!"));
  EXPECT_TRUE(ql::ends_with(sv, ""));
  EXPECT_FALSE(ql::ends_with(sv, "Hello"));
  EXPECT_FALSE(ql::ends_with(sv, "Extra Hello, World!"));

  // Edge cases
  EXPECT_TRUE(ql::ends_with(empty_sv, ""));
  EXPECT_FALSE(ql::ends_with(empty_sv, "a"));
  EXPECT_TRUE(ql::ends_with(single_char_sv, "A"));
  EXPECT_FALSE(ql::ends_with(single_char_sv, "B"));
}

// _____________________________________________________________________________
// Tests for string with string_view suffix
TEST_F(EndsWithTest, StringWithStringViewSuffix) {
  EXPECT_TRUE(ql::ends_with(str, std::string_view("World!")));
  EXPECT_TRUE(ql::ends_with(str, std::string_view("Hello, World!")));
  EXPECT_TRUE(ql::ends_with(str, std::string_view("")));
  EXPECT_FALSE(ql::ends_with(str, std::string_view("Hello")));
  EXPECT_FALSE(ql::ends_with(str, std::string_view("Extra Hello, World!")));

  // Edge cases
  EXPECT_TRUE(ql::ends_with(empty_str, std::string_view("")));
  EXPECT_FALSE(ql::ends_with(empty_str, std::string_view("a")));
  EXPECT_TRUE(ql::ends_with(single_char_str, std::string_view("A")));
  EXPECT_FALSE(ql::ends_with(single_char_str, std::string_view("B")));
}

// _____________________________________________________________________________
// Tests for string with char suffix
TEST_F(EndsWithTest, StringWithCharSuffix) {
  EXPECT_TRUE(ql::ends_with(str, '!'));
  EXPECT_FALSE(ql::ends_with(str, 'H'));
  EXPECT_FALSE(ql::ends_with(str, '.'));

  // Edge cases
  EXPECT_FALSE(ql::ends_with(empty_str, 'a'));
  EXPECT_TRUE(ql::ends_with(single_char_str, 'A'));
  EXPECT_FALSE(ql::ends_with(single_char_str, 'B'));
}

// _____________________________________________________________________________
// Tests for string with C-string suffix
TEST_F(EndsWithTest, StringWithCStringSuffix) {
  EXPECT_TRUE(ql::ends_with(str, "World!"));
  EXPECT_TRUE(ql::ends_with(str, "Hello, World!"));
  EXPECT_TRUE(ql::ends_with(str, ""));
  EXPECT_FALSE(ql::ends_with(str, "Hello"));
  EXPECT_FALSE(ql::ends_with(str, "Extra Hello, World!"));

  // Edge cases
  EXPECT_TRUE(ql::ends_with(empty_str, ""));
  EXPECT_FALSE(ql::ends_with(empty_str, "a"));
  EXPECT_TRUE(ql::ends_with(single_char_str, "A"));
  EXPECT_FALSE(ql::ends_with(single_char_str, "B"));
}

// _____________________________________________________________________________
// Tests with wide characters
TEST_F(EndsWithTest, WideCharacterSupport) {
  std::wstring wstr = L"Hello, World!";
  std::wstring_view wsv = wstr;

  EXPECT_TRUE(ql::ends_with(wsv, std::wstring_view(L"World!")));
  EXPECT_TRUE(ql::ends_with(wsv, L'!'));
  EXPECT_TRUE(ql::ends_with(wsv, L"World!"));

  EXPECT_TRUE(ql::ends_with(wstr, std::wstring_view(L"World!")));
  EXPECT_TRUE(ql::ends_with(wstr, L'!'));
  EXPECT_TRUE(ql::ends_with(wstr, L"World!"));

  EXPECT_FALSE(ql::ends_with(wsv, std::wstring_view(L"Hello")));
  EXPECT_FALSE(ql::ends_with(wsv, L'H'));
  EXPECT_FALSE(ql::ends_with(wsv, L"Hello"));
}

// _____________________________________________________________________________
// Tests for case sensitivity
TEST_F(EndsWithTest, CaseSensitivity) {
  EXPECT_TRUE(ql::ends_with(sv, "World!"));
  EXPECT_FALSE(ql::ends_with(sv, "world!"));
  EXPECT_FALSE(ql::ends_with(sv, "WORLD!"));

  EXPECT_TRUE(ql::ends_with(sv, '!'));
  EXPECT_FALSE(ql::ends_with(sv, '.'));
}

// _____________________________________________________________________________
// Tests for Unicode support
TEST_F(EndsWithTest, UnicodeSupport) {
  std::string unicode_str = "Héllo, Wörld!";
  std::string_view unicode_sv = unicode_str;

  EXPECT_TRUE(ql::ends_with(unicode_sv, "Wörld!"));
  EXPECT_TRUE(ql::ends_with(unicode_sv, '!'));
  EXPECT_FALSE(ql::ends_with(unicode_sv, "World!"));

  EXPECT_TRUE(ql::ends_with(unicode_str, "Wörld!"));
  EXPECT_TRUE(ql::ends_with(unicode_str, '!'));
  EXPECT_FALSE(ql::ends_with(unicode_str, "World!"));
}

// _____________________________________________________________________________
// Performance and constexpr tests
TEST_F(EndsWithTest, ConstexprSupport) {
  constexpr std::string_view csv = "Hello";
  constexpr bool result1 = ql::ends_with(csv, 'o');
  constexpr bool result2 =
      ql::ends_with(csv, std::string_view("llo"));  // codespell-ignore
  constexpr bool result3 = ql::ends_with(csv, "ello");

  EXPECT_TRUE(result1);
  EXPECT_TRUE(result2);
  EXPECT_TRUE(result3);
}

// _____________________________________________________________________________
// Tests for types convertible to string_view or char
TEST_F(EndsWithTest, ConvertibleTypes) {
  // Test with std::string convertible to string_view
  std::string world_string = "World!";
  std::string exclamation_string = "!";

  EXPECT_TRUE(ql::ends_with(sv, world_string));
  EXPECT_TRUE(ql::ends_with(str, world_string));
  EXPECT_FALSE(ql::ends_with(sv, std::string("Hello")));
  EXPECT_FALSE(ql::ends_with(str, std::string("Hello")));

  // Test with string literal as C-string (const char*)
  EXPECT_TRUE(ql::ends_with(sv, "World!"));
  EXPECT_TRUE(ql::ends_with(str, "World!"));
  EXPECT_FALSE(ql::ends_with(sv, "Hello"));
  EXPECT_FALSE(ql::ends_with(str, "Hello"));

  // Test with character types convertible to char
  char exclamation_char = '!';
  int exclamation_int = '!';  // ASCII value of '!'

  EXPECT_TRUE(ql::ends_with(sv, exclamation_char));
  EXPECT_TRUE(ql::ends_with(str, exclamation_char));
  EXPECT_TRUE(ql::ends_with(sv, exclamation_int));
  EXPECT_TRUE(ql::ends_with(str, exclamation_int));

  char h_char = 'H';
  int h_int = 'H';

  EXPECT_FALSE(ql::ends_with(sv, h_char));
  EXPECT_FALSE(ql::ends_with(str, h_char));
  EXPECT_FALSE(ql::ends_with(sv, h_int));
  EXPECT_FALSE(ql::ends_with(str, h_int));
}

// _____________________________________________________________________________
// Tests for wide character convertible types
TEST_F(EndsWithTest, WideConvertibleTypes) {
  std::wstring wstr = L"Hello, World!";
  std::wstring_view wsv = wstr;

  // Test with std::wstring convertible to wstring_view
  std::wstring world_wstring = L"World!";

  EXPECT_TRUE(ql::ends_with(wsv, world_wstring));
  EXPECT_TRUE(ql::ends_with(wstr, world_wstring));
  EXPECT_FALSE(ql::ends_with(wsv, std::wstring(L"Hello")));
  EXPECT_FALSE(ql::ends_with(wstr, std::wstring(L"Hello")));

  // Test with wide character types
  wchar_t exclamation_wchar = L'!';

  EXPECT_TRUE(ql::ends_with(wsv, exclamation_wchar));
  EXPECT_TRUE(ql::ends_with(wstr, exclamation_wchar));

  wchar_t h_wchar = L'H';

  EXPECT_FALSE(ql::ends_with(wsv, h_wchar));
  EXPECT_FALSE(ql::ends_with(wstr, h_wchar));
}
