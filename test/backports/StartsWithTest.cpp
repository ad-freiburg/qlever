//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"

class StartsWithTest : public ::testing::Test {
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
// Tests for string_view with string_view prefix
TEST_F(StartsWithTest, StringViewWithStringViewPrefix) {
  EXPECT_TRUE(ql::starts_with(sv, std::string_view("Hello")));
  EXPECT_TRUE(ql::starts_with(sv, std::string_view("Hello, World!")));
  EXPECT_TRUE(ql::starts_with(sv, std::string_view("")));
  EXPECT_FALSE(ql::starts_with(sv, std::string_view("World")));
  EXPECT_FALSE(ql::starts_with(sv, std::string_view("Hello, World! Extra")));

  // Edge cases
  EXPECT_TRUE(ql::starts_with(empty_sv, std::string_view("")));
  EXPECT_FALSE(ql::starts_with(empty_sv, std::string_view("a")));
  EXPECT_TRUE(ql::starts_with(single_char_sv, std::string_view("A")));
  EXPECT_FALSE(ql::starts_with(single_char_sv, std::string_view("B")));
}

// _____________________________________________________________________________
// Tests for string_view with char prefix
TEST_F(StartsWithTest, StringViewWithCharPrefix) {
  EXPECT_TRUE(ql::starts_with(sv, 'H'));
  EXPECT_FALSE(ql::starts_with(sv, 'W'));
  EXPECT_FALSE(ql::starts_with(sv, 'h'));

  // Edge cases
  EXPECT_FALSE(ql::starts_with(empty_sv, 'a'));
  EXPECT_TRUE(ql::starts_with(single_char_sv, 'A'));
  EXPECT_FALSE(ql::starts_with(single_char_sv, 'B'));
}

// _____________________________________________________________________________
// Tests for string_view with C-string prefix
TEST_F(StartsWithTest, StringViewWithCStringPrefix) {
  EXPECT_TRUE(ql::starts_with(sv, "Hello"));
  EXPECT_TRUE(ql::starts_with(sv, "Hello, World!"));
  EXPECT_TRUE(ql::starts_with(sv, ""));
  EXPECT_FALSE(ql::starts_with(sv, "World"));
  EXPECT_FALSE(ql::starts_with(sv, "Hello, World! Extra"));

  // Edge cases
  EXPECT_TRUE(ql::starts_with(empty_sv, ""));
  EXPECT_FALSE(ql::starts_with(empty_sv, "a"));
  EXPECT_TRUE(ql::starts_with(single_char_sv, "A"));
  EXPECT_FALSE(ql::starts_with(single_char_sv, "B"));
}

// _____________________________________________________________________________
// Tests for string with string_view prefix
TEST_F(StartsWithTest, StringWithStringViewPrefix) {
  EXPECT_TRUE(ql::starts_with(str, std::string_view("Hello")));
  EXPECT_TRUE(ql::starts_with(str, std::string_view("Hello, World!")));
  EXPECT_TRUE(ql::starts_with(str, std::string_view("")));
  EXPECT_FALSE(ql::starts_with(str, std::string_view("World")));
  EXPECT_FALSE(ql::starts_with(str, std::string_view("Hello, World! Extra")));

  // Edge cases
  EXPECT_TRUE(ql::starts_with(empty_str, std::string_view("")));
  EXPECT_FALSE(ql::starts_with(empty_str, std::string_view("a")));
  EXPECT_TRUE(ql::starts_with(single_char_str, std::string_view("A")));
  EXPECT_FALSE(ql::starts_with(single_char_str, std::string_view("B")));
}

// _____________________________________________________________________________
// Tests for string with char prefix
TEST_F(StartsWithTest, StringWithCharPrefix) {
  EXPECT_TRUE(ql::starts_with(str, 'H'));
  EXPECT_FALSE(ql::starts_with(str, 'W'));
  EXPECT_FALSE(ql::starts_with(str, 'h'));

  // Edge cases
  EXPECT_FALSE(ql::starts_with(empty_str, 'a'));
  EXPECT_TRUE(ql::starts_with(single_char_str, 'A'));
  EXPECT_FALSE(ql::starts_with(single_char_str, 'B'));
}

// _____________________________________________________________________________
// Tests for string with C-string prefix
TEST_F(StartsWithTest, StringWithCStringPrefix) {
  EXPECT_TRUE(ql::starts_with(str, "Hello"));
  EXPECT_TRUE(ql::starts_with(str, "Hello, World!"));
  EXPECT_TRUE(ql::starts_with(str, ""));
  EXPECT_FALSE(ql::starts_with(str, "World"));
  EXPECT_FALSE(ql::starts_with(str, "Hello, World! Extra"));

  // Edge cases
  EXPECT_TRUE(ql::starts_with(empty_str, ""));
  EXPECT_FALSE(ql::starts_with(empty_str, "a"));
  EXPECT_TRUE(ql::starts_with(single_char_str, "A"));
  EXPECT_FALSE(ql::starts_with(single_char_str, "B"));
}

// _____________________________________________________________________________
// Tests with wide characters
TEST_F(StartsWithTest, WideCharacterSupport) {
  std::wstring wstr = L"Hello, World!";
  std::wstring_view wsv = wstr;

  EXPECT_TRUE(ql::starts_with(wsv, std::wstring_view(L"Hello")));
  EXPECT_TRUE(ql::starts_with(wsv, L'H'));
  EXPECT_TRUE(ql::starts_with(wsv, L"Hello"));

  EXPECT_TRUE(ql::starts_with(wstr, std::wstring_view(L"Hello")));
  EXPECT_TRUE(ql::starts_with(wstr, L'H'));
  EXPECT_TRUE(ql::starts_with(wstr, L"Hello"));

  EXPECT_FALSE(ql::starts_with(wsv, std::wstring_view(L"World")));
  EXPECT_FALSE(ql::starts_with(wsv, L'W'));
  EXPECT_FALSE(ql::starts_with(wsv, L"World"));
}

// _____________________________________________________________________________
// Tests for case sensitivity
TEST_F(StartsWithTest, CaseSensitivity) {
  EXPECT_TRUE(ql::starts_with(sv, "Hello"));
  EXPECT_FALSE(ql::starts_with(sv, "hello"));
  EXPECT_FALSE(ql::starts_with(sv, "HELLO"));

  EXPECT_TRUE(ql::starts_with(sv, 'H'));
  EXPECT_FALSE(ql::starts_with(sv, 'h'));
}

// _____________________________________________________________________________
// Tests for Unicode support
TEST_F(StartsWithTest, UnicodeSupport) {
  std::string unicode_str = "Héllo, Wörld!";
  std::string_view unicode_sv = unicode_str;

  EXPECT_TRUE(ql::starts_with(unicode_sv, "Héllo"));
  EXPECT_TRUE(ql::starts_with(unicode_sv, 'H'));
  EXPECT_FALSE(ql::starts_with(unicode_sv, "Hello"));

  EXPECT_TRUE(ql::starts_with(unicode_str, "Héllo"));
  EXPECT_TRUE(ql::starts_with(unicode_str, 'H'));
  EXPECT_FALSE(ql::starts_with(unicode_str, "Hello"));
}

// _____________________________________________________________________________
// Performance and constexpr tests
TEST_F(StartsWithTest, ConstexprSupport) {
  constexpr std::string_view csv = "Hello";
  constexpr bool result1 = ql::starts_with(csv, 'H');
  constexpr bool result2 =
      ql::starts_with(csv, std::string_view("Hel"));  // codespell-ignore
  constexpr bool result3 = ql::starts_with(csv, "Hell");

  EXPECT_TRUE(result1);
  EXPECT_TRUE(result2);
  EXPECT_TRUE(result3);
}

// _____________________________________________________________________________
// Tests for types convertible to string_view or char
TEST_F(StartsWithTest, ConvertibleTypes) {
  // Test with std::string convertible to string_view
  std::string hello_string = "Hello";
  std::string h_string = "H";

  EXPECT_TRUE(ql::starts_with(sv, hello_string));
  EXPECT_TRUE(ql::starts_with(str, hello_string));
  EXPECT_FALSE(ql::starts_with(sv, std::string("World")));
  EXPECT_FALSE(ql::starts_with(str, std::string("World")));

  // Test with string literal as C-string (const char*)
  EXPECT_TRUE(ql::starts_with(sv, "Hello"));
  EXPECT_TRUE(ql::starts_with(str, "Hello"));
  EXPECT_FALSE(ql::starts_with(sv, "World"));
  EXPECT_FALSE(ql::starts_with(str, "World"));

  // Test with character types convertible to char
  char h_char = 'H';
  int h_int = 'H';  // ASCII value of 'H'

  EXPECT_TRUE(ql::starts_with(sv, h_char));
  EXPECT_TRUE(ql::starts_with(str, h_char));
  EXPECT_TRUE(ql::starts_with(sv, h_int));
  EXPECT_TRUE(ql::starts_with(str, h_int));

  char w_char = 'W';
  int w_int = 'W';

  EXPECT_FALSE(ql::starts_with(sv, w_char));
  EXPECT_FALSE(ql::starts_with(str, w_char));
  EXPECT_FALSE(ql::starts_with(sv, w_int));
  EXPECT_FALSE(ql::starts_with(str, w_int));
}

// _____________________________________________________________________________
// Tests for wide character convertible types
TEST_F(StartsWithTest, WideConvertibleTypes) {
  std::wstring wstr = L"Hello, World!";
  std::wstring_view wsv = wstr;

  // Test with std::wstring convertible to wstring_view
  std::wstring hello_wstring = L"Hello";

  EXPECT_TRUE(ql::starts_with(wsv, hello_wstring));
  EXPECT_TRUE(ql::starts_with(wstr, hello_wstring));
  EXPECT_FALSE(ql::starts_with(wsv, std::wstring(L"World")));
  EXPECT_FALSE(ql::starts_with(wstr, std::wstring(L"World")));

  // Test with wide character types
  wchar_t h_wchar = L'H';

  EXPECT_TRUE(ql::starts_with(wsv, h_wchar));
  EXPECT_TRUE(ql::starts_with(wstr, h_wchar));

  wchar_t w_wchar = L'W';

  EXPECT_FALSE(ql::starts_with(wsv, w_wchar));
  EXPECT_FALSE(ql::starts_with(wstr, w_wchar));
}
