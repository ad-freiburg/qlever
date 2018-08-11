// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#include <gtest/gtest.h>
#include <string>
#include "../src/parser/Tokenizer.h"

using std::string;
using std::wstring;
TEST(TokenTest, Numbers) {
  TurtleToken t;

  wstring integer = L"+235632";
  wstring decimal = L"-235632.23";
  ASSERT_TRUE(std::regex_match(integer, t.Integer));
  ASSERT_TRUE(std::regex_match(decimal, t.Decimal));
  ASSERT_FALSE(std::regex_match(integer, t.Decimal));
  ASSERT_TRUE(std::regex_match(L"A", t.PnCharsBase));
  ASSERT_TRUE(std::regex_match(L"\u00dd", t.PnCharsBase));
  ASSERT_TRUE(std::regex_search(L"\u00DD", t.PnCharsBase));
  ASSERT_TRUE(std::regex_search(L"\u00De", t.PnCharsBase));
  ASSERT_FALSE(std::regex_search(L"\u00D0", t.PnCharsBase));
}
