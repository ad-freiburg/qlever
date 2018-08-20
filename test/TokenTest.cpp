// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#include <gtest/gtest.h>
#include <string>
#include "../src/parser/Tokenizer.h"

using std::string;
TEST(TokenTest, Numbers) {
  TurtleToken t;

  string integer = "235632";
  string decimal = u8"-235632.23";
  ASSERT_TRUE(RE2::FullMatch("123", "[123]+"));
  ASSERT_TRUE(RE2::FullMatch("123", "[1-3]+"));
  ASSERT_TRUE(RE2::FullMatch("234", t.Integer));
  ASSERT_TRUE(RE2::FullMatch(integer, t.Integer));
  ASSERT_TRUE(RE2::FullMatch(decimal, t.Decimal));
  ASSERT_FALSE(RE2::FullMatch(integer, t.Decimal));
  ASSERT_TRUE(RE2::FullMatch(u8"A", t.PnCharsBase));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00dd", t.PnCharsBase));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00DD", t.PnCharsBase));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00De", t.PnCharsBase));
  ASSERT_FALSE(RE2::FullMatch(u8"\u00D7", t.PnCharsBase));
}

TEST(TokenizerTest, Compilation) {
  std::string s(u8"    #comment of some way\n  start");
  Tokenizer tok(s.data(), s.size());
  auto [success, ws] = tok.getNextToken(tok._tokens.Comment);
  (void)ws;
  ASSERT_FALSE(success);
  tok.skipWhitespaceAndComments();
  ASSERT_EQ(tok._data.begin() - s.data(), 27);
}

