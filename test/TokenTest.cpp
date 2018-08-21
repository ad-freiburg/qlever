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

  string integer1 = "235632";
  string integer2 = "-342";
  string integer3 = "+5425";
  string noInteger = "+54a";

  string decimal1 = u8"-235632.23";
  string decimal2 = u8"+23832.23";
  string decimal3 = u8"32.3";
  string noDecimal = u8"-23.";

  string double1 = "e+3";
  string double2 = "E-92";
  string double3 = "+43.8e+3";
  string double4 = "-42.3e-2";
  string double5 = "-42.3E+3";

  ASSERT_TRUE(RE2::FullMatch(integer1, t.Integer, nullptr));
  ASSERT_TRUE(RE2::FullMatch(integer2, t.Integer));
  ASSERT_TRUE(RE2::FullMatch(integer3, t.Integer));
  ASSERT_FALSE(RE2::FullMatch(noInteger, t.Integer));
  ASSERT_FALSE(RE2::FullMatch(decimal1, t.Integer));

  ASSERT_TRUE(RE2::FullMatch(decimal1, t.Decimal, nullptr));
  ASSERT_TRUE(RE2::FullMatch(decimal2, t.Decimal));
  ASSERT_TRUE(RE2::FullMatch(decimal3, t.Decimal));
  ASSERT_FALSE(RE2::FullMatch(noDecimal, t.Decimal));
  ASSERT_FALSE(RE2::FullMatch(integer3, t.Decimal));
  ASSERT_FALSE(RE2::FullMatch(double2, t.Decimal));

  ASSERT_TRUE(RE2::FullMatch(double1, t.Double, nullptr));
  ASSERT_TRUE(RE2::FullMatch(double2, t.Double));
  ASSERT_TRUE(RE2::FullMatch(double3, t.Double));
  ASSERT_TRUE(RE2::FullMatch(double4, t.Double));
  ASSERT_TRUE(RE2::FullMatch(double5, t.Double));
  ASSERT_FALSE(RE2::FullMatch(decimal1, t.Double));
  ASSERT_FALSE(RE2::FullMatch(integer2, t.Double));
}

TEST(TokenizerTest, SingleChars) {
  TurtleToken t;

  ASSERT_TRUE(RE2::FullMatch(u8"A", t.PnCharsBaseString));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00dd", t.PnCharsBaseString));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00DD", t.PnCharsBaseString));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00De", t.PnCharsBaseString));
  ASSERT_FALSE(RE2::FullMatch(u8"\u00D7", t.PnCharsBaseString));
}

TEST(TokenizerTest, StringLiterals){
  TurtleToken t;
  string sQuote1 = "\"this is a quote \"";
  string sQuote2 =
      u8"\"this is a quote \' $@#ä\u1234 \U000A1234 \\\\ \\n \"";  // $@#\u3344\U00FF00DD\"";
  string sQuote3 = u8"\"\\uAB23SomeotherChars\"";
  string NoSQuote1 = "\"illegalQuoteBecauseOfNewline\n\"";
  string NoSQuote2 = "\"illegalQuoteBecauseOfBackslash\\  \"";
  ASSERT_TRUE(RE2::FullMatch(sQuote1, t.StringLiteralQuote, nullptr));
  ASSERT_TRUE(RE2::FullMatch(sQuote2, t.StringLiteralQuote, nullptr));
  ASSERT_TRUE(RE2::FullMatch(sQuote3, t.StringLiteralQuote, nullptr));
  ASSERT_FALSE(RE2::FullMatch(NoSQuote1, t.StringLiteralQuote, nullptr));
  ASSERT_FALSE(RE2::FullMatch(NoSQuote2, t.StringLiteralQuote, nullptr));

  string sSingleQuote1 = "\'this is a quote \'";
  string sSingleQuote2 =
      u8"\'this is a quote \" $@#ä\u1234 \U000A1234 \\\\ \\n \'";
  string sSingleQuote3 = u8"\'\\uAB23SomeotherChars\'";
  string NoSSingleQuote1 = "\'illegalQuoteBecauseOfNewline\n\'";
  string NoSSingleQuote2 = "\'illegalQuoteBecauseOfBackslash\\  \'";

  ASSERT_TRUE(
      RE2::FullMatch(sSingleQuote1, t.StringLiteralSingleQuote, nullptr));
  ASSERT_TRUE(
      RE2::FullMatch(sSingleQuote2, t.StringLiteralSingleQuote, nullptr));
  ASSERT_TRUE(
      RE2::FullMatch(sSingleQuote3, t.StringLiteralSingleQuote, nullptr));
  ASSERT_FALSE(RE2::FullMatch(NoSQuote1, t.StringLiteralSingleQuote, nullptr));
  ASSERT_FALSE(RE2::FullMatch(NoSQuote2, t.StringLiteralSingleQuote, nullptr));
}

TEST(TokenizerTest, Consume) {
  std::string s("bla");
  re2::StringPiece sp(s);
  std::string match;
  ASSERT_TRUE(RE2::Consume(&sp, "(bla)", &match));
  ASSERT_EQ(match, s);
}

TEST(TokenizerTest, WhitespaceAndComments) {
  TurtleToken t;
  ASSERT_TRUE(RE2::FullMatch(u8"  \t  \n", t.WsMultiple));
  ASSERT_TRUE(RE2::FullMatch(u8"# theseareComme$#n  \tts\n", t.Comment));
  ASSERT_TRUE(RE2::FullMatch(u8"#", u8"\\#"));
  ASSERT_TRUE(RE2::FullMatch(u8"\n", u8"\\n"));
  std::string s2(u8"#only Comment\n");
  Tokenizer tok(s2.data(), s2.size());
  tok.skipComments();
  ASSERT_EQ(tok.data().begin() - s2.data(), 14);
  std::string s(u8"    #comment of some way\n  start");
  tok.reset(s.data(), s.size());
  auto [success2, ws] = tok.getNextToken(tok._tokens.Comment);
  (void)ws;
  ASSERT_FALSE(success2);
  tok.skipWhitespaceAndComments();
  ASSERT_EQ(tok.data().begin() - s.data(), 27);
}

