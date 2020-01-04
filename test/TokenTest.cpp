// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#include <gtest/gtest.h>
#include <iostream>
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

  ASSERT_TRUE(RE2::FullMatch(u8"A", t.cls(t.PnCharsBaseString)));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00dd", t.cls(t.PnCharsBaseString)));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00DD", t.cls(t.PnCharsBaseString)));
  ASSERT_TRUE(RE2::FullMatch(u8"\u00De", t.cls(t.PnCharsBaseString)));
  ASSERT_FALSE(RE2::FullMatch(u8"\u00D7", t.cls(t.PnCharsBaseString)));
}

TEST(TokenizerTest, StringLiterals) {
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

  string sMultiline1 = u8"\"\"\"test\n\"\"\"";
  string sMultiline2 =
      "\"\"\"MultilineString\' \'\'\'\n\\n\\u00FF\\U0001AB34\"  \"\" "
      "someMore\"\"\"";
  string sNoMultiline1 = u8"\"\"\"\\autsch\"\"\"";
  string sNoMultiline2 = u8"\"\"\"\"\"\"\"";
  ASSERT_TRUE(RE2::FullMatch(sMultiline1, t.StringLiteralLongQuote, nullptr));
  ASSERT_TRUE(RE2::FullMatch(sMultiline2, t.StringLiteralLongQuote, nullptr));
  ASSERT_FALSE(
      RE2::FullMatch(sNoMultiline1, t.StringLiteralLongQuote, nullptr));
  ASSERT_FALSE(
      RE2::FullMatch(sNoMultiline2, t.StringLiteralLongQuote, nullptr));

  string sSingleMultiline1 = u8"\'\'\'test\n\'\'\'";
  string sSingleMultiline2 =
      "\'\'\'MultilineString\" \"\"\"\n\\n\\u00FF\\U0001AB34\'  \'\' "
      "someMore\'\'\'";
  string sSingleNoMultiline1 = u8"\'\'\'\\autsch\'\'\'";
  string sSingleNoMultiline2 = u8"\'\'\'\'\'\'\'";
  ASSERT_TRUE(RE2::FullMatch(sSingleMultiline1, t.StringLiteralLongSingleQuote,
                             nullptr));
  ASSERT_TRUE(RE2::FullMatch(sSingleMultiline2, t.StringLiteralLongSingleQuote,
                             nullptr));
  ASSERT_FALSE(RE2::FullMatch(sSingleNoMultiline1,
                              t.StringLiteralLongSingleQuote, nullptr));
  ASSERT_FALSE(RE2::FullMatch(sSingleNoMultiline2,
                              t.StringLiteralLongSingleQuote, nullptr));
}

TEST(TokenizerTest, Entities) {
  TurtleToken t;
  string iriref1 = "<>";
  string iriref2 = "<simple>";
  string iriref3 = "<unicode\uAA34\U000ABC34end>";
  string iriref4 = "<escaped\\uAA34\\U000ABC34end>";
  string noIriref1 = "<\n>";
  string noIriref2 = "< >";
  ASSERT_TRUE(RE2::FullMatch(iriref1, t.Iriref, nullptr));
  ASSERT_TRUE(RE2::FullMatch(iriref2, t.Iriref, nullptr));
  ASSERT_TRUE(RE2::FullMatch(iriref3, t.Iriref, nullptr));
  ASSERT_TRUE(RE2::FullMatch(iriref4, t.Iriref, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noIriref1, t.Iriref, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noIriref2, t.Iriref, nullptr));

  string prefix1 = "wd:";
  string prefix2 = "wdDDäéa_afa:";
  string prefix3 = "wD\u00D2:";
  string prefix4 = "wD.aä:";
  string noPrefix1 = "_w:";
  string noPrefix2 = "wd";
  string noPrefix3 = "w\nd";
  string noPrefix4 = "wd\u00D7:";
  ASSERT_TRUE(RE2::FullMatch(prefix1, t.PnameNS, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefix2, t.PnameNS, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefix3, t.PnameNS, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefix4, t.PnameNS, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noPrefix1, t.PnameNS, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noPrefix2, t.PnameNS, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noPrefix3, t.PnameNS, nullptr));

  string prefName1 = "wd:Q34";
  string prefName2 = "wdDDäé_afa::93.x";
  string prefName3 = "wd:\%FF\%33...\%FF";
  string prefName4 = "wd:\\_\\~ab.c";
  string prefName5 = "wd:_hey";
  string prefName6 = "wd:h-ey";
  string prefName7 = "wd:::.::";

  string noPrefName1 = "wd:.hey";
  string noPrefName2 = "wd:-hey";
  string noPrefName3 = "wd:\u00BF";

  ASSERT_FALSE(RE2::FullMatch("\xBF", t.cls(t.PnCharsUString)));
  ASSERT_FALSE(RE2::FullMatch("\xBF", t.PnLocalString));

  ASSERT_TRUE(RE2::FullMatch(prefName1, t.PnameLN, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefName2, t.PnameLN, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefName3, t.PnameLN, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefName4, t.PnameLN, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefName5, t.PnameLN, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefName6, t.PnameLN, nullptr));
  ASSERT_TRUE(RE2::FullMatch(prefName7, t.PnameLN, nullptr));

  ASSERT_FALSE(RE2::FullMatch(noPrefName1, t.PnameLN, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noPrefName2, t.PnameLN, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noPrefName3, t.PnameLN, nullptr));

  string blank1 = "_:easy";
  string blank2 = "_:_easy";
  string blank3 = "_:d-35\u00B7";
  string blank4 = "_:a..d...A";
  string blank5 = "_:a..\u00B7";
  string blank6 = "_:3numberFirst";

  string noBlank1 = "_:ab.";
  string noBlank2 = "_:-ab";
  string noBlank3 = "_:\u00B7";
  string noBlank4 = "_:.pointFirst";

  ASSERT_TRUE(RE2::FullMatch(blank1, t.BlankNodeLabel, nullptr));
  ASSERT_TRUE(RE2::FullMatch(blank2, t.BlankNodeLabel, nullptr));
  ASSERT_TRUE(RE2::FullMatch(blank3, t.BlankNodeLabel, nullptr));
  ASSERT_TRUE(RE2::FullMatch(blank4, t.BlankNodeLabel, nullptr));
  ASSERT_TRUE(RE2::FullMatch(blank5, t.BlankNodeLabel, nullptr));
  ASSERT_TRUE(RE2::FullMatch(blank6, t.BlankNodeLabel, nullptr));

  ASSERT_FALSE(RE2::FullMatch(noBlank1, t.BlankNodeLabel, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noBlank2, t.BlankNodeLabel, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noBlank3, t.BlankNodeLabel, nullptr));
  ASSERT_FALSE(RE2::FullMatch(noBlank4, t.BlankNodeLabel, nullptr));
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

TEST(TokenizerTest, normalizeRDFLiteral) {
  {
    std::string l1 = "\"simpleLiteral\"";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l1));
    std::string l2 = "\'simpleLiteral\'";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l2));
    std::string l3 = R"('''simpleLiteral''')";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l3));
    std::string l4 = R"("""simpleLiteral""")";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l4));

    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l1)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l2)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l3)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l4)));
  }
  {
    std::string l1 = "\"simpleLiteral\"@en-ca";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l1));
    std::string l2 = "\'simpleLiteral\'@en-ca";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l2));
    std::string l3 = R"('''simpleLiteral'''@en-ca)";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l3));
    std::string l4 = R"("""simpleLiteral"""@en-ca)";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l4));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l1)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l2)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l3)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l4)));
  }
  {
    std::string l1 = "\"simpleLiteral\"^^xsd::boolean";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l1));
    std::string l2 = "\'simpleLiteral\'^^xsd::boolean";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l2));
    std::string l3 = R"('''simpleLiteral'''^^xsd::boolean)";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l3));
    std::string l4 = R"("""simpleLiteral"""^^xsd::boolean)";
    ASSERT_EQ(l1, TurtleToken::normalizeRDFLiteral(l4));

    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l1)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l2)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l3)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l4)));
  }

  {
    std::string t = "\"si\"mple\'Li\n\rt\t\b\fer\\\"";
    std::string l1 = R"("si\"mple\'Li\n\rt\t\b\fer\\")";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l1));
    std::string l2 = R"('si\"mple\'Li\n\rt\t\b\fer\\')";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l2));
    std::string l3 = R"('''si\"mple\'Li\n\rt\t\b\fer\\''')";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l3));
    std::string l4 = R"("""si\"mple\'Li\n\rt\t\b\fer\\""")";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l4));

    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(t));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l1)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l2)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l3)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l4)));
  }
  {
    std::string t = "\"si\"mple\'Li\n\rt\t\b\fer\\\"@de-us";
    std::string l1 = R"("si\"mple\'Li\n\rt\t\b\fer\\"@de-us)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l1));
    std::string l2 = R"('si\"mple\'Li\n\rt\t\b\fer\\'@de-us)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l2));
    std::string l3 = R"('''si\"mple\'Li\n\rt\t\b\fer\\'''@de-us)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l3));
    std::string l4 = R"("""si\"mple\'Li\n\rt\t\b\fer\\"""@de-us)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l4));

    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(t));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l1)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l2)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l3)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l4)));
  }

  {
    std::string t = "\"si\"mple\'Li\n\rt\t\b\fer\\\"^^xsd::integer";
    std::string l1 = R"("si\"mple\'Li\n\rt\t\b\fer\\"^^xsd::integer)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l1));
    std::string l2 = R"('si\"mple\'Li\n\rt\t\b\fer\\'^^xsd::integer)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l2));
    std::string l3 = R"('''si\"mple\'Li\n\rt\t\b\fer\\'''^^xsd::integer)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l3));
    std::string l4 = R"("""si\"mple\'Li\n\rt\t\b\fer\\"""^^xsd::integer)";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l4));

    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(t));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l1)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l2)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l3)));
    ASSERT_EQ(l1, TurtleToken::escapeRDFLiteral(
                      TurtleToken::normalizeRDFLiteral(l4)));
  }

  {
    std::string t = "\"si\"mple\'Li\n\rt\t\b\fer\\\"^^xsd::integer";
    std::string tEscaped = R"("si\"mple\'Li\n\rt\t\b\fer\\"^^xsd::integer)";
    std::string l3 = "\"\"\"si\"mple\'Li\n\rt\t\b\fer\\\\\"\"\"^^xsd::integer";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l3));
    std::string l4 = "\'\'\'si\"mple\'Li\n\rt\t\b\fer\\\\\'\'\'^^xsd::integer";
    ASSERT_EQ(t, TurtleToken::normalizeRDFLiteral(l4));

    ASSERT_EQ(tEscaped, TurtleToken::escapeRDFLiteral(t));
    ASSERT_EQ(tEscaped, TurtleToken::escapeRDFLiteral(
                            TurtleToken::normalizeRDFLiteral(l3)));
    ASSERT_EQ(tEscaped, TurtleToken::escapeRDFLiteral(
                            TurtleToken::normalizeRDFLiteral(l4)));
  }
}
