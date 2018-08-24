// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include "../src/parser/TurtleParser.h"

using std::string;
TEST(TurtleParserTest, prefixedName) {
  TurtleParser p;
  p._prefixMap["wd"] = "www.wikidata.org/";
  p.reset("wd:Q430 someotherContent");
  ASSERT_TRUE(p.prefixedName());
  ASSERT_EQ(p._lastParseResult, "<www.wikidata.org/Q430>");
  ASSERT_EQ(p.getPosition(), size_t(7));

  p.reset(" wd:Q430 someotherContent");
  ASSERT_TRUE(p.prefixedName());
  ASSERT_EQ(p._lastParseResult, "<www.wikidata.org/Q430>");
  ASSERT_EQ(p.getPosition(), 8u);

  // empty name
  p.reset("wd: someotherContent");
  ASSERT_TRUE(p.prefixedName());
  ASSERT_EQ(p._lastParseResult, "<www.wikidata.org/>");
  ASSERT_EQ(p.getPosition(), size_t(3));

  p.reset("<wd.de> rest");
  p._lastParseResult = "comp1";
  ASSERT_FALSE(p.prefixedName());
  ASSERT_EQ(p._lastParseResult, "comp1");
  ASSERT_EQ(p.getPosition(), 0u);

  p.reset("unregistered:bla");
  ASSERT_THROW(p.prefixedName(), ParseException);
}

TEST(TurtleParserTest, prefixID) {
  TurtleParser p;
  string s = "@prefix bla:<www.bla.org/> .";
  p.reset(s);
  ASSERT_TRUE(p.prefixID());
  ASSERT_EQ(p._prefixMap["bla"], "www.bla.org/");
  ASSERT_EQ(p.getPosition(), s.size());

  // different spaces that don't change meaning
  s = "@prefix bla: <www.bla.org/>.";
  p.reset(s);
  ASSERT_TRUE(p.prefixID());
  ASSERT_EQ(p._prefixMap["bla"], "www.bla.org/");
  ASSERT_EQ(p.getPosition(), s.size());

  // invalid LL1
  s = "@prefix bla<www.bla.org/>.";
  p.reset(s);
  ASSERT_THROW(p.prefixID(), ParseException);

  s = "@prefxxix bla<www.bla.org/>.";
  p.reset(s);
  p._lastParseResult = "comp1";
  ASSERT_FALSE(p.prefixID());
  ASSERT_EQ(p._lastParseResult, "comp1");
  ASSERT_EQ(p.getPosition(), 0u);
}

TEST(TurtleParserTest, stringParse) {
  TurtleParser p;
  string s1("\"double quote\"");
  string s2("\'single quote\'");
  string s3("\"\"\"multiline \n double quote\"\"\"");
  string s4("\'\'\'multiline \n single quote\'\'\'");

  // the main thing to test here is that s3 does not prefix-match the simple
  // string "" but the complex string """..."""

  p.reset(s1);
  ASSERT_TRUE(p.stringParse());
  ASSERT_EQ(p._lastParseResult, s1);
  ASSERT_EQ(p.getPosition(), s1.size());

  p.reset(s2);
  ASSERT_TRUE(p.stringParse());
  ASSERT_EQ(p._lastParseResult, s2);
  ASSERT_EQ(p.getPosition(), s2.size());

  p.reset(s3);
  ASSERT_TRUE(p.stringParse());
  ASSERT_EQ(p._lastParseResult, s3);
  ASSERT_EQ(p.getPosition(), s3.size());

  p.reset(s4);
  ASSERT_TRUE(p.stringParse());
  ASSERT_EQ(p._lastParseResult, s4);
  ASSERT_EQ(p.getPosition(), s4.size());
}

TEST(TurtleParserTest, rdfLiteral) {
  std::vector<string> literals;
  std::vector<string> expected;
  literals.push_back("\"simpleString\"");
  literals.push_back("\"langtag\"@en-gb");
  literals.push_back("\"valueLong\"^^<www.xsd.org/integer>");

  TurtleParser p;
  for (const auto& s : literals) {
    p.reset(s);
    ASSERT_TRUE(p.rdfLiteral());
    ASSERT_EQ(p._lastParseResult, s);
    ASSERT_EQ(p.getPosition(), s.size());
  }

  p._prefixMap["xsd"] = "www.xsd.org/";
  string s("\"valuePrefixed\"^^xsd:integer");
  p.reset(s);
  ASSERT_TRUE(p.rdfLiteral());
  ASSERT_EQ(p._lastParseResult, "\"valuePrefixed\"^^<www.xsd.org/integer>");
  ASSERT_EQ(p.getPosition(), s.size());
}

