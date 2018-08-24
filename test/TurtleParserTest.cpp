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
}

