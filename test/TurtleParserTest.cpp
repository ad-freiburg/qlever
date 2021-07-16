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
  auto runCommonTests = [](auto& parser) {
    parser._prefixMap["wd"] = "www.wikidata.org/";
    parser.setInputStream("wd:Q430 someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser._lastParseResult, "<www.wikidata.org/Q430>");
    ASSERT_EQ(parser.getPosition(), size_t(7));

    parser.setInputStream(" wd:Q430 someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser._lastParseResult, "<www.wikidata.org/Q430>");
    ASSERT_EQ(parser.getPosition(), 8u);

    // empty name
    parser.setInputStream("wd: someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser._lastParseResult, "<www.wikidata.org/>");
    ASSERT_EQ(parser.getPosition(), size_t(3));

    parser.setInputStream("<wd.de> rest");
    parser._lastParseResult = "comp1";
    ASSERT_FALSE(parser.prefixedName());
    ASSERT_EQ(parser._lastParseResult, "comp1");
    ASSERT_EQ(parser.getPosition(), 0u);

    parser.setInputStream("unregistered:bla");
    ASSERT_THROW(parser.prefixedName(),
                 typename std::decay_t<decltype(parser)>::ParseException);
  };
  {
    TurtleStringParser<Tokenizer> p;
    runCommonTests(p);
    p.setInputStream(R"(wd:esc\,aped)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p._lastParseResult, "<www.wikidata.org/esc,aped>");
    ASSERT_EQ(p.getPosition(), 12u);

    // escapes at the beginning are illegal, so this is parsed as an empty wd:
    p.setInputStream(R"(wd:\\esc\,aped)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p._lastParseResult, "<www.wikidata.org/>");
    ASSERT_EQ(p.getPosition(), 3u);

    p.setInputStream(R"(wd:esc\,aped\.)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p._lastParseResult, "<www.wikidata.org/esc,aped.>");
    ASSERT_EQ(p.getPosition(), 14u);
  }

  {
    TurtleStringParser<TokenizerCtre> p;
    runCommonTests(p);
    // These unit tests document the current (fast, but suboptimal) behavior of
    // the CTRE parser. TODO: Try to improve the parser without sacrificing
    // speed. If that succeeds, adapt this unit test.

    // TokenizerCTRE parsers `esc\` , which fails to unescape (single backslash)
    p.setInputStream("wd:esc\\,aped");
    ASSERT_THROW(p.prefixedName(), ad_semsearch::Exception);

    // TokenizerCTRE parses `esc\\aped` which fails to unescape (escaped
    // backslashes are not allowed in prefixed names)
    p.setInputStream(R"(wd:esc\\aped.)");
    ASSERT_THROW(p.prefixedName(), ad_semsearch::Exception);
  }
}

TEST(TurtleParserTest, prefixID) {
  TurtleStringParser<Tokenizer> p;
  string s = "@prefix bla:<www.bla.org/> .";
  p.setInputStream(s);
  ASSERT_TRUE(p.prefixID());
  ASSERT_EQ(p._prefixMap["bla"], "www.bla.org/");
  ASSERT_EQ(p.getPosition(), s.size());

  // different spaces that don't change meaning
  s = "@prefix bla: <www.bla.org/>.";
  p.setInputStream(s);
  ASSERT_TRUE(p.prefixID());
  ASSERT_EQ(p._prefixMap["bla"], "www.bla.org/");
  ASSERT_EQ(p.getPosition(), s.size());

  // invalid LL1
  s = "@prefix bla<www.bla.org/>.";
  p.setInputStream(s);
  ASSERT_THROW(p.prefixID(), TurtleParser<Tokenizer>::ParseException);

  s = "@prefxxix bla<www.bla.org/>.";
  p.setInputStream(s);
  p._lastParseResult = "comp1";
  ASSERT_FALSE(p.prefixID());
  ASSERT_EQ(p._lastParseResult, "comp1");
  ASSERT_EQ(p.getPosition(), 0u);
}

TEST(TurtleParserTest, stringParse) {
  TurtleStringParser<Tokenizer> p;
  string s1("\"double quote\"");
  string s2("\'single quote\'");
  string s3("\"\"\"multiline \n double quote\"\"\"");
  string s4("\'\'\'multiline \n single quote\'\'\'");

  // the main thing to test here is that s3 does not prefix-match the simple
  // string "" but the complex string """..."""

  p.setInputStream(s1);
  ASSERT_TRUE(p.stringParse());
  ASSERT_EQ(p._lastParseResult, s1);
  ASSERT_EQ(p.getPosition(), s1.size());

  p.setInputStream(s2);
  ASSERT_TRUE(p.stringParse());
  ASSERT_EQ(p._lastParseResult, s2);
  ASSERT_EQ(p.getPosition(), s2.size());

  p.setInputStream(s3);
  ASSERT_TRUE(p.stringParse());
  ASSERT_EQ(p._lastParseResult, s3);
  ASSERT_EQ(p.getPosition(), s3.size());

  p.setInputStream(s4);
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

  TurtleStringParser<Tokenizer> p;
  for (const auto& s : literals) {
    p.setInputStream(s);
    ASSERT_TRUE(p.rdfLiteral());
    ASSERT_EQ(p._lastParseResult, s);
    ASSERT_EQ(p.getPosition(), s.size());
  }

  p._prefixMap["xsd"] = "www.xsd.org/";
  string s("\"valuePrefixed\"^^xsd:integer");
  p.setInputStream(s);
  ASSERT_TRUE(p.rdfLiteral());
  ASSERT_EQ(p._lastParseResult, "\"valuePrefixed\"^^<www.xsd.org/integer>");
  ASSERT_EQ(p.getPosition(), s.size());
}

TEST(TurtleParserTest, blankNode) {
  TurtleStringParser<Tokenizer> p;
  p.setInputStream(" _:blank1");
  ASSERT_TRUE(p.blankNode());
  ASSERT_EQ(p._lastParseResult, "_:blank1");
  ASSERT_EQ(p.getPosition(), 9u);

  p.setInputStream(" _:blank1 someRemainder");
  ASSERT_TRUE(p.blankNode());
  ASSERT_EQ(p._lastParseResult, "_:blank1");
  ASSERT_EQ(p.getPosition(), 9u);

  p.setInputStream("  _:blank2 someOtherStuff");
  ASSERT_TRUE(p.blankNode());
  ASSERT_EQ(p._lastParseResult, "_:blank2");
  ASSERT_EQ(p._numBlankNodes, 0u);
  ASSERT_EQ(p.getPosition(), 10u);

  // anonymous blank node
  p.setInputStream(" [    \n\t  ]");
  ASSERT_TRUE(p.blankNode());
  ASSERT_EQ(p._lastParseResult, "QLever-Anon-Node:0");
  ASSERT_EQ(p._numBlankNodes, 1u);
  ASSERT_EQ(p.getPosition(), 11u);
}

TEST(TurtleParserTest, blankNodePropertyList) {
  TurtleStringParser<Tokenizer> p;
  p._activeSubject = "<s>";
  p._activePredicate = "<p1>";

  string blankNodeL = "[<p2> <ob2>; <p3> <ob3>]";
  std::vector<std::array<string, 3>> exp;
  exp.push_back({"<s>", "<p1>", "QLever-Anon-Node:0"});
  exp.push_back({"QLever-Anon-Node:0", "<p2>", "<ob2>"});
  exp.push_back({"QLever-Anon-Node:0", "<p3>", "<ob3>"});
  p.setInputStream(blankNodeL);
  ASSERT_TRUE(p.blankNodePropertyList());
  ASSERT_EQ(p._triples, exp);
  ASSERT_EQ(p.getPosition(), blankNodeL.size());

  blankNodeL = "[<2> <ob2>; \"invalidPred\" <ob3>]";
  p.setInputStream(blankNodeL);
  ASSERT_THROW(p.blankNodePropertyList(),
               TurtleParser<Tokenizer>::ParseException);
}

TEST(TurtleParserTest, object) {
  TurtleStringParser<Tokenizer> p;
  string sub = "<sub>";
  string pred = "<pred>";
  using triple = std::array<string, 3>;
  p._activeSubject = sub;
  p._activePredicate = pred;
  p._prefixMap["b"] = "bla/";
  string iri = " b:iri";
  p.setInputStream(iri);
  ASSERT_TRUE(p.object());
  ASSERT_EQ(p._lastParseResult, "<bla/iri>");
  auto exp = triple{sub, pred, "<bla/iri>"};
  ASSERT_EQ(p._triples.back(), exp);

  string literal = "\"literal\"";
  p.setInputStream(literal);
  ASSERT_TRUE(p.object());
  ASSERT_EQ(p._lastParseResult, literal);
  exp = triple{sub, pred, literal};
  ASSERT_EQ(p._triples.back(), exp);

  string blank = "_:someblank";
  p.setInputStream(blank);
  ASSERT_TRUE(p.object());
  ASSERT_EQ(p._lastParseResult, "_:someblank");

  exp = triple{sub, pred, "_:someblank"};
  ASSERT_EQ(p._triples.back(), exp);
}

TEST(TurtleParserTest, objectList) {
  TurtleStringParser<Tokenizer> p;
  p._activeSubject = "<s>";
  p._activePredicate = "<p>";
  string objectL = " <ob1>, <ob2>, <ob3>";
  std::vector<std::array<string, 3>> exp;
  exp.push_back({"<s>", "<p>", "<ob1>"});
  exp.push_back({"<s>", "<p>", "<ob2>"});
  exp.push_back({"<s>", "<p>", "<ob3>"});
  p.setInputStream(objectL);
  ASSERT_TRUE(p.objectList());
  ASSERT_EQ(p._triples, exp);
  ASSERT_EQ(p.getPosition(), objectL.size());

  p.setInputStream("@noObject");
  ASSERT_FALSE(p.objectList());

  p.setInputStream("<obj1>, @illFormed");
  ASSERT_THROW(p.objectList(), TurtleParser<Tokenizer>::ParseException);
}

TEST(TurtleParserTest, predicateObjectList) {
  TurtleStringParser<Tokenizer> p;
  p._activeSubject = "<s>";
  string predL = "\n <p1> <ob1>;<p2> \"ob2\",\n <ob3>";
  std::vector<std::array<string, 3>> exp;
  exp.push_back({"<s>", "<p1>", "<ob1>"});
  exp.push_back({"<s>", "<p2>", "\"ob2\""});
  exp.push_back({"<s>", "<p2>", "<ob3>"});
  p.setInputStream(predL);
  ASSERT_TRUE(p.predicateObjectList());
  ASSERT_EQ(p._triples, exp);
  ASSERT_EQ(p.getPosition(), predL.size());
}
