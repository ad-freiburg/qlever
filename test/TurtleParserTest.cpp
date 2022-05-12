// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "../src/parser/TurtleParser.h"
#include "../src/util/Conversions.h"

using std::string;
using namespace std::literals;
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
  std::vector<TripleObject> expected;
  literals.emplace_back(R"("simpleString")");
  expected.emplace_back(R"("simpleString")");
  literals.emplace_back(R"("langtag"@en-gb)");
  expected.emplace_back(R"("langtag"@en-gb)");
  literals.emplace_back("\"valueLong\"^^<www.someunknownType/integer>");
  expected.emplace_back("\"valueLong\"^^<www.someunknownType/integer>");

  literals.emplace_back(R"("42.1234"^^)"s + "<" + XSD_DOUBLE_TYPE + ">");
  expected.emplace_back(42.1234);
  literals.push_back(R"("-142.321"^^)"s + "<" + XSD_DECIMAL_TYPE + ">");
  expected.emplace_back(-142.321);
  literals.push_back(R"("-142321"^^)"s + "<" + XSD_INT_TYPE + ">");
  expected.emplace_back(-142321);
  literals.push_back(R"("+144321"^^)"s + "<" + XSD_INTEGER_TYPE + ">");
  expected.emplace_back(144321);

  TurtleStringParser<Tokenizer> p;
  for (size_t i = 0; i < literals.size(); ++i) {
    const auto& literal = literals[i];
    const auto& exp = expected[i];
    p.setInputStream(literal);
    ASSERT_TRUE(p.rdfLiteral());
    ASSERT_EQ(p._lastParseResult, exp);
    ASSERT_EQ(p.getPosition(), literal.size());
  }

  p._prefixMap["doof"] = "www.doof.org/";
  string s("\"valuePrefixed\"^^doof:sometype");
  p.setInputStream(s);
  ASSERT_TRUE(p.rdfLiteral());
  ASSERT_EQ(p._lastParseResult, "\"valuePrefixed\"^^<www.doof.org/sometype>");
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
  std::vector<TurtleTriple> exp;
  exp.push_back({"<s>", "<p1>", TripleObject{"QLever-Anon-Node:0"}});
  exp.push_back({"QLever-Anon-Node:0", "<p2>", TripleObject{"<ob2>"}});
  exp.push_back({"QLever-Anon-Node:0", "<p3>", TripleObject{"<ob3>"}});
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
  p._activeSubject = sub;
  p._activePredicate = pred;
  p._prefixMap["b"] = "bla/";
  string iri = " b:iri";
  p.setInputStream(iri);
  ASSERT_TRUE(p.object());
  ASSERT_EQ(p._lastParseResult, "<bla/iri>");
  auto exp = TurtleTriple{sub, pred, "<bla/iri>"};
  ASSERT_EQ(p._triples.back(), exp);

  string literal = "\"literal\"";
  p.setInputStream(literal);
  ASSERT_TRUE(p.object());
  ASSERT_EQ(p._lastParseResult, literal);
  exp = TurtleTriple{sub, pred, literal};
  ASSERT_EQ(p._triples.back(), exp);

  string blank = "_:someblank";
  p.setInputStream(blank);
  ASSERT_TRUE(p.object());
  ASSERT_EQ(p._lastParseResult, "_:someblank");

  exp = TurtleTriple{sub, pred, "_:someblank"};
  ASSERT_EQ(p._triples.back(), exp);
}

TEST(TurtleParserTest, objectList) {
  TurtleStringParser<Tokenizer> parser;
  parser._activeSubject = "<s>";
  parser._activePredicate = "<p>";
  string objectL = " <ob1>, <ob2>, <ob3>";
  std::vector<TurtleTriple> exp;
  exp.push_back({"<s>", "<p>", "<ob1>"});
  exp.push_back({"<s>", "<p>", "<ob2>"});
  exp.push_back({"<s>", "<p>", "<ob3>"});
  parser.setInputStream(objectL);
  ASSERT_TRUE(parser.objectList());
  ASSERT_EQ(parser._triples, exp);
  ASSERT_EQ(parser.getPosition(), objectL.size());

  parser.setInputStream("@noObject");
  ASSERT_FALSE(parser.objectList());

  parser.setInputStream("<obj1>, @illFormed");
  ASSERT_THROW(parser.objectList(), TurtleParser<Tokenizer>::ParseException);
}

TEST(TurtleParserTest, predicateObjectList) {
  TurtleStringParser<Tokenizer> parser;
  parser._activeSubject = "<s>";
  string predL = "\n <p1> <ob1>;<p2> \"ob2\",\n <ob3>";
  std::vector<TurtleTriple> exp;
  exp.push_back({"<s>", "<p1>", "<ob1>"});
  exp.push_back({"<s>", "<p2>", "\"ob2\""});
  exp.push_back({"<s>", "<p2>", "<ob3>"});
  parser.setInputStream(predL);
  ASSERT_TRUE(parser.predicateObjectList());
  ASSERT_EQ(parser._triples, exp);
  ASSERT_EQ(parser.getPosition(), predL.size());
}

TEST(TurtleParserTest, numericLiteral) {
  std::vector<std::string> literals{"2",   "-2",     "42.209",   "-42.239",
                                    ".74", "2.3e12", "2.34E-14", "-0.3e2"};
  std::vector<TripleObject> expected{2,   -2,     42.209,   -42.239,
                                     .74, 2.3e12, 2.34e-14, -0.3e2};

  TurtleStringParser<Tokenizer> parser;
  for (size_t i = 0; i < literals.size(); ++i) {
    const auto& literal = literals[i];
    parser.setInputStream(literal);
    ASSERT_TRUE(parser.numericLiteral());
    ASSERT_EQ(parser._lastParseResult, expected[i]);
  }
}

TEST(TurtleParserTest, numericLiteralErrorBehavior) {
  auto assertParsingFails = [](auto& parser, std::string input) {
    parser.setInputStream(input);
    ASSERT_THROW(parser.parseAndReturnAllTriples(),
                 TurtleStringParser<Tokenizer>::ParseException);
  };

  auto parseAllTriples = [](auto& parser, std::string input) {
    parser.setInputStream(input);
    return parser.parseAndReturnAllTriples();
  };

  auto testTripleObjects = [&parseAllTriples]<typename T>(
                               auto& parser, std::vector<std::string> triples,
                               std::vector<T> expectedObjects) {
    for (size_t i = 0; i < triples.size(); ++i) {
      const auto& triple = triples[i];
      std::vector<TurtleTriple> result;
      ASSERT_NO_THROW(result = parseAllTriples(parser, triple));
      ASSERT_EQ(result.size(), 1ul);
      ASSERT_EQ(result[0]._object, expectedObjects[i]);
    }
  };
  {
    // Test the default mode (overflowing integers throw an exception).
    std::vector<std::string> inputs{
        "<a> <b> 99999999999999999999999",
        "<a> <b> \"99999999999999999999\"^^xsd:integer",
        "<a> <b> \"9999.0\"^^xsd:integer",
        "<a> <b> \"-9999.0\"^^xsd:int",
        "<a> <b> \"9999E4\"^^xsd:integer",
        "<a> <b> \"9999E4\"^^xsd:int",
        "<a> <b> \"kartoffelsalat\"^^xsd:integer",
        "<a> <b> \"123kartoffel\"^^xsd:integer",
        "<a> <b> \"kartoffelsalat\"^^xsd:double",
        "<a> <b> \"123kartoffel\"^^xsd:decimal"};
    TurtleStringParser<Tokenizer> parser;
    parser._prefixMap["xsd"] = "http://www.w3.org/2001/XMLSchema#";
    for (const auto& input : inputs) {
      assertParsingFails(parser, input);
    }
  }
  {
    // These all work when the datatype is double.
    std::vector<std::string> inputs{
        "<a> <b> 99999999999999999999999.0",
        "<a> <b> \"-9999999999999999999999\"^^xsd:double",
        "<a> <b> \"9999999999999999999999\"^^xsd:decimal",
        "<a> <b> \"99999999999999999999E4\"^^xsd:double",
        "<a> <b> \"99999999999999999999.0E4\"^^xsd:decimal"};
    std::vector<double> expectedObjects{
        99999999999999999999999.0, -9999999999999999999999.0,
        9999999999999999999999.0, 99999999999999999999E4,
        99999999999999999999E4};
    TurtleStringParser<Tokenizer> parser;
    parser._prefixMap["xsd"] = "http://www.w3.org/2001/XMLSchema#";
    testTripleObjects(parser, inputs, expectedObjects);
  }
  {
    // Test the overflow to double mode.
    std::vector<std::string> nonWorkingInputs{
        "<a> <b> \"9999.0\"^^xsd:integer",
        "<a> <b> \"-9999.0\"^^xsd:int",
        "<a> <b> \"9999E4\"^^xsd:integer",
        "<a> <b> \"9999E4\"^^xsd:int",
        "<a> <b> \"kartoffelsalat\"^^xsd:integer",
        "<a> <b> \"123kartoffel\"^^xsd:integer"};
    TurtleStringParser<Tokenizer> parser;
    parser._prefixMap["xsd"] = "http://www.w3.org/2001/XMLSchema#";
    parser.integerOverflowBehavior() =
        TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    for (const auto& input : nonWorkingInputs) {
      assertParsingFails(parser, input);
    }
    std::vector<std::string> workingInputs{
        "<a> <b> 99999999999999999999999",
        "<a> <b> \"-99999999999999999999\"^^xsd:integer",
    };
    std::vector<double> expectedDoubles{99999999999999999999999.0,
                                        -99999999999999999999.0};
    testTripleObjects(parser, workingInputs, expectedDoubles);
  }
  {
    // Test the "all integers become doubles" mode.
    std::vector<std::string> nonWorkingInputs{
        "<a> <b> \"kartoffelsalat\"^^xsd:integer",
        "<a> <b> \"123kartoffel\"^^xsd:integer"};
    TurtleStringParser<Tokenizer> parser;
    parser._prefixMap["xsd"] = "http://www.w3.org/2001/XMLSchema#";
    parser.integerOverflowBehavior() =
        TurtleParserIntegerOverflowBehavior::AllToDouble;
    for (const auto& input : nonWorkingInputs) {
      assertParsingFails(parser, input);
    }
    std::vector<std::string> workingInputs{
        "<a> <b> \"123\"^^xsd:integer",
        "<a> <b> 456",
        "<a> <b> \"-9999.0\"^^xsd:integer",
        "<a> <b> \"9999.0\"^^xsd:int",
        "<a> <b> \"9999E4\"^^xsd:integer",
        "<a> <b> \"9999E4\"^^xsd:int",
        "<a> <b> 99999999999999999999999",
        "<a> <b> \"99999999999999999999\"^^xsd:integer",
    };
    std::vector<double> expectedDoubles{123.0,
                                        456.0,
                                        -9999.0,
                                        9999.0,
                                        9999e4,
                                        9999e4,
                                        99999999999999999999999.0,
                                        99999999999999999999.0};
    testTripleObjects(parser, workingInputs, expectedDoubles);
  }
  {
    // Test the skipping of unsupported triples with the "overflow is error"
    // behavior.
    std::string input{
        "<a> <b> 123. <c> <d> 99999999999999999999999. <e> <f> 234"};
    std::vector<TurtleTriple> expected{{"<a>", "<b>", 123},
                                       {"<e>", "<f>", 234}};
    TurtleStringParser<Tokenizer> parser;
    parser.invalidLiteralsAreSkipped() = true;
    auto result = parseAllTriples(parser, input);
    ASSERT_EQ(result, expected);
  }
  {
    // Test the skipping of unsupported triples with the "overflow to double"
    // behavior.
    std::string input{
        "<a> <b> 99999999999999999999999. <c> <d> \"invalid\"^^xsd:integer. "
        "<e> <f> 234"};
    std::vector<TurtleTriple> expected{
        {"<a>", "<b>", 99999999999999999999999.0}, {"<e>", "<f>", 234}};
    TurtleStringParser<Tokenizer> parser;
    parser._prefixMap["xsd"] = "http://www.w3.org/2001/XMLSchema#";
    parser.invalidLiteralsAreSkipped() = true;
    parser.integerOverflowBehavior() =
        TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    auto result = parseAllTriples(parser, input);
    ASSERT_EQ(result, expected);
  }
}

TEST(TurtleParserTest, booleanLiteral) {
  TurtleStringParser<Tokenizer> parser;
  parser.setInputStream("true");
  ASSERT_TRUE(parser.booleanLiteral());
  ASSERT_EQ("\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            parser._lastParseResult);

  parser.setInputStream("false");
  ASSERT_TRUE(parser.booleanLiteral());
  ASSERT_EQ("\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            parser._lastParseResult);

  parser.setInputStream("maybe");
  ASSERT_FALSE(parser.booleanLiteral());
}
