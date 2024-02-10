// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#include <iostream>
#include <string>

#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/TurtleParser.h"
#include "util/Conversions.h"

using std::string;
using namespace std::literals;
using Re2Parser = TurtleStringParser<Tokenizer>;
using CtreParser = TurtleStringParser<TokenizerCtre>;

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;
}

// TODO<joka921>: Use the following abstractions and the alias `Parser` in all
// of this file. Set up a `Parser` with the given `input` and call the given
// `rule` (a member function of `Parser` that returns a bool). Return the
// parser, if the call to `rule` returns true, else return `std::nullopt`.
template <typename Parser, auto rule, size_t blankNodePrefix = 0>
std::optional<Parser> parseRule(const std::string& input) {
  Parser parser;
  parser.setBlankNodePrefixOnlyForTesting(blankNodePrefix);
  parser.setInputStream(input);
  if (!std::invoke(rule, parser)) {
    return std::nullopt;
  }
  return parser;
}

// Asserts that parsing the `rule` works and that the last parse result and the
// emitted triples are as expected. Returns the `Parser` instance that is used
// to parse the rule for further inspection. The anonymous blank nodes that are
// generated will start with "_:g_<blankNodePrefix>_".
template <typename Parser, auto rule, size_t blankNodePrefix = 0>
auto checkParseResult =
    [](const std::string& input,
       std::optional<TripleComponent> expectedLastParseResult = {},
       std::optional<size_t> expectedPosition = {},
       std::optional<std::vector<TurtleTriple>> expectedTriples = {})
    -> Parser {
  auto optionalParser = parseRule<Parser, rule, blankNodePrefix>(input);
  // We have to wrap this code into a void lambda because the `ASSERT_...`
  // macros only work inside void functions and we want to return the parser.
  [&]() {
    ASSERT_TRUE(optionalParser.has_value());
    auto& parser = optionalParser.value();
    ASSERT_EQ(parser.getPosition(), expectedPosition.value_or(input.size()));
    if (expectedLastParseResult.has_value()) {
      ASSERT_EQ(expectedLastParseResult, parser.getLastParseResult());
    }
    if (expectedTriples.has_value()) {
      ASSERT_EQ(expectedTriples, parser.getTriples());
    }
  }();
  return std::move(optionalParser.value());
};

// Formatted output of TurtleTriples in case of test failures.
std::ostream& operator<<(std::ostream& os, const TurtleTriple& tr) {
  os << "( " << tr.subject_ << " " << tr.predicate_ << " " << tr.object_ << ")";
  return os;
}
TEST(TurtleParserTest, prefixedName) {
  auto runCommonTests = [](auto& parser) {
    parser.prefixMap_["wd"] = "www.wikidata.org/";
    parser.setInputStream("wd:Q430 someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser.lastParseResult_, "<www.wikidata.org/Q430>");
    ASSERT_EQ(parser.getPosition(), size_t(7));

    parser.setInputStream(" wd:Q430 someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser.lastParseResult_, "<www.wikidata.org/Q430>");
    ASSERT_EQ(parser.getPosition(), 8u);

    // empty name
    parser.setInputStream("wd: someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser.lastParseResult_, "<www.wikidata.org/>");
    ASSERT_EQ(parser.getPosition(), size_t(3));

    parser.setInputStream("<wd.de> rest");
    parser.lastParseResult_ = "comp1";
    ASSERT_FALSE(parser.prefixedName());
    ASSERT_EQ(parser.lastParseResult_, "comp1");
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
    ASSERT_EQ(p.lastParseResult_, "<www.wikidata.org/esc,aped>");
    ASSERT_EQ(p.getPosition(), 12u);

    // escapes at the beginning are illegal, so this is parsed as an empty wd:
    p.setInputStream(R"(wd:\\esc\,aped)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p.lastParseResult_, "<www.wikidata.org/>");
    ASSERT_EQ(p.getPosition(), 3u);

    p.setInputStream(R"(wd:esc\,aped\.)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p.lastParseResult_, "<www.wikidata.org/esc,aped.>");
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
    ASSERT_THROW(p.prefixedName(), ad_utility::Exception);

    // TokenizerCTRE parses `esc\\aped` which fails to unescape (escaped
    // backslashes are not allowed in prefixed names)
    p.setInputStream(R"(wd:esc\\aped.)");
    ASSERT_THROW(p.prefixedName(), ad_utility::Exception);
  }
}

TEST(TurtleParserTest, prefixID) {
  auto runCommonTests = [](const auto& checker) {
    auto p = checker("@prefix bla:<www.bla.org/> .");
    ASSERT_EQ(p.prefixMap_["bla"], "www.bla.org/");

    // different spaces that don't change meaning
    std::string s;
    p = checker("@prefix bla: <www.bla.org/>.");
    ASSERT_EQ(p.prefixMap_["bla"], "www.bla.org/");

    // invalid LL1
    ASSERT_THROW(checker("@prefix bla<www.bla.org/>."),
                 Re2Parser::ParseException);

    s = "@prefxxix bla<www.bla.org/>.";
    p.setInputStream(s);
    p.lastParseResult_ = "comp1";
    ASSERT_FALSE(p.prefixID());
    ASSERT_EQ(p.lastParseResult_, "comp1");
    ASSERT_EQ(p.getPosition(), 0u);
  };

  auto checkRe2 = checkParseResult<Re2Parser, &Re2Parser::prefixID>;
  auto checkCTRE = checkParseResult<CtreParser, &CtreParser::prefixID>;
  runCommonTests(checkRe2);
  runCommonTests(checkCTRE);
}

TEST(TurtleParserTest, stringParse) {
  auto runCommonTests = [](const auto& checker) {
    std::string s1("\"double quote\"");
    std::string s1Normalized("\"double quote\"");
    std::string s2("\'single quote\'");
    std::string s2Normalized("\"single quote\"");
    std::string s3("\"\"\"multiline \n double quote\"\"\"");
    std::string s3Normalized("\"multiline \n double quote\"");
    std::string s4("\'\'\'multiline \n single quote\'\'\'");
    std::string s4Normalized("\"multiline \n single quote\"");

    checker(s1, lit(s1Normalized));
    checker(s2, lit(s2Normalized));
    // the main thing to test here is that s3 does not prefix-match the simple
    // string "" but the complex string """..."""
    checker(s3, lit(s3Normalized));
    checker(s4, lit(s4Normalized));
  };
  auto checkRe2 = checkParseResult<Re2Parser, &Re2Parser::stringParse>;
  auto checkCtre = checkParseResult<CtreParser, &CtreParser::stringParse>;
  runCommonTests(checkRe2);
  runCommonTests(checkCtre);
}

TEST(TurtleParserTest, rdfLiteral) {
  std::vector<string> literals;
  std::vector<TripleComponent> expected;
  literals.emplace_back(R"("simpleString")");
  expected.emplace_back(lit(R"("simpleString")"));
  literals.emplace_back(R"("langtag"@en-gb)");
  expected.emplace_back(lit(R"("langtag")", "@en-gb"));
  literals.emplace_back("\"valueLong\"^^<www.someunknownType/integer>");
  expected.emplace_back(
      lit("\"valueLong\"", "^^<www.someunknownType/integer>"));

  literals.emplace_back(R"("42.1234"^^)"s + "<" + XSD_DOUBLE_TYPE + ">");
  expected.emplace_back(42.1234);
  literals.push_back(R"("-142.321"^^)"s + "<" + XSD_DECIMAL_TYPE + ">");
  expected.emplace_back(-142.321);
  literals.push_back(R"("-142321"^^)"s + "<" + XSD_INT_TYPE + ">");
  expected.emplace_back(-142321);
  literals.push_back(R"("+144321"^^)"s + "<" + XSD_INTEGER_TYPE + ">");
  expected.emplace_back(144321);

  for (size_t i = 0; i < literals.size(); ++i) {
    checkParseResult<Re2Parser, &Re2Parser::rdfLiteral>(literals[i],
                                                        expected[i]);
    checkParseResult<CtreParser, &CtreParser::rdfLiteral>(literals[i],
                                                          expected[i]);
  }

  auto runCommonTests = [](auto p) {
    p.prefixMap_["doof"] = "www.doof.org/";

    string s("\"valuePrefixed\"^^doof:sometype");
    p.setInputStream(s);
    ASSERT_TRUE(p.rdfLiteral());
    ASSERT_EQ(p.lastParseResult_,
              lit("\"valuePrefixed\"", "^^<www.doof.org/sometype>"));
    ASSERT_EQ(p.getPosition(), s.size());
  };
  runCommonTests(Re2Parser{});
  runCommonTests(CtreParser{});
}

TEST(TurtleParserTest, blankNode) {
  auto runCommonTests = [](const auto& checker) {
    checker(" _:blank1", "_:u_blank1", 9);
    checker(" _:blank1 someRemainder", "_:u_blank1", 9);
    auto p = checker("  _:blank2 someOtherStuff", "_:u_blank2", 10);
    ASSERT_EQ(p.numBlankNodes_, 0u);
    // anonymous blank node
    p = checker(" [    \n\t  ]", "_:g_4_0", 11u);
    ASSERT_EQ(p.numBlankNodes_, 1u);
  };
  auto checkRe2 = checkParseResult<Re2Parser, &Re2Parser::blankNode, 4>;
  auto checkCtre = checkParseResult<CtreParser, &CtreParser::blankNode, 4>;
  runCommonTests(checkRe2);
  runCommonTests(checkCtre);
}

TEST(TurtleParserTest, blankNodePropertyList) {
  auto testPropertyListAsObject = [](auto p) {
    p.activeSubject_ = "<s>";
    p.activePredicate_ = "<p1>";

    string blankNodeL = "[<p2> <ob2>; <p3> <ob3>]";
    std::vector<TurtleTriple> exp = {{"_:g_5_0", "<p2>", "<ob2>"},
                                     {"_:g_5_0", "<p3>", "<ob3>"},
                                     {"<s>", "<p1>", "_:g_5_0"}};
    p.setInputStream(blankNodeL);
    p.setBlankNodePrefixOnlyForTesting(5);
    ASSERT_TRUE(p.object());
    ASSERT_EQ(p.triples_, exp);
    ASSERT_EQ(p.getPosition(), blankNodeL.size());

    blankNodeL = "[<2> <ob2>; \"invalidPred\" <ob3>]";
    p.setInputStream(blankNodeL);
    ASSERT_THROW(p.blankNodePropertyList(),
                 TurtleParser<Tokenizer>::ParseException);
  };
  testPropertyListAsObject(Re2Parser{});
  testPropertyListAsObject(CtreParser{});

  auto testPropertyListAsSubject = [](auto p) {
    string blankNodeL = "[<p2> <ob2>; <p3> <ob3>] <p1> <ob1>";
    std::vector<TurtleTriple> exp = {{"_:g_5_0", "<p2>", "<ob2>"},
                                     {"_:g_5_0", "<p3>", "<ob3>"},
                                     {"_:g_5_0", "<p1>", "<ob1>"}};
    p.setInputStream(blankNodeL);
    p.setBlankNodePrefixOnlyForTesting(5);
    ASSERT_TRUE(p.triples());
    ASSERT_EQ(p.triples_, exp);
    ASSERT_EQ(p.getPosition(), blankNodeL.size());

    blankNodeL = "[<2> <ob2>; \"invalidPred\" <ob3>]";
    p.setInputStream(blankNodeL);
    ASSERT_THROW(p.blankNodePropertyList(),
                 TurtleParser<Tokenizer>::ParseException);
  };
  testPropertyListAsSubject(Re2Parser{});
  testPropertyListAsSubject(CtreParser{});
}

TEST(TurtleParserTest, object) {
  auto runCommonTests = [](auto p) {
    string sub = "<sub>";
    string pred = "<pred>";
    p.activeSubject_ = sub;
    p.activePredicate_ = pred;
    p.prefixMap_["b"] = "bla/";
    string iri = " b:iri";
    p.setInputStream(iri);
    ASSERT_TRUE(p.object());
    ASSERT_EQ(p.lastParseResult_, "<bla/iri>");
    auto exp = TurtleTriple{sub, pred, "<bla/iri>"};
    ASSERT_EQ(p.triples_.back(), exp);

    string literal = "\"literal\"";
    p.setInputStream(literal);
    ASSERT_TRUE(p.object());
    ASSERT_EQ(p.lastParseResult_, lit(literal));
    exp = TurtleTriple{sub, pred, lit(literal)};
    ASSERT_EQ(p.triples_.back(), exp);

    string blank = "_:someblank";
    p.setInputStream(blank);
    ASSERT_TRUE(p.object());
    ASSERT_EQ(p.lastParseResult_, "_:u_someblank");

    exp = TurtleTriple{sub, pred, "_:u_someblank"};
    ASSERT_EQ(p.triples_.back(), exp);
  };
  runCommonTests(Re2Parser{});
  runCommonTests(CtreParser{});
}

TEST(TurtleParserTest, objectList) {
  auto runCommonTests = [](auto parser) {
    parser.activeSubject_ = "<s>";
    parser.activePredicate_ = "<p>";
    string objectL = " <ob1>, <ob2>, <ob3>";
    std::vector<TurtleTriple> exp;
    exp.push_back({"<s>", "<p>", "<ob1>"});
    exp.push_back({"<s>", "<p>", "<ob2>"});
    exp.push_back({"<s>", "<p>", "<ob3>"});
    parser.setInputStream(objectL);
    ASSERT_TRUE(parser.objectList());
    ASSERT_EQ(parser.triples_, exp);
    ASSERT_EQ(parser.getPosition(), objectL.size());

    parser.setInputStream("@noObject");
    ASSERT_FALSE(parser.objectList());

    parser.setInputStream("<obj1>, @illFormed");
    ASSERT_THROW(parser.objectList(), TurtleParser<Tokenizer>::ParseException);
  };
  runCommonTests(Re2Parser{});
  runCommonTests(CtreParser{});
}

TEST(TurtleParserTest, predicateObjectList) {
  auto runCommonTests = [](auto parser) {
    parser.activeSubject_ = "<s>";
    string predL = "\n <p1> <ob1>;<p2> \"ob2\",\n <ob3>";
    std::vector<TurtleTriple> exp;
    exp.push_back({"<s>", "<p1>", "<ob1>"});
    exp.push_back({"<s>", "<p2>", lit("\"ob2\"")});
    exp.push_back({"<s>", "<p2>", "<ob3>"});
    parser.setInputStream(predL);
    ASSERT_TRUE(parser.predicateObjectList());
    ASSERT_EQ(parser.triples_, exp);
    ASSERT_EQ(parser.getPosition(), predL.size());
  };
  runCommonTests(Re2Parser{});
  runCommonTests(CtreParser{});
}

TEST(TurtleParserTest, numericLiteral) {
  std::vector<std::string> literals{"2",   "-2",     "42.209",   "-42.239",
                                    ".74", "2.3e12", "2.34E-14", "-0.3e2",
                                    "3E2", "-14E-1", ".1E1",     "-.2E0"};
  std::vector<TripleComponent> expected{2,   -2,     42.209,   -42.239,
                                        .74, 2.3e12, 2.34e-14, -0.3e2,
                                        3e2, -14e-1, .1e1,     -.2e0};

  auto checkRe2 = checkParseResult<Re2Parser, &Re2Parser::numericLiteral>;
  auto checkCtre = checkParseResult<CtreParser, &CtreParser::numericLiteral>;
  for (size_t i = 0; i < literals.size(); ++i) {
    checkRe2(literals[i], expected[i]);
    checkCtre(literals[i], expected[i]);
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
      ASSERT_EQ(result[0].object_, expectedObjects[i]);
    }
  };
  auto runCommonTests = [&assertParsingFails, &testTripleObjects,
                         &parseAllTriples]<typename Parser>(const Parser&) {
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
      Parser parser;
      parser.prefixMap_["xsd"] = "http://www.w3.org/2001/XMLSchema#";
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
      Parser parser;
      parser.prefixMap_["xsd"] = "http://www.w3.org/2001/XMLSchema#";
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
      Parser parser;
      parser.prefixMap_["xsd"] = "http://www.w3.org/2001/XMLSchema#";
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
      Parser parser;
      parser.prefixMap_["xsd"] = "http://www.w3.org/2001/XMLSchema#";
      parser.integerOverflowBehavior() =
          TurtleParserIntegerOverflowBehavior::AllToDouble;
      for (const auto& input : nonWorkingInputs) {
        assertParsingFails(parser, input);
      }
      std::vector<std::string> workingInputs{
          "<a> <b> \"123\"^^xsd:integer",
          "<a> <b> 456",
          "<a> <b> \"-9999.0\"^^xsd:integer",
          "<a> <b> \"9999.0\"^^xsd:short",
          "<a> <b> \"9999E4\"^^xsd:nonNegativeInteger",
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
      Parser parser;
      parser.invalidLiteralsAreSkipped() = true;
      auto result = parseAllTriples(parser, input);
      ASSERT_EQ(result, expected);
    }
    {
      // Test the skipping of unsupported triples with the "overflow to double"
      // behavior.
      std::string input{
          "<a> <b> 99999999999999999999999. <c> <d> \"invalid\"^^xsd:integer . "
          "<e> <f> 234"};
      std::vector<TurtleTriple> expected{
          {"<a>", "<b>", 99999999999999999999999.0}, {"<e>", "<f>", 234}};
      Parser parser;
      parser.prefixMap_["xsd"] = "http://www.w3.org/2001/XMLSchema#";
      parser.invalidLiteralsAreSkipped() = true;
      parser.integerOverflowBehavior() =
          TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
      auto result = parseAllTriples(parser, input);
      ASSERT_EQ(result, expected);
    }
  };
  runCommonTests(Re2Parser{});
  runCommonTests(CtreParser{});
}

TEST(TurtleParserTest, DateLiterals) {
  std::vector<std::string> dateLiterals{
      R"("2000-10-15"^^<)"s + XSD_DATE_TYPE + ">",
      R"("-2014-03-16T12:13:52"^^<)"s + XSD_DATETIME_TYPE + ">",
      R"("2084"^^<)"s + XSD_GYEAR_TYPE + ">",
      R"("2083-12"^^<)"s + XSD_GYEARMONTH_TYPE + ">"};
  using L = DateOrLargeYear;
  std::vector<DateOrLargeYear> expected{
      L{Date{2000, 10, 15}}, L{Date{-2014, 3, 16, 12, 13, 52}},
      L{Date{2084, 0, 0}}, L{Date{2083, 12, 0}}};

  for (size_t i = 0; i < dateLiterals.size(); ++i) {
    checkParseResult<Re2Parser, &Re2Parser::object>(dateLiterals[i],
                                                    expected[i]);
    checkParseResult<CtreParser, &CtreParser::object>(dateLiterals[i],
                                                      expected[i]);
  }

  std::vector<std::string> invalidDateLiterals{
      R"("2000-10"^^<)"s + XSD_DATE_TYPE + ">",
      R"("2000-50"^^<)"s + XSD_GYEARMONTH_TYPE + ">",
      R"("-2014-03-16T12:13"^^<)"s + XSD_DATETIME_TYPE + ">",
      R"("2084-12"^^<)"s + XSD_GYEAR_TYPE + ">",
      R"("20##"^^<)"s + XSD_GYEAR_TYPE + ">",
      R"("2083-12-13"^^<)"s + XSD_GYEARMONTH_TYPE + ">"};
  std::vector<TripleComponent::Literal> expectedInvalidDateLiterals{
      lit("\"2000-10\""),   lit("\"2000-50\""), lit(R"("-2014-03-16T12:13")"s),
      lit(R"("2084-12")"s), lit(R"("20##")"s),  lit(R"("2083-12-13")"s)};
  using L = DateOrLargeYear;
  for (size_t i = 0; i < invalidDateLiterals.size(); ++i) {
    checkParseResult<Re2Parser, &Re2Parser::object>(
        invalidDateLiterals[i], expectedInvalidDateLiterals[i]);
    checkParseResult<CtreParser, &CtreParser::object>(
        invalidDateLiterals[i], expectedInvalidDateLiterals[i]);
  }
}

TEST(TurtleParserTest, booleanLiteral) {
  auto runCommonTests = [](const auto& ruleChecker, const auto& ruleParser) {
    ruleChecker("true", true);
    ruleChecker("false", false);
    ASSERT_FALSE(ruleParser("maybe"));
  };
  runCommonTests(checkParseResult<Re2Parser, &Re2Parser::booleanLiteral>,
                 parseRule<Re2Parser, &Re2Parser::booleanLiteral>);
  runCommonTests(checkParseResult<CtreParser, &CtreParser::booleanLiteral>,
                 parseRule<CtreParser, &CtreParser::booleanLiteral>);
}

TEST(TurtleParserTest, booleanLiteralLongForm) {
  auto runCommonTests = [](const auto& ruleChecker) {
    ruleChecker("\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>", true);
    ruleChecker("\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>", false);
    ruleChecker("\"maybe\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
                lit("\"maybe\""));
  };
  runCommonTests(checkParseResult<Re2Parser, &Re2Parser::rdfLiteral>);
  runCommonTests(checkParseResult<CtreParser, &CtreParser::rdfLiteral>);
}

TEST(TurtleParserTest, collection) {
  auto runCommonTests = [](const auto& checker) {
    using TC = TripleComponent;
    using TT = TurtleTriple;
    std::string nil = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
    std::string first = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
    std::string rest = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";

    checker("()", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>");

    checker("(42 <alpha> \"me\")", TC{"_:g_22_0"}, {},
            std::vector<TT>{{"_:g_22_0", first, 42},
                            {"_:g_22_0", rest, "_:g_22_1"},
                            {"_:g_22_1", first, "<alpha>"},
                            {"_:g_22_1", rest, "_:g_22_2"},
                            {"_:g_22_2", first, lit("\"me\"")},
                            {"_:g_22_2", rest, nil}});
  };
  runCommonTests(checkParseResult<Re2Parser, &Re2Parser::collection, 22>);
  runCommonTests(checkParseResult<CtreParser, &CtreParser::collection, 22>);
}

// Sort a vector of triples to get a deterministic order for comparison.
void sortTriples(std::vector<TurtleTriple>& triples) {
  auto toRef = [](const TurtleTriple& t) {
    return std::tie(t.subject_, t.predicate_, t.object_.getString());
  };
  std::ranges::sort(triples, std::less{}, toRef);
}

// Parse the file at `filename` using a parser of type `Parser` and return the
// sorted result. Iff `useBatchInterface` then the `getBatch()` function is used
// for parsing, else `getLine()` is used.
template <typename Parser>
std::vector<TurtleTriple> parseFromFile(const std::string& filename,
                                        bool useBatchInterface) {
  Parser parserChild{filename};
  TurtleParserBase& parser = parserChild;

  std::vector<TurtleTriple> result;
  if (useBatchInterface) {
    while (auto batch = parser.getBatch()) {
      result.insert(result.end(), batch.value().begin(), batch.value().end());
      parser.printAndResetQueueStatistics();
    }
  } else {
    TurtleTriple next;
    while (parser.getLine(next)) {
      result.push_back(next);
    }
  }
  // The order of triples in not necessarily the same, so we sort them.
  sortTriples(result);
  return result;
}

// Run a function that takes a bool as the first argument (typically the
// `useBatchInterface` argument) and possible additional args, and run this
// function for all the different parsers that can read from a file (stream and
// parallel parser, with all the combinations of the different tokenizers).
auto forAllParallelParsers(const auto& function, const auto&... args) {
  function.template operator()<TurtleParallelParser<Tokenizer>>(true, args...);
  function.template operator()<TurtleParallelParser<Tokenizer>>(false, args...);
  function.template operator()<TurtleParallelParser<TokenizerCtre>>(true,
                                                                    args...);
  function.template operator()<TurtleParallelParser<TokenizerCtre>>(false,
                                                                    args...);
}
auto forAllParsers(const auto& function, const auto&... args) {
  function.template operator()<TurtleStreamParser<Tokenizer>>(true, args...);
  function.template operator()<TurtleStreamParser<Tokenizer>>(false, args...);
  function.template operator()<TurtleStreamParser<TokenizerCtre>>(true,
                                                                  args...);
  function.template operator()<TurtleStreamParser<TokenizerCtre>>(false,
                                                                  args...);
  forAllParallelParsers(function, args...);
}

TEST(TurtleParserTest, TurtleStreamAndParallelParser) {
  std::string filename{"turtleStreamAndParallelParserTest.dat"};
  std::vector<TurtleTriple> expectedTriples;
  {
    auto of = ad_utility::makeOfstream(filename);
    for (size_t i = 0; i < 1'000; ++i) {
      auto subject = absl::StrCat("<", i / 1000, ">");
      auto predicate = absl::StrCat("<", i / 100, ">");
      auto object = absl::StrCat("<", i / 10, ">");
      of << subject << ' ' << predicate << ' ' << object << ".\n";
      expectedTriples.emplace_back(subject, predicate, object);
    }
  }
  // The order of triples in not necessarily the same, so we sort them.
  sortTriples(expectedTriples);

  FILE_BUFFER_SIZE = 1000;
  auto testWithParser = [&]<typename Parser>(bool useBatchInterface) {
    auto result = parseFromFile<Parser>(filename, useBatchInterface);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expectedTriples));
  };

  forAllParsers(testWithParser);
  ad_utility::deleteFile(filename);
}

// _______________________________________________________________________
TEST(TurtleParserTest, emptyInput) {
  std::string filename{"turtleParserEmptyInput.dat"};
  FILE_BUFFER_SIZE = 1000;
  auto testWithParser = [&]<typename Parser>(bool useBatchInterface,
                                             std::string_view input = "") {
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    auto result = parseFromFile<Parser>(filename, useBatchInterface);
    EXPECT_THAT(result, ::testing::ElementsAre());
    ad_utility::deleteFile(filename);
  };

  forAllParsers(testWithParser, "");
  std::string onlyPrefixes = "PREFIX bim: <http://www.bimm.bam.de/blubb/>";
  forAllParsers(testWithParser, onlyPrefixes);
}

// ________________________________________________________________________
TEST(TurtleParserTest, multilineComments) {
  std::string filename{"turtleParserMultilineComments.dat"};
  FILE_BUFFER_SIZE = 1000;
  auto testWithParser = [&]<typename Parser>(bool useBatchInterface,
                                             std::string_view input,
                                             const auto& expectedTriples) {
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    auto result = parseFromFile<Parser>(filename, useBatchInterface);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expectedTriples));
    ad_utility::deleteFile(filename);
  };

  // Test an input with many lines that only comments and whitespace. There
  // was a bug for this case in a previous version of the parser.
  std::string input = R"(#Comments
  #at
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning
##the beginning

#and so on
<s> <p> <o> .
#comment

#comment
#commentnext
<s> <p> <o2> .

)";
  std::vector<TurtleTriple> expected{{"<s>", "<p>", "<o>"},
                                     {"<s>", "<p>", "<o2>"}};
  sortTriples(expected);
  forAllParsers(testWithParser, input, expected);
}

// Test that exceptions during the turtle parsing are properly propagated to the
// calling code. This is especially important for the parallel parsers where the
// actual parsing happens on background threads.
TEST(TurtleParserTest, exceptionPropagation) {
  std::string filename{"turtleParserExceptionPropagation.dat"};
  FILE_BUFFER_SIZE = 1000;
  auto testWithParser = [&]<typename Parser>(bool useBatchInterface,
                                             std::string_view input) {
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    AD_EXPECT_THROW_WITH_MESSAGE(
        (parseFromFile<Parser>(filename, useBatchInterface)),
        ::testing::ContainsRegex("Parse error"));
    ad_utility::deleteFile(filename);
  };
  forAllParsers(testWithParser, "<missing> <object> .");
}

// Test that exceptions in the batched reading of the input file are properly
// propagated.
TEST(TurtleParserTest, exceptionPropagationFileBufferReading) {
  std::string filename{"turtleParserExceptionPropagationFileBufferReading.dat"};
  auto testWithParser = [&]<typename Parser>(bool useBatchInterface,
                                             std::string_view input) {
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    AD_EXPECT_THROW_WITH_MESSAGE(
        (parseFromFile<Parser>(filename, useBatchInterface)),
        ::testing::ContainsRegex("Please increase the FILE_BUFFER_SIZE"));
    ad_utility::deleteFile(filename);
  };
  // Deliberately chosen s.t. the first triple fits in a block, but the second
  // one doesn't.
  FILE_BUFFER_SIZE = 40;
  forAllParallelParsers(testWithParser,
                        "<subject> <predicate> <object> . \n <veryLongSubject> "
                        "<veryLongPredicate> <veryLongObject> .");
}

// Test that the parallel parser's destructor can be run quickly and without
// blocking, even when there are still lots of blocks in the pipeline that are
// currently being parsed.
TEST(TurtleParserTest, stopParsingOnOutsideFailure) {
#ifdef __APPLE__
  GTEST_SKIP_("sleep_for is unreliable for macos builds");
#endif
  std::string filename{"turtleParserStopParsingOnOutsideFailure.dat"};
  auto testWithParser = [&]<typename Parser>(
                            [[maybe_unused]] bool useBatchInterface,
                            std::string_view input) {
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    ad_utility::Timer t{ad_utility::Timer::Stopped};
    {
      [[maybe_unused]] Parser parserChild{filename, 10ms};
      t.cont();
    }
    EXPECT_LE(t.msecs(), 20ms);
  };
  const std::string input = []() {
    std::string singleBlock = "<subject> <predicate> <object> . \n ";
    std::string longBlock;
    longBlock.reserve(200 * singleBlock.size());
    for ([[maybe_unused]] size_t i : ad_utility::integerRange(200ul)) {
      longBlock.append(singleBlock);
    }
    return longBlock;
  }();
  FILE_BUFFER_SIZE = 40;
  forAllParallelParsers(testWithParser, input);
}
