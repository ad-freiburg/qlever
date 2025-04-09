// Copyright 2018 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <iostream>
#include <string>

#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "parser/RdfParser.h"
#include "parser/TripleComponent.h"
#include "util/Conversions.h"
#include "util/MemorySize/MemorySize.h"

using std::string;
using namespace std::literals;
using ad_utility::use_type_identity::ti;
using Re2Parser = RdfStringParser<TurtleParser<Tokenizer>>;
using CtreParser = RdfStringParser<TurtleParser<TokenizerCtre>>;
using NQuadRe2Parser = RdfStringParser<NQuadParser<Tokenizer>>;
using NQuadCtreParser = RdfStringParser<NQuadParser<TokenizerCtre>>;

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;
auto iri = [](std::string_view s) {
  return TripleComponent::Iri::fromIriref(s);
};
}  // namespace

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
    EXPECT_EQ(parser.getPosition(), expectedPosition.value_or(input.size()));
    if (expectedLastParseResult.has_value()) {
      EXPECT_EQ(expectedLastParseResult, parser.getLastParseResult());
    }
    if (expectedTriples.has_value()) {
      EXPECT_EQ(expectedTriples, parser.getTriples());
    }
  }();
  return std::move(optionalParser.value());
};

// Formatted output of TurtleTriples in case of test failures.
std::ostream& operator<<(std::ostream& os, const TurtleTriple& tr) {
  os << "( " << tr.subject_ << " " << tr.predicate_.toStringRepresentation()
     << " " << tr.object_ << " " << tr.graphIri_ << ")";
  return os;
}
TEST(RdfParserTest, prefixedName) {
  auto runCommonTests = [](auto& parser) {
    parser.prefixMap_["wd"] = iri("<www.wikidata.org/>");
    parser.setInputStream("wd:Q430 someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser.lastParseResult_, iri("<www.wikidata.org/Q430>"));
    ASSERT_EQ(parser.getPosition(), size_t(7));

    parser.setInputStream(" wd:Q430 someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser.lastParseResult_, iri("<www.wikidata.org/Q430>"));
    ASSERT_EQ(parser.getPosition(), 8u);

    // empty name
    parser.setInputStream("wd: someotherContent");
    ASSERT_TRUE(parser.prefixedName());
    ASSERT_EQ(parser.lastParseResult_, iri("<www.wikidata.org/>"));
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
    Re2Parser p;
    runCommonTests(p);
    p.setInputStream(R"(wd:esc\,aped)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p.lastParseResult_, iri("<www.wikidata.org/esc,aped>"));
    ASSERT_EQ(p.getPosition(), 12u);

    // escapes at the beginning are illegal, so this is parsed as an empty wd:
    p.setInputStream(R"(wd:\\esc\,aped)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p.lastParseResult_, iri("<www.wikidata.org/>"));
    ASSERT_EQ(p.getPosition(), 3u);

    p.setInputStream(R"(wd:esc\,aped\.)");
    ASSERT_TRUE(p.prefixedName());
    ASSERT_EQ(p.lastParseResult_, iri("<www.wikidata.org/esc,aped.>"));
    ASSERT_EQ(p.getPosition(), 14u);
  }

  {
    CtreParser p;
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

TEST(RdfParserTest, prefixID) {
  auto runCommonTests = [](const auto& checker) {
    auto p = checker("@prefix bla:<www.bla.org/> .");
    ASSERT_EQ(p.prefixMap_["bla"], iri("<www.bla.org/>"));

    // different spaces that don't change meaning
    std::string s;
    p = checker("@prefix bla: <www.bla.org/>.");
    ASSERT_EQ(p.prefixMap_["bla"], iri("<www.bla.org/>"));

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

TEST(RdfParserTest, stringParse) {
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

TEST(RdfParserTest, rdfLiteral) {
  std::vector<string> literals;
  std::vector<TripleComponent> expected;
  literals.emplace_back(R"("simpleString")");
  expected.emplace_back(lit(R"("simpleString")"));
  literals.emplace_back(R"("string\"with \\ escapes\n"^^<www.x.de>)");
  expected.emplace_back(TripleComponent::Literal::fromEscapedRdfLiteral(
      R"("string\"with \\ escapes\n")",
      TripleComponent::Iri::fromIriref("<www.x.de>")));
  literals.emplace_back(R"("langtag"@en-gb)");
  expected.emplace_back(lit(R"("langtag")", "@en-gb"));
  literals.emplace_back("\"valueLong\"^^<www.someunknownType/integer>");
  expected.emplace_back(
      lit("\"valueLong\"", "^^<www.someunknownType/integer>"));

  literals.emplace_back(R"("42.1234"^^)"s + "<" + XSD_DOUBLE_TYPE + ">");
  expected.emplace_back(42.1234);
  literals.emplace_back(R"("+42.2345"^^)"s + "<" + XSD_DOUBLE_TYPE + ">");
  expected.emplace_back(42.2345);
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
    p.prefixMap_["doof"] = iri("<www.doof.org/>");

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

TEST(RdfParserTest, literalAndDatatypeToTripleComponent) {
  auto ladttc =
      TurtleParser<TokenizerCtre>::literalAndDatatypeToTripleComponent;
  auto fromIri = TripleComponent::Iri::fromIrirefWithoutBrackets;

  ASSERT_EQ(ladttc("42.1234", fromIri(XSD_DOUBLE_TYPE)), 42.1234);
  ASSERT_EQ(ladttc("+42.2345", fromIri(XSD_DOUBLE_TYPE)), +42.2345);
  ASSERT_EQ(ladttc("-142.321", fromIri(XSD_DECIMAL_TYPE)), -142.321);
  ASSERT_EQ(ladttc("-142321", fromIri(XSD_INT_TYPE)), -142321);
  ASSERT_EQ(ladttc("+144321", fromIri(XSD_INTEGER_TYPE)), +144321);
  ASSERT_EQ(ladttc("true", fromIri(XSD_BOOLEAN_TYPE)), true);
  ASSERT_EQ(ladttc("false", fromIri(XSD_BOOLEAN_TYPE)), false);
  auto result = ladttc("POINT(7.8 47.9)", fromIri(GEO_WKT_LITERAL));
  auto vid = result.toValueIdIfNotString();
  ASSERT_TRUE(vid.has_value() &&
              vid.value().getDatatype() == Datatype::GeoPoint);
  auto result2 = ladttc("POLYGON(7.8 47.9, 40.0 40.5, 10.9 20.5)",
                        fromIri(GEO_WKT_LITERAL));
  ASSERT_TRUE(result2.isLiteral());
  EXPECT_EQ(asStringViewUnsafe(result2.getLiteral().getContent()),
            "POLYGON(7.8 47.9, 40.0 40.5, 10.9 20.5)");
  EXPECT_EQ(asStringViewUnsafe(result2.getLiteral().getDatatype()),
            GEO_WKT_LITERAL);
  // Invalid points should be converted to plain string literals
  auto result3 = ladttc("POINT(0.0 99.9999)", fromIri(GEO_WKT_LITERAL));
  ASSERT_FALSE(result3.getLiteral().hasDatatype());
  ASSERT_EQ(asStringViewUnsafe(result3.getLiteral().getContent()),
            "POINT(0.0 99.9999)");
}

TEST(RdfParserTest, blankNode) {
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
  auto checkRe2Subject = checkParseResult<Re2Parser, &Re2Parser::subject, 4>;
  auto checkCtreSubject = checkParseResult<CtreParser, &CtreParser::subject, 4>;
  runCommonTests(checkRe2);
  runCommonTests(checkCtre);
  runCommonTests(checkRe2Subject);
  runCommonTests(checkCtreSubject);
}

TEST(RdfParserTest, blankNodePropertyList) {
  auto testPropertyListAsObject = [](auto p) {
    p.activeSubject_ = iri("<s>");
    p.activePredicate_ = iri("<p1>");

    string blankNodeL = "[<p2> <ob2>; <p3> <ob3>]";
    std::vector<TurtleTriple> exp = {{"_:g_5_0", iri("<p2>"), iri("<ob2>")},
                                     {"_:g_5_0", iri("<p3>"), iri("<ob3>")},
                                     {iri("<s>"), iri("<p1>"), "_:g_5_0"}};
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
    std::vector<TurtleTriple> exp = {{"_:g_5_0", iri("<p2>"), iri("<ob2>")},
                                     {"_:g_5_0", iri("<p3>"), iri("<ob3>")},
                                     {"_:g_5_0", iri("<p1>"), iri("<ob1>")}};
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

TEST(RdfParserTest, base) {
  auto testForGivenParser = [](auto parser) {
    parser.setInputStream("@base <http://www.example.org/path/> .");
    ASSERT_TRUE(parser.base());
    ASSERT_EQ(parser.baseForRelativeIri().toStringRepresentation(),
              "<http://www.example.org/path/>");
    ASSERT_EQ(parser.baseForAbsoluteIri().toStringRepresentation(),
              "<http://www.example.org/>");
    parser.setInputStream("@base \"no iriref\" .");
    ASSERT_THROW(parser.base(), TurtleParser<Tokenizer>::ParseException);
  };
  testForGivenParser(Re2Parser{});
  testForGivenParser(CtreParser{});
}

TEST(RdfParserTest, sparqlBase) {
  auto testForGivenParser = [](auto parser) {
    parser.setInputStream("BASE <http://www.example.org/path/> .");
    ASSERT_TRUE(parser.sparqlBase());
    ASSERT_EQ(parser.baseForRelativeIri().toStringRepresentation(),
              "<http://www.example.org/path/>");
    ASSERT_EQ(parser.baseForAbsoluteIri().toStringRepresentation(),
              "<http://www.example.org/>");
    parser.setInputStream("BASE \"no iriref\" .");
    ASSERT_THROW(parser.sparqlBase(), TurtleParser<Tokenizer>::ParseException);
  };
  testForGivenParser(Re2Parser{});
  testForGivenParser(CtreParser{});
}

TEST(RdfParserTest, object) {
  auto runCommonTests = [](auto p) {
    auto sub = iri("<sub>");
    auto pred = iri("<pred>");
    p.activeSubject_ = sub;
    p.activePredicate_ = pred;
    p.prefixMap_["b"] = iri("<bla/>");
    string input = " b:input";
    p.setInputStream(input);
    ASSERT_TRUE(p.object());
    ASSERT_EQ(p.lastParseResult_, iri("<bla/input>"));
    auto exp = TurtleTriple{sub, pred, iri("<bla/input>")};
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

TEST(RdfParserTest, objectList) {
  auto runCommonTests = [](auto parser) {
    parser.activeSubject_ = iri("<s>");
    parser.activePredicate_ = iri("<p>");
    string objectL = " <ob1>, <ob2>, <ob3>";
    std::vector<TurtleTriple> exp;
    exp.push_back({iri("<s>"), iri("<p>"), iri("<ob1>")});
    exp.push_back({iri("<s>"), iri("<p>"), iri("<ob2>")});
    exp.push_back({iri("<s>"), iri("<p>"), iri("<ob3>")});
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

TEST(RdfParserTest, predicateObjectList) {
  auto runCommonTests = [](auto parser) {
    parser.activeSubject_ = iri("<s>");
    string predL = "\n <p1> <ob1>;<p2> \"ob2\",\n <ob3>";
    std::vector<TurtleTriple> exp;
    exp.push_back({iri("<s>"), iri("<p1>"), iri("<ob1>")});
    exp.push_back({iri("<s>"), iri("<p2>"), lit("\"ob2\"")});
    exp.push_back({iri("<s>"), iri("<p2>"), iri("<ob3>")});
    parser.setInputStream(predL);
    ASSERT_TRUE(parser.predicateObjectList());
    ASSERT_EQ(parser.triples_, exp);
    ASSERT_EQ(parser.getPosition(), predL.size());
  };
  runCommonTests(Re2Parser{});
  runCommonTests(CtreParser{});
}

TEST(RdfParserTest, numericLiteral) {
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

TEST(RdfParserTest, numericLiteralErrorBehavior) {
  auto assertParsingFails = [](auto& parser, std::string input) {
    parser.setInputStream(input);
    ASSERT_THROW(parser.parseAndReturnAllTriples(), Re2Parser::ParseException);
  };

  auto parseAllTriples = [](auto& parser, std::string input) {
    parser.setInputStream(input);
    return parser.parseAndReturnAllTriples();
  };

  auto testTripleObjects = [&parseAllTriples](auto& parser,
                                              std::vector<std::string> triples,
                                              auto expectedObjects) {
    for (size_t i = 0; i < triples.size(); ++i) {
      const auto& triple = triples[i];
      std::vector<TurtleTriple> result;
      ASSERT_NO_THROW(result = parseAllTriples(parser, triple));
      ASSERT_EQ(result.size(), 1ul);
      ASSERT_EQ(result[0].object_, expectedObjects[i]);
    }
  };
  auto runCommonTests = [&assertParsingFails, &testTripleObjects,
                         &parseAllTriples](const auto& p) {
    using Parser = std::decay_t<decltype(p)>;
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
      parser.prefixMap_["xsd"] = iri("<http://www.w3.org/2001/XMLSchema#>");
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
      parser.prefixMap_["xsd"] = iri("<http://www.w3.org/2001/XMLSchema#>");
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
      parser.prefixMap_["xsd"] = iri("<http://www.w3.org/2001/XMLSchema#>");
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
      parser.prefixMap_["xsd"] = iri("<http://www.w3.org/2001/XMLSchema#>");
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
      std::vector<TurtleTriple> expected{{iri("<a>"), iri("<b>"), 123},
                                         {iri("<e>"), iri("<f>"), 234}};
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
          {iri("<a>"), iri("<b>"), 99999999999999999999999.0},
          {iri("<e>"), iri("<f>"), 234}};
      Parser parser;
      parser.prefixMap_["xsd"] = iri("<http://www.w3.org/2001/XMLSchema#>");
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

TEST(RdfParserTest, DateLiterals) {
  std::vector<std::string> dateLiterals{
      R"("2000-10-15"^^<)"s + XSD_DATE_TYPE + ">",
      R"("-2014-03-16T12:13:52"^^<)"s + XSD_DATETIME_TYPE + ">",
      R"("2084"^^<)"s + XSD_GYEAR_TYPE + ">",
      R"("2083-12"^^<)"s + XSD_GYEARMONTH_TYPE + ">"};
  using L = DateYearOrDuration;
  std::vector<DateYearOrDuration> expected{
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
  using L = DateYearOrDuration;
  for (size_t i = 0; i < invalidDateLiterals.size(); ++i) {
    checkParseResult<Re2Parser, &Re2Parser::object>(
        invalidDateLiterals[i], expectedInvalidDateLiterals[i]);
    checkParseResult<CtreParser, &CtreParser::object>(
        invalidDateLiterals[i], expectedInvalidDateLiterals[i]);
  }
}

TEST(RdfParserTest, DayTimeDurationLiterals) {
  std::vector<std::string> dayTimeDurationLiterals{
      R"("P0DT0H0M0.00S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("PT0S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("-P365DT23H23M59.99S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("-PT126.76S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("-P9999D"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("P23DT23.22S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("PT72H134M129.98S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">"};

  using L = DateYearOrDuration;
  auto positive = DayTimeDuration::Type::Positive;
  auto negative = DayTimeDuration::Type::Negative;

  std::vector<DateYearOrDuration> expected{
      L{DayTimeDuration{}},
      L{DayTimeDuration{}},
      L{DayTimeDuration{negative, 365, 23, 23, 59.9900}},
      L{DayTimeDuration{negative, 0, 0, 0, 126.7600}},
      L{DayTimeDuration{negative, 9999, 0, 0, 0.0000}},
      L{DayTimeDuration{positive, 23, 0, 0, 23.2200}},
      L{DayTimeDuration{positive, 0, 72, 134, 129.9800}}};

  for (size_t i = 0; i < dayTimeDurationLiterals.size(); ++i) {
    checkParseResult<Re2Parser, &Re2Parser::object>(dayTimeDurationLiterals[i],
                                                    expected[i]);
    checkParseResult<CtreParser, &CtreParser::object>(
        dayTimeDurationLiterals[i], expected[i]);
  }

  std::vector<std::string> invalidDayTimeDurationLiterals{
      R"("PinvalidDH2.53S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("P234D23H23M23.55S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("PH-33.33S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("P2001693"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("-98D59M59.99S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("-PDTS"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("P200000032DT4H3M4S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">",
      R"("-P1048576DT4H3M4.23S"^^<)"s + XSD_DAYTIME_DURATION_TYPE + ">"};

  std::vector<TripleComponent::Literal> expectedInvalidDayTimeDurationLiterals{
      lit("\"PinvalidDH2.53S\""),    lit("\"P234D23H23M23.55S\""),
      lit("\"PH-33.33S\""),          lit("\"P2001693\""),
      lit("\"-98D59M59.99S\""),      lit("\"-PDTS\""),
      lit("\"P200000032DT4H3M4S\""), lit("\"-P1048576DT4H3M4.23S\"")};

  for (size_t i = 0; i < invalidDayTimeDurationLiterals.size(); ++i) {
    checkParseResult<Re2Parser, &Re2Parser::object>(
        invalidDayTimeDurationLiterals[i],
        expectedInvalidDayTimeDurationLiterals[i]);
    checkParseResult<CtreParser, &CtreParser::object>(
        invalidDayTimeDurationLiterals[i],
        expectedInvalidDayTimeDurationLiterals[i]);
  }
}

TEST(RdfParserTest, booleanLiteral) {
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

TEST(RdfParserTest, booleanLiteralLongForm) {
  auto runCommonTests = [](const auto& ruleChecker) {
    ruleChecker("\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>", true);
    ruleChecker("\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>", false);
    ruleChecker("\"maybe\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
                lit("\"maybe\""));
  };
  runCommonTests(checkParseResult<Re2Parser, &Re2Parser::rdfLiteral>);
  runCommonTests(checkParseResult<CtreParser, &CtreParser::rdfLiteral>);
}

TEST(RdfParserTest, collection) {
  auto runCommonTests = [](const auto& checker) {
    using TC = TripleComponent;
    using TT = TurtleTriple;
    auto nil = iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>");
    auto first = iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>");
    auto rest = iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>");

    checker("()", nil);

    checker("(42 <alpha> \"me\")", TC{"_:g_22_0"}, {},
            std::vector<TT>{{"_:g_22_0", first, 42},
                            {"_:g_22_0", rest, "_:g_22_1"},
                            {"_:g_22_1", first, iri("<alpha>")},
                            {"_:g_22_1", rest, "_:g_22_2"},
                            {"_:g_22_2", first, lit("\"me\"")},
                            {"_:g_22_2", rest, nil}});
  };
  runCommonTests(checkParseResult<Re2Parser, &Re2Parser::collection, 22>);
  runCommonTests(checkParseResult<CtreParser, &CtreParser::collection, 22>);
}

// Test the parsing of an IRI reference.
TEST(RdfParserTest, iriref) {
  // Run test for given parser.
  auto runTestsForParser = [](auto parser) {
    std::string iriref_1 = "<fine>";
    std::string iriref_2 = "<okay ish>";
    std::string iriref_3 = "<not\x19okay_for_RE2>";
    std::string iriref_4 = "<throws\"exception>";
    std::string iriref_5 = "no iriref at all";
    // The first IRI ref is fine for both parsers.
    parser.setInputStream(iriref_1);
    ASSERT_TRUE(parser.iriref());
    ASSERT_EQ(parser.lastParseResult_, iri(iriref_1));
    // The second IRI ref is accepted by both parsers, but produces a
    // warning for the Re2Parser.
    testing::internal::CaptureStdout();
    parser.setInputStream(iriref_2);
    ASSERT_TRUE(parser.iriref());
    ASSERT_EQ(parser.lastParseResult_, iri(iriref_2));
    std::string warning = testing::internal::GetCapturedStdout();
    if constexpr (std::is_same_v<decltype(parser), CtreParser>) {
      EXPECT_EQ(warning, "");
    } else {
      EXPECT_THAT(warning, ::testing::HasSubstr("not standard-compliant"));
      EXPECT_THAT(warning, ::testing::HasSubstr(iriref_2));
    }
    // The third IRI ref is accepted by the CtreParser, but not by the
    // Re2Parser.
    parser.setInputStream(iriref_3);
    if constexpr (std::is_same_v<decltype(parser), CtreParser>) {
      ASSERT_TRUE(parser.iriref());
      ASSERT_EQ(parser.lastParseResult_, iri(iriref_3));
    } else {
      ASSERT_FALSE(parser.iriref());
    }
    // The fourth IRI ref throws an exception when parsed (because " is
    // encountered before the closing >).
    parser.setInputStream(iriref_4);
    ASSERT_THROW(parser.iriref(), TurtleParser<Tokenizer>::ParseException);
    // The fifth IRI ref is not recognized as an IRI ref.
    parser.setInputStream(iriref_5);
    ASSERT_FALSE(parser.iriref());
  };
  // Run tests for both parsers and reset std::cout.
  runTestsForParser(Re2Parser{});
  runTestsForParser(CtreParser{});
}

// Parse the file at `filename` using a parser of type `Parser` and return the
// sorted result. Iff `useBatchInterface` then the `getBatch()` function is used
// for parsing, else `getLine()` is used. The default size for the parse buffer
// in the following tests is 1 kB (which is much less than the default value
// `DEFAULT_PARSER_BUFFER_SIZE` defined in `src/global/Constants.h`).
template <typename Parser>
std::vector<TurtleTriple> parseFromFile(
    const std::string& filename, bool useBatchInterface,
    ad_utility::MemorySize bufferSize = 1_kB) {
  auto parserChild = [&]() {
    if constexpr (ad_utility::isInstantiation<Parser, RdfMultifileParser>) {
      return Parser{{{filename, qlever::Filetype::Turtle, std::nullopt}},
                    bufferSize};
    } else {
      return Parser{filename, bufferSize};
    }
  }();
  RdfParserBase& parser = parserChild;

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
  return result;
}

// Run a function that takes a bool as the first argument (typically the
// `useBatchInterface` argument) and possible additional args, and run this
// function for all the different parsers that can read from a file (stream and
// parallel parser, with all the combinations of the different tokenizers).
auto forAllParallelParsers(const auto& function, const auto&... args) {
  function(ti<RdfParallelParser<TurtleParser<Tokenizer>>>, true, args...);
  function(ti<RdfParallelParser<TurtleParser<Tokenizer>>>, false, args...);
  function(ti<RdfParallelParser<TurtleParser<TokenizerCtre>>>, true, args...);
  function(ti<RdfParallelParser<TurtleParser<TokenizerCtre>>>, false, args...);
}
auto forAllMultifileParsers(const auto& function, const auto&... args) {
  function(ti<RdfMultifileParser<Tokenizer>>, true, args...);
  function(ti<RdfMultifileParser<TokenizerCtre>>, true, args...);
}

auto forAllParsers(const auto& function, const auto&... args) {
  function(ti<RdfStreamParser<TurtleParser<Tokenizer>>>, true, args...);
  function(ti<RdfStreamParser<TurtleParser<Tokenizer>>>, false, args...);
  function(ti<RdfStreamParser<TurtleParser<TokenizerCtre>>>, true, args...);
  function(ti<RdfStreamParser<TurtleParser<TokenizerCtre>>>, false, args...);
  forAllParallelParsers(function, args...);
  forAllMultifileParsers(function, args...);
}

TEST(RdfParserTest, TurtleStreamAndParallelParser) {
  std::string filename{"turtleStreamAndParallelParserTest.dat"};
  std::vector<TurtleTriple> expectedTriples;
  {
    auto of = ad_utility::makeOfstream(filename);
    for (size_t i = 0; i < 1'000; ++i) {
      auto subject = absl::StrCat("<", i / 1000, ">");
      auto predicate = absl::StrCat("<", i / 100, ">");
      auto object = absl::StrCat("<", i / 10, ">");
      of << subject << ' ' << predicate << ' ' << object << ".\n";
      expectedTriples.emplace_back(iri(subject), iri(predicate), iri(object));
    }
  }

  auto testWithParser = [&](auto t, bool useBatchInterface) {
    using Parser = typename decltype(t)::type;
    auto result = parseFromFile<Parser>(filename, useBatchInterface);
    EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expectedTriples));
  };

  forAllParsers(testWithParser);
  forAllMultifileParsers(testWithParser);
  ad_utility::deleteFile(filename);
}

// _______________________________________________________________________
TEST(RdfParserTest, emptyInput) {
  std::string filename{"turtleParserEmptyInput.dat"};
  auto testWithParser = [&](auto t, bool useBatchInterface,
                            std::string_view input = "") {
    using Parser = typename decltype(t)::type;
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
  forAllMultifileParsers(testWithParser);
}

// ________________________________________________________________________
TEST(RdfParserTest, multilineComments) {
  std::string filename{"turtleParserMultilineComments.dat"};
  auto testWithParser = [&](auto t, bool useBatchInterface,
                            std::string_view input,
                            const auto& expectedTriples) {
    using Parser = typename decltype(t)::type;
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    auto result = parseFromFile<Parser>(filename, useBatchInterface);
    EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expectedTriples));
    ad_utility::deleteFile(filename);
  };

  // Test an input with many lines that contain only comments and whitespace.
  // There was a bug for this case in a previous version of the parser.
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
  std::vector<TurtleTriple> expected{{iri("<s>"), iri("<p>"), iri("<o>")},
                                     {iri("<s>"), iri("<p>"), iri("<o2>")}};
  forAllParsers(testWithParser, input, expected);
}

// Test that exceptions during the turtle parsing are properly propagated to the
// calling code. This is especially important for the parallel parsers where the
// actual parsing happens on background threads.
TEST(RdfParserTest, exceptionPropagation) {
  std::string filename{"turtleParserExceptionPropagation.dat"};
  auto testWithParser = [&](auto t, bool useBatchInterface,
                            std::string_view input) {
    using Parser = typename decltype(t)::type;
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
TEST(RdfParserTest, exceptionPropagationFileBufferReading) {
  std::string filename{"turtleParserExceptionPropagationFileBufferReading.dat"};
  auto testWithParser = [&](auto t, bool useBatchInterface,
                            ad_utility::MemorySize bufferSize,
                            std::string_view input) {
    using Parser = typename decltype(t)::type;
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    AD_EXPECT_THROW_WITH_MESSAGE(
        (parseFromFile<Parser>(filename, useBatchInterface, bufferSize)),
        ::testing::AllOf(
            ::testing::HasSubstr("end of a statement was not found"),
            ::testing::HasSubstr("use `--parser-buffer-size`"),
            ::testing::HasSubstr("use `--parse-parallel false`")));
    ad_utility::deleteFile(filename);
  };
  // Input, where the first triple fits into a 40_B buffer, but the second
  // one does not.
  std::string inputWithLongTriple =
      "<subject> <predicate> <object> . \n "
      "<veryLongSubject> <veryLongPredicate> "
      "<veryLongObject> .";
  forAllParallelParsers(testWithParser, 40_B, inputWithLongTriple);
}

// Test that the parallel parser's destructor can be run quickly and without
// blocking, even when there are still lots of blocks in the pipeline that are
// currently being parsed.
TEST(RdfParserTest, stopParsingOnOutsideFailure) {
#ifdef _QLEVER_NO_TIMING_TESTS
  GTEST_SKIP_("because _QLEVER_NO_TIMING_TESTS defined");
#endif
  std::string filename{"turtleParserStopParsingOnOutsideFailure.dat"};
  auto testWithParser = [&](auto t, [[maybe_unused]] bool useBatchInterface,
                            std::string_view input) {
    using Parser = typename decltype(t)::type;
    {
      auto of = ad_utility::makeOfstream(filename);
      of << input;
    }
    ad_utility::Timer timer{ad_utility::Timer::Stopped};
    {
      [[maybe_unused]] Parser parserChild = [&]() {
        if constexpr (ad_utility::isInstantiation<Parser, RdfMultifileParser>) {
          return Parser{{{filename, qlever::Filetype::Turtle, std::nullopt}},
                        40_B};
        } else {
          return Parser{filename, 40_B, 10ms};
        }
      }();
      timer.cont();
    }
    EXPECT_LE(timer.msecs(), 20ms);
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
  forAllParallelParsers(testWithParser, input);
  forAllMultifileParsers(testWithParser, input);
}

// _____________________________________________________________________________
TEST(RdfParserTest, nQuadParser) {
  auto runTestsForParser = [](auto parser) {
    parser.setInputStream(
        "<x> <y> <z> <g>. <x2> <y2> _:blank . <x2> <y2> \"literal\" _:blank2 "
        ".");
    auto triples = parser.parseAndReturnAllTriples();
    auto iri = ad_utility::testing::iri;
    auto lit = ad_utility::testing::tripleComponentLiteral;
    std::vector<TurtleTriple> expected;
    expected.emplace_back(iri("<x>"), iri("<y>"), iri("<z>"), iri("<g>"));
    auto internalGraphId =
        qlever::specialIds().at(std::string{DEFAULT_GRAPH_IRI});
    expected.emplace_back(iri("<x2>"), iri("<y2>"), "_:u_blank",
                          internalGraphId);
    expected.emplace_back(iri("<x2>"), iri("<y2>"), lit("literal"),
                          "_:u_blank2");
    EXPECT_THAT(triples, ::testing::ElementsAreArray(expected));

    auto expectParsingFails = [](const std::string& input) {
      auto parser = RdfStringParser<NQuadParser<Tokenizer>>();
      parser.setInputStream(input);
      EXPECT_ANY_THROW(parser.parseAndReturnAllTriples());
    };

    expectParsingFails("<x> <y> <z> <g>");  // missing dot after last triple
    expectParsingFails("<x> 3 <z> <g> .");  // predicate must be an iriref
    expectParsingFails("3 <x> <z> <g> .");  // predicate must be an iriref
    expectParsingFails(
        "<x> <y> '''literalIllegal''' <g> .");  // No multiline literals in
                                                // NQuad
    // format.
  };
  runTestsForParser(NQuadRe2Parser{});
  runTestsForParser(NQuadCtreParser{});
}

// _____________________________________________________________________________
TEST(RdfParserTest, noGetlineInStringParser) {
  auto runTestsForParser = [](auto parser) {
    parser.setInputStream("<x> <p> <o> .");
    TurtleTriple t;
    EXPECT_ANY_THROW(parser.getLine(t));
  };
  runTestsForParser(NQuadRe2Parser{});
  runTestsForParser(NQuadCtreParser{});
  runTestsForParser(Re2Parser{});
  runTestsForParser(CtreParser{});
}

// _____________________________________________________________________________
TEST(RdfParserTest, noGetlineInMultifileParsers) {
  auto runTestsForParser = [](auto t, [[maybe_unused]] bool interface) {
    using Parser = typename decltype(t)::type;
    Parser parser{};
    TurtleTriple triple;
    // Also test the dummy parse position member.
    EXPECT_EQ(parser.getParsePosition(), 0u);
    EXPECT_ANY_THROW(parser.getLine(triple));
  };
  forAllMultifileParsers(runTestsForParser);
}

// _____________________________________________________________________________
TEST(RdfParserTest, multifileParser) {
  auto impl = [](auto t, bool useParallelParser) {
    using Parser = typename decltype(t)::type;
    std::vector<TurtleTriple> expected;
    std::string ttl = "<x> <y> <z>. <x> <y> <z2>.";
    expected.push_back(TurtleTriple{iri("<x>"), iri("<y>"), iri("<z>"),
                                    iri("<defaultGraphTTL>")});
    expected.push_back(TurtleTriple{iri("<x>"), iri("<y>"), iri("<z2>"),
                                    iri("<defaultGraphTTL>")});
    std::string nq = "<x2> <y2> <z2> <g1>. <x3> <y3> <z3>.";
    expected.push_back(
        TurtleTriple{iri("<x2>"), iri("<y2>"), iri("<z2>"), iri("<g1>")});
    expected.push_back(TurtleTriple{iri("<x3>"), iri("<y3>"), iri("<z3>"),
                                    iri("<defaultGraphNQ>")});
    std::string file1 = "multifileParserTest1.ttl";
    std::string file2 = "multifileParserTest2.nq";
    {
      auto f = ad_utility::makeOfstream(file1);
      f << ttl;
    }
    {
      auto f = ad_utility::makeOfstream(file2);
      f << nq;
    }
    std::vector<qlever::InputFileSpecification> specs;
    specs.emplace_back(file1, qlever::Filetype::Turtle, "defaultGraphTTL",
                       useParallelParser);
    specs.emplace_back(file2, qlever::Filetype::NQuad, "defaultGraphNQ",
                       useParallelParser);
    Parser p{specs};
    std::vector<TurtleTriple> result;
    while (auto batch = p.getBatch()) {
      ql::ranges::copy(batch.value(), std::back_inserter(result));
    }
    EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
  };
  forAllMultifileParsers(impl);
}

// _____________________________________________________________________________

TEST(RdfParserTest, specialPredicateA) {
  auto runCommonTests = [](const auto& checker) {
    checker(
        "@prefix a: <http://example.org/ontology/> . a:subject a:predicate "
        "\"object\" .",
        {}, {},
        std::vector<TurtleTriple>{
            {iri("<http://example.org/ontology/subject>"),
             iri("<http://example.org/ontology/predicate>"),
             lit("\"object\"")}});
    checker(
        "@prefix a: <http://example.org/ontology/> . a:subject a \"object\" .",
        {}, {},
        std::vector<TurtleTriple>{
            {iri("<http://example.org/ontology/subject>"),
             iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
             lit("\"object\"")}});
  };

  auto parseTwoStatements = [](auto& parser) {
    return parser.statement() && parser.statement();
  };

  auto checkRe2 = checkParseResult<Re2Parser, parseTwoStatements>;
  auto checkCTRE = checkParseResult<CtreParser, parseTwoStatements>;
  runCommonTests(checkRe2);
  runCommonTests(checkCTRE);
}
