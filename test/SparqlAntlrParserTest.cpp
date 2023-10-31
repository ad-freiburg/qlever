// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
//   2022      Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gtest/gtest.h>

#include <iostream>
#include <type_traits>
#include <typeindex>
#include <utility>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "SparqlAntlrParserTestHelpers.h"
#include "engine/sparqlExpressions/LangExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RandomExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "global/Constants.h"
#include "parser/ConstructClause.h"
#include "parser/SparqlParserHelpers.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "util/AllocatorTestHelpers.h"
#include "util/SourceLocation.h"

using namespace sparqlParserHelpers;
namespace m = matchers;
using Parser = SparqlAutomaticParser;
using namespace std::literals;
using Var = Variable;

auto lit = ad_utility::testing::tripleComponentLiteral;

template <auto F>
auto parse =
    [](const string& input, SparqlQleverVisitor::PrefixMap prefixes = {},
       SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
           SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False) {
      ParserAndVisitor p{input, std::move(prefixes), disableSomeChecks};
      return p.parseTypesafe(F);
    };

auto parseBlankNode = parse<&Parser::blankNode>;
auto parseCollection = parse<&Parser::collection>;
auto parseConstructTriples = parse<&Parser::constructTriples>;
auto parseGraphNode = parse<&Parser::graphNode>;
auto parseObjectList = parse<&Parser::objectList>;
auto parsePropertyList = parse<&Parser::propertyList>;
auto parsePropertyListNotEmpty = parse<&Parser::propertyListNotEmpty>;
auto parseSelectClause = parse<&Parser::selectClause>;
auto parseTriplesSameSubject = parse<&Parser::triplesSameSubject>;
auto parseVariable = parse<&Parser::var>;
auto parseVarOrTerm = parse<&Parser::varOrTerm>;
auto parseVerb = parse<&Parser::verb>;

template <auto Clause,
          typename Value = decltype(parse<Clause>("").resultOfParse_)>
struct ExpectCompleteParse {
  SparqlQleverVisitor::PrefixMap prefixMap_ = {};
  SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False;

  auto operator()(const string& input, const Value& value,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    return operator()(input, value, prefixMap_, l);
  };

  auto operator()(const string& input,
                  const testing::Matcher<const Value&>& matcher,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    return operator()(input, matcher, prefixMap_, l);
  };

  auto operator()(const string& input, const Value& value,
                  SparqlQleverVisitor::PrefixMap prefixMap,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    return operator()(input, testing::Eq(value), std::move(prefixMap), l);
  };

  auto operator()(const string& input,
                  const testing::Matcher<const Value&>& matcher,
                  SparqlQleverVisitor::PrefixMap prefixMap,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    auto tr = generateLocationTrace(l, "successful parsing was expected here");
    EXPECT_NO_THROW({
      return expectCompleteParse(
          parse<Clause>(input, std::move(prefixMap), disableSomeChecks),
          matcher, l);
    });
  };
};

template <auto Clause>
struct ExpectParseFails {
  SparqlQleverVisitor::PrefixMap prefixMap_ = {};
  SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False;

  auto operator()(
      const string& input,
      const testing::Matcher<const std::string&>& messageMatcher = ::testing::_,
      ad_utility::source_location l = ad_utility::source_location::current()) {
    return operator()(input, prefixMap_, messageMatcher, l);
  }

  auto operator()(
      const string& input, SparqlQleverVisitor::PrefixMap prefixMap,
      const testing::Matcher<const std::string&>& messageMatcher = ::testing::_,
      ad_utility::source_location l = ad_utility::source_location::current()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE(
        parse<Clause>(input, std::move(prefixMap), disableSomeChecks),
        messageMatcher);
  }
};

// TODO: make function that creates both the complete and fails parser. and use
// them with structured binding.

auto nil = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
auto first = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
auto rest = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
auto type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::SizeIs;
using ::testing::StrEq;

TEST(SparqlParser, NumericLiterals) {
  auto expectNumericLiteral = ExpectCompleteParse<&Parser::numericLiteral>{};
  auto expectNumericLiteralFails = ExpectParseFails<&Parser::numericLiteral>{};
  expectNumericLiteral("3.0", m::NumericLiteralDouble(3.0));
  expectNumericLiteral("3.0e2", m::NumericLiteralDouble(300.0));
  expectNumericLiteral("3.0e-2", m::NumericLiteralDouble(0.030));
  expectNumericLiteral("3", m::NumericLiteralInt(3ll));
  expectNumericLiteral("-3.0", m::NumericLiteralDouble(-3.0));
  expectNumericLiteral("-3", m::NumericLiteralInt(-3ll));
  expectNumericLiteral("+3", m::NumericLiteralInt(3ll));
  expectNumericLiteral("+3.02", m::NumericLiteralDouble(3.02));
  expectNumericLiteral("+3.1234e12", m::NumericLiteralDouble(3123400000000.0));
  expectNumericLiteral(".234", m::NumericLiteralDouble(0.234));
  expectNumericLiteral("+.0123", m::NumericLiteralDouble(0.0123));
  expectNumericLiteral("-.5123", m::NumericLiteralDouble(-0.5123));
  expectNumericLiteral(".234e4", m::NumericLiteralDouble(2340.0));
  expectNumericLiteral("+.0123E-3", m::NumericLiteralDouble(0.0000123));
  expectNumericLiteral("-.5123E12", m::NumericLiteralDouble(-512300000000.0));
  expectNumericLiteralFails("1000000000000000000000000000000000000");
  expectNumericLiteralFails("-99999999999999999999");
  expectNumericLiteralFails("12E400");
  expectNumericLiteralFails("-4.2E550");
}

TEST(SparqlParser, Prefix) {
  SparqlQleverVisitor::PrefixMap prefixMap{{"wd", "<www.wikidata.org/>"}};

  {
    ParserAndVisitor p{"PREFIX wd: <www.wikidata.org/>"};
    auto defaultPrefixes = p.visitor_.prefixMap();
    ASSERT_EQ(defaultPrefixes.size(), 0);
    p.visitor_.visit(p.parser_.prefixDecl());
    auto prefixes = p.visitor_.prefixMap();
    ASSERT_EQ(prefixes.size(), 1);
    ASSERT_EQ(prefixes.at("wd"), "<www.wikidata.org/>");
  }
  expectCompleteParse(parse<&Parser::pnameLn>("wd:bimbam", prefixMap),
                      StrEq("<www.wikidata.org/bimbam>"));
  expectCompleteParse(parse<&Parser::pnameNs>("wd:", prefixMap),
                      StrEq("<www.wikidata.org/>"));
  expectCompleteParse(parse<&Parser::prefixedName>("wd:bimbam", prefixMap),
                      StrEq("<www.wikidata.org/bimbam>"));
  expectIncompleteParse(
      parse<&Parser::iriref>("<somethingsomething> <rest>", prefixMap),
      "<rest>", testing::StrEq("<somethingsomething>"));
}

TEST(SparqlExpressionParser, First) {
  string s = "(5 * 5 ) bimbam";
  // This is an example on how to access a certain parsed substring.
  /*
  LOG(INFO) << context->getText() << std::endl;
  LOG(INFO) << p.parser_.getTokenStream()
                   ->getTokenSource()
                   ->getInputStream()
                   ->toString()
            << std::endl;
  LOG(INFO) << p.parser_.getCurrentToken()->getStartIndex() << std::endl;
   */
  auto resultofParse = parse<&Parser::expression>(s);
  EXPECT_EQ(resultofParse.remainingText_.length(), 6);
  auto resultAsExpression = std::move(resultofParse.resultOfParse_);

  VariableToColumnMap map;
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::testing::makeAllocator()};
  IdTable table{alloc};
  LocalVocab localVocab;
  sparqlExpression::EvaluationContext input{*ad_utility::testing::getQec(), map,
                                            table, alloc, localVocab};
  auto result = resultAsExpression->evaluate(&input);
  AD_CONTRACT_CHECK(std::holds_alternative<Id>(result));
  ASSERT_EQ(std::get<Id>(result).getDatatype(), Datatype::Int);
  ASSERT_EQ(25, std::get<Id>(result).getInt());
}

TEST(SparqlParser, ComplexConstructTemplate) {
  string input =
      "{ [?a ( ?b (?c) )] ?d [?e [?f ?g]] . "
      "<http://wallscope.co.uk/resource/olympics/medal/#something> a "
      "<http://wallscope.co.uk/resource/olympics/medal/#somethingelse> }";

  auto Blank = [](const std::string& label) { return BlankNode(true, label); };
  expectCompleteParse(
      parse<&Parser::constructTemplate>(input),
      m::ConstructClause(
          {{Blank("0"), Var("?a"), Blank("3")},
           {Blank("1"), Iri(first), Blank("2")},
           {Blank("1"), Iri(rest), Iri(nil)},
           {Blank("2"), Iri(first), Var("?c")},
           {Blank("2"), Iri(rest), Iri(nil)},
           {Blank("3"), Iri(first), Var("?b")},
           {Blank("3"), Iri(rest), Blank("1")},
           {Blank("0"), Var("?d"), Blank("4")},
           {Blank("4"), Var("?e"), Blank("5")},
           {Blank("5"), Var("?f"), Var("?g")},
           {Iri("<http://wallscope.co.uk/resource/olympics/medal/"
                "#something>"),
            Iri(type),
            Iri("<http://wallscope.co.uk/resource/olympics/medal/"
                "#somethingelse>")}}));
}

TEST(SparqlParser, GraphTerm) {
  auto expectGraphTerm = ExpectCompleteParse<&Parser::graphTerm>{};
  expectGraphTerm("1337", m::Literal("1337"));
  expectGraphTerm("true", m::Literal("true"));
  expectGraphTerm("[]", m::BlankNode(true, "0"));
  {
    const std::string iri = "<http://dummy-iri.com#fragment>";
    expectCompleteParse(parse<&Parser::graphTerm>(iri), m::Iri(iri));
  }
  expectGraphTerm("\"abc\"", m::Literal("\"abc\""));
  expectGraphTerm("()", m::Iri(nil));
}

TEST(SparqlParser, RdfCollectionSingleVar) {
  expectCompleteParse(
      parseCollection("( ?a )"),
      Pair(m::BlankNode(true, "0"),
           ElementsAre(ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                                   m::VariableVariant("?a")),
                       ElementsAre(m::BlankNode(true, "0"), m::Iri(rest),
                                   m::Iri(nil)))));
}

TEST(SparqlParser, RdfCollectionTripleVar) {
  auto Var = m::VariableVariant;
  auto Blank = [](const std::string& label) {
    return m::BlankNode(true, label);
  };
  expectCompleteParse(
      parseCollection("( ?a ?b ?c )"),
      Pair(m::BlankNode(true, "2"),
           ElementsAre(ElementsAre(Blank("0"), m::Iri(first), Var("?c")),
                       ElementsAre(Blank("0"), m::Iri(rest), m::Iri(nil)),
                       ElementsAre(Blank("1"), m::Iri(first), Var("?b")),
                       ElementsAre(Blank("1"), m::Iri(rest), Blank("0")),
                       ElementsAre(Blank("2"), m::Iri(first), Var("?a")),
                       ElementsAre(Blank("2"), m::Iri(rest), Blank("1")))));
}

TEST(SparqlParser, BlankNodeAnonymous) {
  expectCompleteParse(parseBlankNode("[ \t\r\n]"), m::BlankNode(true, "0"));
}

TEST(SparqlParser, BlankNodeLabelled) {
  expectCompleteParse(parseBlankNode("_:label123"),
                      m::BlankNode(false, "label123"));
}

TEST(SparqlParser, ConstructTemplateEmpty) {
  expectCompleteParse(parse<&Parser::constructTemplate>("{}"),
                      testing::Eq(std::nullopt));
}

TEST(SparqlParser, ConstructTriplesSingletonWithTerminator) {
  expectCompleteParse(parseConstructTriples("?a ?b ?c ."),
                      ElementsAre(ElementsAre(m::VariableVariant("?a"),
                                              m::VariableVariant("?b"),
                                              m::VariableVariant("?c"))));
}

TEST(SparqlParser, ConstructTriplesWithTerminator) {
  auto IsVar = m::VariableVariant;
  expectCompleteParse(
      parseConstructTriples("?a ?b ?c . ?d ?e ?f . ?g ?h ?i ."),
      ElementsAre(
          ElementsAre(IsVar("?a"), IsVar("?b"), IsVar("?c")),
          ElementsAre(IsVar("?d"), IsVar("?e"), IsVar("?f")),
          ElementsAre(IsVar("?g"), IsVar("?h"), m::VariableVariant("?i"))));
}

TEST(SparqlParser, TriplesSameSubjectVarOrTerm) {
  expectCompleteParse(parseConstructTriples("?a ?b ?c"),
                      ElementsAre(ElementsAre(m::VariableVariant("?a"),
                                              m::VariableVariant("?b"),
                                              m::VariableVariant("?c"))));
}

TEST(SparqlParser, TriplesSameSubjectTriplesNodeWithPropertyList) {
  expectCompleteParse(
      parseTriplesSameSubject("(?a) ?b ?c"),
      ElementsAre(
          ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                      m::VariableVariant("?a")),
          ElementsAre(m::BlankNode(true, "0"), m::Iri(rest), m::Iri(nil)),
          ElementsAre(m::BlankNode(true, "0"), m::VariableVariant("?b"),
                      m::VariableVariant("?c"))));
}

TEST(SparqlParser, TriplesSameSubjectTriplesNodeEmptyPropertyList) {
  expectCompleteParse(
      parseTriplesSameSubject("(?a)"),
      ElementsAre(
          ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                      m::VariableVariant("?a")),
          ElementsAre(m::BlankNode(true, "0"), m::Iri(rest), m::Iri(nil))));
}

TEST(SparqlParser, PropertyList) {
  expectCompleteParse(
      parsePropertyList("a ?a"),
      Pair(ElementsAre(ElementsAre(m::Iri(type), m::VariableVariant("?a"))),
           IsEmpty()));
}

TEST(SparqlParser, EmptyPropertyList) {
  expectCompleteParse(parsePropertyList(""), Pair(IsEmpty(), IsEmpty()));
}

TEST(SparqlParser, PropertyListNotEmptySingletonWithTerminator) {
  expectCompleteParse(
      parsePropertyListNotEmpty("a ?a ;"),
      Pair(ElementsAre(ElementsAre(m::Iri(type), m::VariableVariant("?a"))),
           IsEmpty()));
}

TEST(SparqlParser, PropertyListNotEmptyWithTerminator) {
  expectCompleteParse(
      parsePropertyListNotEmpty("a ?a ; a ?b ; a ?c ;"),
      Pair(ElementsAre(ElementsAre(m::Iri(type), m::VariableVariant("?a")),
                       ElementsAre(m::Iri(type), m::VariableVariant("?b")),
                       ElementsAre(m::Iri(type), m::VariableVariant("?c"))),
           IsEmpty()));
}

TEST(SparqlParser, VerbA) { expectCompleteParse(parseVerb("a"), m::Iri(type)); }

TEST(SparqlParser, VerbVariable) {
  expectCompleteParse(parseVerb("?a"), m::VariableVariant("?a"));
}

TEST(SparqlParser, ObjectListSingleton) {
  expectCompleteParse(parseObjectList("?a"),
                      Pair(ElementsAre(m::VariableVariant("?a")), IsEmpty()));
}

TEST(SparqlParser, ObjectList) {
  expectCompleteParse(
      parseObjectList("?a , ?b , ?c"),
      Pair(ElementsAre(m::VariableVariant("?a"), m::VariableVariant("?b"),
                       m::VariableVariant("?c")),
           IsEmpty()));
}

TEST(SparqlParser, BlankNodePropertyList) {
  expectCompleteParse(
      parse<&Parser::blankNodePropertyList>("[ a ?a ; a ?b ; a ?c ]"),
      Pair(m::BlankNode(true, "0"),
           ElementsAre(ElementsAre(m::BlankNode(true, "0"), m::Iri(type),
                                   m::VariableVariant("?a")),
                       ElementsAre(m::BlankNode(true, "0"), m::Iri(type),
                                   m::VariableVariant("?b")),
                       ElementsAre(m::BlankNode(true, "0"), m::Iri(type),
                                   m::VariableVariant("?c")))));
}

TEST(SparqlParser, GraphNodeVarOrTerm) {
  expectCompleteParse(parseGraphNode("?a"),
                      Pair(m::VariableVariant("?a"), IsEmpty()));
}

TEST(SparqlParser, GraphNodeTriplesNode) {
  expectCompleteParse(
      parseGraphNode("(?a)"),
      Pair(m::BlankNode(true, "0"),
           ElementsAre(ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                                   m::VariableVariant("?a")),
                       ElementsAre(m::BlankNode(true, "0"), m::Iri(rest),
                                   m::Iri(nil)))));
}

TEST(SparqlParser, VarOrTermVariable) {
  expectCompleteParse(parseVarOrTerm("?a"), m::VariableVariant("?a"));
}

TEST(SparqlParser, VarOrTermGraphTerm) {
  expectCompleteParse(parseVarOrTerm("()"), m::Iri(nil));
}

TEST(SparqlParser, Iri) {
  auto expectIri = ExpectCompleteParse<&Parser::iri>{};
  expectIri("rdfs:label", "<http://www.w3.org/2000/01/rdf-schema#label>"s,
            {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}});
  expectIri(
      "rdfs:label", "<http://www.w3.org/2000/01/rdf-schema#label>"s,
      {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}, {"foo", "<bar#>"}});
  expectIri("<http://www.w3.org/2000/01/rdf-schema>"s,
            "<http://www.w3.org/2000/01/rdf-schema>"s,
            SparqlQleverVisitor::PrefixMap{});
  expectIri("@en@rdfs:label"s,
            "@en@<http://www.w3.org/2000/01/rdf-schema#label>"s,
            {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}});
  expectIri("@en@<http://www.w3.org/2000/01/rdf-schema>"s,
            "@en@<http://www.w3.org/2000/01/rdf-schema>"s,
            SparqlQleverVisitor::PrefixMap{});
}

TEST(SparqlParser, VarOrIriIri) {
  expectCompleteParse(parseVarOrTerm("<http://testiri>"),
                      m::Iri("<http://testiri>"));
}

TEST(SparqlParser, VariableWithQuestionMark) {
  expectCompleteParse(parseVariable("?variableName"),
                      m::Variable("?variableName"));
}

TEST(SparqlParser, VariableWithDollarSign) {
  expectCompleteParse(parseVariable("$variableName"),
                      m::Variable("?variableName"));
}

TEST(SparqlParser, Bind) {
  auto noChecks = SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::True;
  auto expectBind = ExpectCompleteParse<&Parser::bind>{{}, noChecks};
  expectBind("BIND (10 - 5 as ?a)", m::Bind(Var{"?a"}, "10 - 5"));
  expectBind("bInD (?age - 10 As ?s)", m::Bind(Var{"?s"}, "?age - 10"));
}

TEST(SparqlParser, Integer) {
  auto expectInteger = ExpectCompleteParse<&Parser::integer>{};
  auto expectIntegerFails = ExpectParseFails<&Parser::integer>();
  expectInteger("1931", 1931ull);
  expectInteger("0", 0ull);
  expectInteger("18446744073709551615", 18446744073709551615ull);
  expectIntegerFails("18446744073709551616");
  expectIntegerFails("10000000000000000000000000000000000000000");
  expectIntegerFails("-1");
}

TEST(SparqlParser, LimitOffsetClause) {
  auto expectLimitOffset = ExpectCompleteParse<&Parser::limitOffsetClauses>{};
  auto expectLimitOffsetFails = ExpectParseFails<&Parser::limitOffsetClauses>();
  expectLimitOffset("LIMIT 10", m::LimitOffset(10, TEXT_LIMIT_DEFAULT, 0));
  expectLimitOffset("OFFSET 31 LIMIT 12 TEXTLIMIT 14",
                    m::LimitOffset(12, 14, 31));
  expectLimitOffset("textlimit 999", m::LimitOffset(std::nullopt, 999, 0));
  expectLimitOffset("LIMIT      999",
                    m::LimitOffset(999, TEXT_LIMIT_DEFAULT, 0));
  expectLimitOffset("OFFSET 43",
                    m::LimitOffset(std::nullopt, TEXT_LIMIT_DEFAULT, 43));
  expectLimitOffset("TEXTLIMIT 43 LIMIT 19", m::LimitOffset(19, 43, 0));
  expectLimitOffsetFails("LIMIT20");
  expectIncompleteParse(parse<&Parser::limitOffsetClauses>(
                            "Limit 10 TEXTLIMIT 20 offset 0 Limit 20"),
                        "Limit 20", m::LimitOffset(10ull, 20ull, 0ull));
}

TEST(SparqlParser, OrderCondition) {
  auto expectOrderCondition = ExpectCompleteParse<&Parser::orderCondition>{};
  auto expectOrderConditionFails = ExpectParseFails<&Parser::orderCondition>();
  // var
  expectOrderCondition("?test",
                       m::VariableOrderKeyVariant(Var{"?test"}, false));
  // brackettedExpression
  expectOrderCondition("DESC (?foo)",
                       m::VariableOrderKeyVariant(Var{"?foo"}, true));
  expectOrderCondition("ASC (?bar)",
                       m::VariableOrderKeyVariant(Var{"?bar"}, false));
  expectOrderCondition("ASC(?test - 5)",
                       m::ExpressionOrderKey("(?test - 5)", false));
  expectOrderCondition("DESC (10 || (5 && ?foo))",
                       m::ExpressionOrderKey("(10 || (5 && ?foo))", true));
  // constraint
  expectOrderCondition("(5 - ?mehr)",
                       m::ExpressionOrderKey("(5 - ?mehr)", false));
  expectOrderCondition("SUM(?i)", m::ExpressionOrderKey("SUM(?i)", false));
  expectOrderConditionFails("ASC SCORE(?i)");
}

TEST(SparqlParser, OrderClause) {
  auto expectOrderClause = ExpectCompleteParse<&Parser::orderClause>{};
  auto expectOrderClauseFails = ExpectParseFails<&Parser::orderClause>{};
  expectOrderClause(
      "ORDER BY ?test DESC(?foo - 5)",
      m::OrderKeys({VariableOrderKey{Var{"?test"}, false},
                    m::ExpressionOrderKeyTest{"(?foo - 5)", true}}));

  expectOrderClause("INTERNAL SORT BY ?test",
                    m::OrderKeys({VariableOrderKey{Var{"?test"}, false}},
                                 IsInternalSort::True));

  expectOrderClauseFails("INTERNAL SORT BY ?test DESC(?blubb)");
}

TEST(SparqlParser, GroupCondition) {
  auto expectGroupCondition = ExpectCompleteParse<&Parser::groupCondition>{};
  // variable
  expectGroupCondition("?test", m::VariableGroupKey("?test"));
  // expression without binding
  expectGroupCondition("(?test)", m::ExpressionGroupKey("?test"));
  // expression with binding
  expectGroupCondition("(?test AS ?mehr)",
                       m::AliasGroupKey("?test", Var{"?mehr"}));
  // builtInCall
  expectGroupCondition("COUNT(?test)", m::ExpressionGroupKey("COUNT(?test)"));
  // functionCall
  expectGroupCondition(
      "<http://www.opengis.net/def/function/geosparql/latitude>(?test)",
      m::ExpressionGroupKey(
          "<http://www.opengis.net/def/function/geosparql/latitude>(?test)"));
}

TEST(SparqlParser, GroupClause) {
  expectCompleteParse(
      parse<&Parser::groupClause>(
          "GROUP BY ?test (?foo - 10 as ?bar) COUNT(?baz)"),
      m::GroupKeys(
          {Var{"?test"}, std::pair{"?foo - 10", Var{"?bar"}}, "COUNT(?baz)"}));
}

TEST(SparqlParser, SolutionModifier) {
  auto expectSolutionModifier =
      ExpectCompleteParse<&Parser::solutionModifier>{};
  auto expectIncompleteParse = [](const string& input) {
    EXPECT_FALSE(
        parse<&Parser::solutionModifier>(input).remainingText_.empty());
  };
  using VOK = VariableOrderKey;

  expectSolutionModifier("", m::SolutionModifier({}, {}, {}, {}));
  // The following are no valid solution modifiers, because ORDER BY
  // has to appear before LIMIT.
  expectIncompleteParse("GROUP BY ?var LIMIT 10 ORDER BY ?var");
  expectSolutionModifier("TEXTLIMIT 1 LIMIT 10",
                         m::SolutionModifier({}, {}, {}, {10, 1, 0}));
  expectSolutionModifier(
      "GROUP BY ?var (?b - 10) HAVING (?var != 10) ORDER BY ?var TEXTLIMIT 1 "
      "LIMIT 10 OFFSET 2",
      m::SolutionModifier({Var{"?var"}, "?b - 10"}, {{"(?var != 10)"}},
                          {VOK{Var{"?var"}, false}}, {10, 1, 2}));
  expectSolutionModifier(
      "GROUP BY ?var HAVING (?foo < ?bar) ORDER BY (5 - ?var) TEXTLIMIT 21 "
      "LIMIT 2",
      m::SolutionModifier({Var{"?var"}}, {{"(?foo < ?bar)"}},
                          {std::pair{"(5 - ?var)", false}}, {2, 21, 0}));
  expectSolutionModifier(
      "GROUP BY (?var - ?bar) ORDER BY (5 - ?var)",
      m::SolutionModifier({"?var - ?bar"}, {}, {std::pair{"(5 - ?var)", false}},
                          {}));
}

TEST(SparqlParser, DataBlock) {
  auto expectDataBlock = ExpectCompleteParse<&Parser::dataBlock>{};
  auto expectDataBlockFails = ExpectParseFails<&Parser::dataBlock>();
  expectDataBlock("?test { \"foo\" }",
                  m::Values({Var{"?test"}}, {{lit("\"foo\"")}}));
  expectDataBlock("?test { 10.0 }", m::Values({Var{"?test"}}, {{10.0}}));
  expectDataBlock("?test { UNDEF }",
                  m::Values({Var{"?test"}}, {{TripleComponent::UNDEF{}}}));
  expectDataBlock("?test { false true }",
                  m::Values({Var{"?test"}}, {{false}, {true}}));
  expectDataBlock(
      R"(?foo { "baz" "bar" })",
      m::Values({Var{"?foo"}}, {{lit("\"baz\"")}, {lit("\"bar\"")}}));
  // TODO: Is this semantics correct?
  expectDataBlock(R"(( ) { ( ) })", m::Values({}, {{}}));
  expectDataBlock(R"(( ) { })", m::Values({}, {}));
  expectDataBlockFails("?test { ( ) }");
  expectDataBlock(R"(?foo { })", m::Values({Var{"?foo"}}, {}));
  expectDataBlock(R"(( ?foo ) { })", m::Values({Var{"?foo"}}, {}));
  expectDataBlockFails(R"(( ?foo ?bar ) { (<foo>) (<bar>) })");
  expectDataBlock(R"(( ?foo ?bar ) { (<foo> <bar>) })",
                  m::Values({Var{"?foo"}, Var{"?bar"}}, {{"<foo>", "<bar>"}}));
  expectDataBlock(
      R"(( ?foo ?bar ) { (<foo> "m") ("1" <bar>) })",
      m::Values({Var{"?foo"}, Var{"?bar"}},
                {{"<foo>", lit("\"m\"")}, {lit("\"1\""), "<bar>"}}));
  expectDataBlock(
      R"(( ?foo ?bar ) { (<foo> "m") (<bar> <e>) (1 "f") })",
      m::Values(
          {Var{"?foo"}, Var{"?bar"}},
          {{"<foo>", lit("\"m\"")}, {"<bar>", "<e>"}, {1, lit("\"f\"")}}));
  // TODO<joka921/qup42> implement
  expectDataBlockFails(R"(( ) { (<foo>) })");
}

TEST(SparqlParser, InlineData) {
  auto expectInlineData = ExpectCompleteParse<&Parser::inlineData>{};
  auto expectInlineDataFails = ExpectParseFails<&Parser::inlineData>();
  expectInlineData("VALUES ?test { \"foo\" }",
                   m::InlineData({Var{"?test"}}, {{lit("\"foo\"")}}));
  // There must always be a block present for InlineData
  expectInlineDataFails("");
}

TEST(SparqlParser, propertyPaths) {
  auto expectPathOrVar = ExpectCompleteParse<&Parser::verbPathOrSimple>{};
  auto Iri = &PropertyPath::fromIri;
  auto Sequence = &PropertyPath::makeSequence;
  auto Alternative = &PropertyPath::makeAlternative;
  auto Transitive = &PropertyPath::makeTransitive;
  auto TransitiveMin = &PropertyPath::makeTransitiveMin;
  auto TransitiveMax = &PropertyPath::makeTransitiveMax;
  using PrefixMap = SparqlQleverVisitor::PrefixMap;
  // Test all the base cases.
  // "a" is a special case. It is a valid PropertyPath.
  // It is short for "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".
  expectPathOrVar("a",
                  Iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
  expectPathOrVar(
      "@en@rdfs:label", Iri("@en@<http://www.w3.org/2000/01/rdf-schema#label>"),
      PrefixMap{{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}});
  EXPECT_THROW(parse<&Parser::verbPathOrSimple>("b"), ParseException);
  expectPathOrVar("test:foo", Iri("<http://www.example.com/foo>"),
                  {{"test", "<http://www.example.com/>"}});
  expectPathOrVar("?bar", Var{"?bar"});
  expectPathOrVar(":", Iri("<http://www.example.com/>"),
                  {{"", "<http://www.example.com/>"}});
  expectPathOrVar("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                  Iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
  // Test the basic combinators / | (...) + * ?.
  expectPathOrVar("a:a / a:b",
                  Sequence({Iri("<http://www.example.com/a>"),
                            Iri("<http://www.example.com/b>")}),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("a:a | a:b",
                  Alternative({Iri("<http://www.example.com/a>"),
                               Iri("<http://www.example.com/b>")}),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("(a:a)", Iri("<http://www.example.com/a>"),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("a:a+", TransitiveMin({Iri("<http://www.example.com/a>")}, 1),
                  {{"a", "<http://www.example.com/>"}});
  {
    PropertyPath expected =
        TransitiveMax({Iri("<http://www.example.com/a>")}, 1);
    expected._can_be_null = true;
    expectPathOrVar("a:a?", expected, {{"a", "<http://www.example.com/>"}});
  }
  {
    PropertyPath expected = Transitive({Iri("<http://www.example.com/a>")});
    expected._can_be_null = true;
    expectPathOrVar("a:a*", expected, {{"a", "<http://www.example.com/>"}});
  }
  // Test a bigger example that contains everything.
  {
    PropertyPath expected = Alternative(
        {Sequence({
             Iri("<http://www.example.com/a/a>"),
             Transitive({Iri("<http://www.example.com/b/b>")}),
         }),
         Iri("<http://www.example.com/c/c>"),
         TransitiveMin(
             {Sequence({Iri("<http://www.example.com/a/a>"),
                        Iri("<http://www.example.com/b/b>"), Iri("<a/b/c>")})},
             1)});
    expected.computeCanBeNull();
    expected._can_be_null = false;
    expectPathOrVar("a:a/b:b*|c:c|(a:a/b:b/<a/b/c>)+", expected,
                    {{"a", "<http://www.example.com/a/>"},
                     {"b", "<http://www.example.com/b/>"},
                     {"c", "<http://www.example.com/c/>"}});
  }
}

TEST(SparqlParser, propertyListPathNotEmpty) {
  auto expectPropertyListPath =
      ExpectCompleteParse<&Parser::propertyListPathNotEmpty>{};
  auto expectPropertyListPathFails =
      ExpectParseFails<&Parser::propertyListPathNotEmpty>();
  auto Iri = &PropertyPath::fromIri;
  expectPropertyListPath("<bar> ?foo", {{Iri("<bar>"), Var{"?foo"}}});
  expectPropertyListPath(
      "<bar> ?foo ; <mehr> ?f",
      {{Iri("<bar>"), Var{"?foo"}}, {Iri("<mehr>"), Var{"?f"}}});
  expectPropertyListPath("<bar> ?foo , ?baz", {{Iri("<bar>"), Var{"?foo"}},
                                               {Iri("<bar>"), Var{"?baz"}}});
  // Currently unsupported by QLever
  expectPropertyListPathFails("<bar> ( ?foo ?baz )");
  expectPropertyListPathFails("<bar> [ <foo> ?bar ]");
}

TEST(SparqlParser, triplesSameSubjectPath) {
  auto expectTriples = ExpectCompleteParse<&Parser::triplesSameSubjectPath>{};
  auto PathIri = &PropertyPath::fromIri;
  expectTriples("?foo <bar> ?baz",
                {{Var{"?foo"}, PathIri("<bar>"), Var{"?baz"}}});
  expectTriples("?foo <bar> ?baz ; <mehr> ?t",
                {{Var{"?foo"}, PathIri("<bar>"), Var{"?baz"}},
                 {Var{"?foo"}, PathIri("<mehr>"), Var{"?t"}}});
  expectTriples("?foo <bar> ?baz , ?t",
                {{Var{"?foo"}, PathIri("<bar>"), Var{"?baz"}},
                 {Var{"?foo"}, PathIri("<bar>"), Var{"?t"}}});
  expectTriples("?foo <bar> ?baz , ?t ; <mehr> ?d",
                {{Var{"?foo"}, PathIri("<bar>"), Var{"?baz"}},
                 {Var{"?foo"}, PathIri("<bar>"), Var{"?t"}},
                 {Var{"?foo"}, PathIri("<mehr>"), Var{"?d"}}});
  expectTriples("?foo <bar> ?baz ; <mehr> ?t , ?d",
                {{Var{"?foo"}, PathIri("<bar>"), Var{"?baz"}},
                 {Var{"?foo"}, PathIri("<mehr>"), Var{"?t"}},
                 {Var{"?foo"}, PathIri("<mehr>"), Var{"?d"}}});
  expectTriples("<foo> <bar> ?baz ; ?mehr \"a\"",
                {{Iri("<foo>"), PathIri("<bar>"), Var{"?baz"}},
                 {Iri("<foo>"), Var("?mehr"), Literal("\"a\"")}});
  expectTriples("_:1 <bar> ?baz",
                {{BlankNode(false, "1"), PathIri("<bar>"), Var{"?baz"}}});
  expectTriples("10.0 <bar> true",
                {{Literal(10.0), PathIri("<bar>"), Literal(true)}});
  expectTriples(
      "<foo> <QLever-internal-function/contains-word> \"Berlin Freiburg\"",
      {{Iri("<foo>"), PathIri("<QLever-internal-function/contains-word>"),
        Literal("\"Berlin Freiburg\"")}});
}

TEST(SparqlParser, SelectClause) {
  auto expectSelectClause = ExpectCompleteParse<&Parser::selectClause>{};
  auto expectSelectFails = ExpectParseFails<&Parser::selectClause>();

  using Alias = std::pair<string, ::Variable>;
  expectCompleteParse(parseSelectClause("SELECT *"),
                      m::AsteriskSelect(false, false));
  expectCompleteParse(parseSelectClause("SELECT DISTINCT *"),
                      m::AsteriskSelect(true, false));
  expectCompleteParse(parseSelectClause("SELECT REDUCED *"),
                      m::AsteriskSelect(false, true));
  expectSelectFails("SELECT DISTINCT REDUCED *");
  expectSelectFails("SELECT");
  expectSelectClause("SELECT ?foo", m::VariablesSelect({"?foo"}));
  expectSelectClause("SELECT ?foo ?baz ?bar",
                     m::VariablesSelect({"?foo", "?baz", "?bar"}));
  expectSelectClause("SELECT DISTINCT ?foo ?bar",
                     m::VariablesSelect({"?foo", "?bar"}, true, false));
  expectSelectClause("SELECT REDUCED ?foo ?bar ?baz",
                     m::VariablesSelect({"?foo", "?bar", "?baz"}, false, true));
  expectSelectClause("SELECT (10 as ?foo) ?bar",
                     m::Select({Alias{"10", Var{"?foo"}}, Var{"?bar"}}));
  expectSelectClause("SELECT DISTINCT (5 - 10 as ?m)",
                     m::Select({Alias{"5 - 10", Var{"?m"}}}, true, false));
  expectSelectClause(
      "SELECT (5 - 10 as ?m) ?foo (10 as ?bar)",
      m::Select({Alias{"5 - 10", "?m"}, Var{"?foo"}, Alias{"10", "?bar"}}));
}

TEST(SparqlParser, HavingCondition) {
  auto expectHavingCondition = ExpectCompleteParse<&Parser::havingCondition>{};
  auto expectHavingConditionFails =
      ExpectParseFails<&Parser::havingCondition>();

  expectHavingCondition("(?x <= 42.3)", m::stringMatchesFilter("(?x <= 42.3)"));
  expectHavingCondition("(?height > 1.7)",
                        m::stringMatchesFilter("(?height > 1.7)"));
  expectHavingCondition("(?predicate < \"<Z\")",
                        m::stringMatchesFilter("(?predicate < \"<Z\")"));
  expectHavingConditionFails("(LANG(?x) = \"en\")");
}

TEST(SparqlParser, GroupGraphPattern) {
  auto expectGraphPattern = ExpectCompleteParse<&Parser::groupGraphPattern>{
      {{INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI}}};
  auto expectGroupGraphPatternFails =
      ExpectParseFails<&Parser::groupGraphPattern>{{}};
  auto DummyTriplesMatcher = m::Triples({{Var{"?x"}, "?y", Var{"?z"}}});

  // Empty GraphPatterns are not supported.
  expectGroupGraphPatternFails("{ }");
  expectGroupGraphPatternFails("{ SELECT *  WHERE { } }");

  SparqlTriple abc{Var{"?a"}, "?b", Var{"?c"}};
  SparqlTriple def{Var{"?d"}, "?e", Var{"?f"}};
  // Test the Components alone.
  expectGraphPattern("{ { ?a ?b ?c } }",
                     m::GraphPattern(m::GroupGraphPattern(m::Triples({abc}))));
  expectGraphPattern(
      "{ { ?a ?b ?c } UNION { ?d ?e ?f } }",
      m::GraphPattern(m::Union(m::GraphPattern(m::Triples({abc})),
                               m::GraphPattern(m::Triples({def})))));
  expectGraphPattern(
      "{ { ?a ?b ?c } UNION { ?d ?e ?f } UNION { ?g ?h ?i } }",
      m::GraphPattern(m::Union(
          m::GraphPattern(m::Union(m::GraphPattern(m::Triples({abc})),
                                   m::GraphPattern(m::Triples({def})))),
          m::GraphPattern(m::Triples({{Var{"?g"}, "?h", Var{"?i"}}})))));
  expectGraphPattern("{ OPTIONAL { ?a <foo> <bar> } }",
                     m::GraphPattern(m::OptionalGraphPattern(
                         m::Triples({{Var{"?a"}, "<foo>", "<bar>"}}))));
  expectGraphPattern("{ MINUS { ?a <foo> <bar> } }",
                     m::GraphPattern(m::MinusGraphPattern(
                         m::Triples({{Var{"?a"}, "<foo>", "<bar>"}}))));
  expectGraphPattern(
      "{ FILTER (?a = 10) . ?x ?y ?z }",
      m::GraphPattern(false, {"(?a = 10)"}, DummyTriplesMatcher));
  expectGraphPattern("{ BIND (3 as ?c) }",
                     m::GraphPattern(m::Bind(Var{"?c"}, "3")));
  // The variables `?f` and `?b` have not been used before the BIND clause.
  expectGroupGraphPatternFails("{ BIND (?f - ?b as ?c) }");
  expectGraphPattern(
      "{ VALUES (?a ?b) { (<foo> <bar>) (<a> <b>) } }",
      m::GraphPattern(m::InlineData({Var{"?a"}, Var{"?b"}},
                                    {{"<foo>", "<bar>"}, {"<a>", "<b>"}})));
  expectGraphPattern("{ ?x ?y ?z }", m::GraphPattern(DummyTriplesMatcher));
  expectGraphPattern(
      "{ SELECT *  WHERE { ?x ?y ?z } }",
      m::GraphPattern(m::SubSelect(m::AsteriskSelect(false, false),
                                   m::GraphPattern(DummyTriplesMatcher))));
  // Test mixes of the components to make sure that they interact correctly.
  expectGraphPattern("{ ?x ?y ?z ; ?f <bar> }",
                     m::GraphPattern(m::Triples({{Var{"?x"}, "?y", Var{"?z"}},
                                                 {Var{"?x"}, "?f", "<bar>"}})));
  expectGraphPattern("{ ?x ?y ?z . <foo> ?f <bar> }",
                     m::GraphPattern(m::Triples({{Var{"?x"}, "?y", Var{"?z"}},
                                                 {"<foo>", "?f", "<bar>"}})));
  expectGraphPattern(
      "{ ?x <is-a> <Actor> . FILTER(?x != ?y) . ?y <is-a> <Actor> . "
      "FILTER(?y < ?x) }",
      m::GraphPattern(false, {"(?x != ?y)", "(?y < ?x)"},
                      m::Triples({{Var{"?x"}, "<is-a>", "<Actor>"},
                                  {Var{"?y"}, "<is-a>", "<Actor>"}})));
  expectGraphPattern(
      "{?x <is-a> <Actor> . FILTER(?x != ?y) . ?y <is-a> <Actor> . ?c "
      "ql:contains-entity ?x . ?c ql:contains-word \"coca* abuse\"}",
      m::GraphPattern(
          false, {"(?x != ?y)"},
          m::Triples(
              {{Var{"?x"}, "<is-a>", "<Actor>"},
               {Var{"?y"}, "<is-a>", "<Actor>"},
               {Var{"?c"}, CONTAINS_ENTITY_PREDICATE, Var{"?x"}},
               {Var{"?c"}, CONTAINS_WORD_PREDICATE, lit("\"coca* abuse\"")}})));

  // Scoping of variables in combination with a BIND clause.
  expectGraphPattern(
      "{?x <is-a> <Actor> . BIND(10 - ?x as ?y) }",
      m::GraphPattern(m::Triples({{Var{"?x"}, "<is-a>", "<Actor>"}}),
                      m::Bind(Var{"?y"}, "10 - ?x")));
  expectGraphPattern(
      "{?x <is-a> <Actor> . BIND(10 - ?x as ?y) . ?a ?b ?c }",
      m::GraphPattern(m::Triples({{Var{"?x"}, "<is-a>", "<Actor>"}}),
                      m::Bind(Var{"?y"}, "10 - ?x"),
                      m::Triples({{Var{"?a"}, "?b", Var{"?c"}}})));
  expectGroupGraphPatternFails("{?x <is-a> <Actor> . {BIND(10 - ?x as ?y)}}");
  expectGroupGraphPatternFails("{?x <is-a> <Actor> . BIND(3 as ?x)}");
  expectGraphPattern(
      "{?x <is-a> <Actor> . {BIND(3 as ?x)} }",
      m::GraphPattern(m::Triples({{Var{"?x"}, "<is-a>", "<Actor>"}}),
                      m::GroupGraphPattern(m::Bind(Var{"?x"}, "3"))));
  expectGroupGraphPatternFails(
      "{?x <is-a> <Actor> . OPTIONAL {BIND(?x as ?y)}}");

  expectGraphPattern(
      "{?x <is-a> <Actor> . OPTIONAL {BIND(3 as ?x)} }",
      m::GraphPattern(m::Triples({{Var{"?x"}, "<is-a>", "<Actor>"}}),
                      m::OptionalGraphPattern(m::Bind(Var{"?x"}, "3"))));
  expectGraphPattern(
      "{ {?x <is-a> <Actor>} UNION { BIND (3 as ?x)}}",
      m::GraphPattern(m::Union(
          m::GraphPattern(m::Triples({{Var{"?x"}, "<is-a>", "<Actor>"}})),
          m::GraphPattern(m::Bind(Var{"?x"}, "3")))));

  expectGraphPattern(
      "{?x <is-a> <Actor> . OPTIONAL { ?x <foo> <bar> } }",
      m::GraphPattern(m::Triples({{Var{"?x"}, "<is-a>", "<Actor>"}}),
                      m::OptionalGraphPattern(
                          m::Triples({{Var{"?x"}, "<foo>", "<bar>"}}))));
  expectGraphPattern(
      "{ SELECT *  WHERE { ?x ?y ?z } VALUES ?a { <a> <b> } }",
      m::GraphPattern(m::SubSelect(m::AsteriskSelect(false, false),
                                   m::GraphPattern(DummyTriplesMatcher)),
                      m::InlineData({Var{"?a"}}, {{"<a>"}, {"<b>"}})));
  expectGraphPattern("{ SERVICE <endpoint> { ?s ?p ?o } }",
                     m::GraphPattern(m::Service(
                         Iri{"<endpoint>"}, {Var{"?s"}, Var{"?p"}, Var{"?o"}},
                         "{ ?s ?p ?o }")));
  expectGraphPattern(
      "{ SERVICE <ep> { { SELECT ?s ?o WHERE { ?s ?p ?o } } } }",
      m::GraphPattern(m::Service(Iri{"<ep>"}, {Var{"?s"}, Var{"?o"}},
                                 "{ { SELECT ?s ?o WHERE { ?s ?p ?o } } }")));

  // SERVICE with SILENT or a variable endpoint is not yet supported.
  expectGroupGraphPatternFails("{ SERVICE SILENT <ep> { ?s ?p ?o } }");
  expectGroupGraphPatternFails("{ SERVICE ?endpoint { ?s ?p ?o } }");

  // graphGraphPattern is not supported.
  expectGroupGraphPatternFails("{ GRAPH ?a { } }");
  expectGroupGraphPatternFails("{ GRAPH <foo> { } }");
}

TEST(SparqlParser, RDFLiteral) {
  auto expectRDFLiteral = ExpectCompleteParse<&Parser::rdfLiteral>{
      {{"xsd", "<http://www.w3.org/2001/XMLSchema#>"}}};
  auto expectRDFLiteralFails = ExpectParseFails<&Parser::rdfLiteral>();

  expectRDFLiteral("   \"Astronaut\"^^xsd:string  \t",
                   "\"Astronaut\"^^<http://www.w3.org/2001/XMLSchema#string>"s);
  // The conversion to the internal date format
  // (":v:date:0000000000000001950-01-01T00:00:00") is done by
  // TurtleStringParser<TokenizerCtre>::parseTripleObject(resultAsString) which
  // is only called at triplesBlock.
  expectRDFLiteral(
      "\"1950-01-01T00:00:00\"^^xsd:dateTime",
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s);
  expectRDFLiteralFails(R"(?a ?b "The \"Moon\""@en .)");
}

TEST(SparqlParser, SelectQuery) {
  auto contains = [](const std::string& s) { return ::testing::HasSubstr(s); };
  auto expectSelectQuery = ExpectCompleteParse<&Parser::selectQuery>{
      {{INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI}}};
  auto expectSelectQueryFails = ExpectParseFails<&Parser::selectQuery>{};
  auto DummyGraphPatternMatcher =
      m::GraphPattern(m::Triples({{Var{"?x"}, "?y", Var{"?z"}}}));
  expectSelectQuery(
      "SELECT * WHERE { ?a <bar> ?foo }",
      testing::AllOf(m::SelectQuery(
          m::AsteriskSelect(),
          m::GraphPattern(m::Triples({{Var{"?a"}, "<bar>", Var{"?foo"}}})))));
  expectSelectQuery("SELECT * WHERE { ?x ?y ?z }",
                    testing::AllOf(m::SelectQuery(m::AsteriskSelect(),
                                                  DummyGraphPatternMatcher)));
  expectSelectQuery(
      "SELECT ?x WHERE { ?x ?y ?z . FILTER(?x != <foo>) } LIMIT 10 TEXTLIMIT 5",
      testing::AllOf(
          m::SelectQuery(
              m::Select({Var{"?x"}}),
              m::GraphPattern(false, {"(?x != <foo>)"},
                              m::Triples({{Var{"?x"}, "?y", Var{"?z"}}}))),
          m::pq::LimitOffset({10, 5})));

  // ORDER BY
  expectSelectQuery("SELECT ?x WHERE { ?x ?y ?z } ORDER BY ?y ",
                    testing::AllOf(m::SelectQuery(m::Select({Var{"?x"}}),
                                                  DummyGraphPatternMatcher),
                                   m::pq::OrderKeys({{Var{"?y"}, false}})));

  // Ordering by a variable or expression which contains a variable that is not
  // visible in the query body is not allowed.
  expectSelectQueryFails("SELECT ?a WHERE { ?a ?b ?c } ORDER BY ?x",
                         contains("Variable ?x was used by "
                                  "ORDER BY, but is not"));
  expectSelectQueryFails("SELECT ?a WHERE { ?a ?b ?c } ORDER BY (?x - 10)");

  // Explicit GROUP BY
  expectSelectQuery("SELECT ?x WHERE { ?x ?y ?z } GROUP BY ?x",
                    testing::AllOf(m::SelectQuery(m::VariablesSelect({"?x"}),
                                                  DummyGraphPatternMatcher),
                                   m::pq::GroupKeys({Var{"?x"}})));
  expectSelectQuery(
      "SELECT (COUNT(?y) as ?a) WHERE { ?x ?y ?z } GROUP BY ?x",
      testing::AllOf(
          m::SelectQuery(m::Select({std::pair{"COUNT(?y)", Var{"?a"}}}),
                         DummyGraphPatternMatcher),
          m::pq::GroupKeys({Var{"?x"}})));

  expectSelectQuery(
      "SELECT (SUM(?x) as ?a) (COUNT(?y) + ?z AS ?b)  WHERE { ?x ?y ?z } GROUP "
      "BY ?z",
      testing::AllOf(
          m::SelectQuery(m::Select({std::pair{"SUM(?x)", Var{"?a"}},
                                    std::pair{"COUNT(?y) + ?z", Var{"?b"}}}),
                         DummyGraphPatternMatcher)));

  expectSelectQuery(
      "SELECT (SUM(?x) as ?a)  WHERE { ?x ?y ?z } GROUP "
      "BY ?z ORDER BY (COUNT(?y) + ?z)",
      testing::AllOf(
          m::SelectQuery(
              m::Select({std::pair{"SUM(?x)", Var{"?a"}}}, false, false,
                        {std::pair{"(COUNT(?y) + ?z)",
                                   Var{"?_QLever_internal_variable_0"}}}),
              DummyGraphPatternMatcher),
          m::pq::OrderKeys({{Var{"?_QLever_internal_variable_0"}, false}})));

  // It is also illegal to reuse a variable from the body of a query with a
  // GROUP BY as the target of an alias, even if it is the aggregated variable
  // itself.
  expectSelectQueryFails(
      "SELECT (SUM(?y) AS ?y) WHERE { ?x <is-a> ?y } GROUP BY ?x");

  // Grouping by a variable or expression which contains a variable
  // that is not visible in the query body is not allowed.
  expectSelectQueryFails("SELECT ?x WHERE { ?a ?b ?c } GROUP BY ?x");
  expectSelectQueryFails(
      "SELECT (COUNT(?a) as ?d) WHERE { ?a ?b ?c } GROUP BY (?x - 10)");

  // All variables used in an aggregate must be visible in the query body.
  expectSelectQueryFails(
      "SELECT (COUNT(?x) as ?y) WHERE { ?a ?b ?c } GROUP BY ?a");
  // `SELECT *` is not allowed while grouping.
  expectSelectQueryFails("SELECT * WHERE { ?x ?y ?z } GROUP BY ?x");
  // When grouping selected variables must either be grouped by or aggregated.
  // `?y` is neither.
  expectSelectQueryFails("SELECT (?y as ?a) WHERE { ?x ?y ?z } GROUP BY ?x");

  // Explicit GROUP BY but the target of an alias is used twice.
  expectSelectQueryFails(
      "SELECT (?x AS ?z) (?x AS ?z) WHERE { ?x <p> ?y} GROUP BY ?x");

  // Explicit GROUP BY but the second alias uses the target of the first alias
  // as input.
  expectSelectQuery(
      "SELECT (?x AS ?a) (?a AS ?aa) WHERE { ?x ?y ?z} GROUP BY ?x",
      testing::AllOf(m::SelectQuery(m::Select({std::pair{"?x", Var{"?a"}},
                                               std::pair{"?a", Var{"?aa"}}}),
                                    DummyGraphPatternMatcher),
                     m::pq::GroupKeys({Var{"?x"}})));

  // Implicit GROUP BY.
  expectSelectQuery(
      "SELECT (SUM(?x) as ?a) (COUNT(?y) + AVG(?z) AS ?b)  WHERE { ?x ?y ?z }",
      testing::AllOf(m::SelectQuery(m::Select({std::pair{"SUM(?x)", Var{"?a"}},
                                               std::pair{"COUNT(?y) + AVG(?z)",
                                                         Var{"?b"}}}),
                                    DummyGraphPatternMatcher),
                     m::pq::GroupKeys({})));
  // Implicit GROUP BY but the variable `?x` is not aggregated.
  expectSelectQueryFails("SELECT ?x (SUM(?y) AS ?z) WHERE { ?x <p> ?y}");
  // Implicit GROUP BY but the variable `?x` is not aggregated inside the
  // expression that also contains the aggregate.
  expectSelectQueryFails("SELECT (?x + SUM(?y) AS ?z) WHERE { ?x <p> ?y}");

  // When there is no GROUP BY (implicit or explicit), the aliases are
  // equivalently transformed into BINDs and then deleted from the SELECT
  // clause.
  expectSelectQuery("SELECT (?x AS ?y) (?y AS ?z) WHERE { BIND(1 AS ?x)}",
                    m::SelectQuery(m::Select({Var("?y"), Var("?z")}),
                                   m::GraphPattern(m::Bind(Var("?x"), "1"),
                                                   m::Bind(Var("?y"), "?x"),
                                                   m::Bind(Var{"?z"}, "?y"))));

  // No GROUP BY but the target of an alias is used twice.
  expectSelectQueryFails("SELECT (?x AS ?z) (?x AS ?z) WHERE { ?x <p> ?y}",
                         contains("The target ?z of an AS clause was already "
                                  "used before in the SELECT clause."));

  // `?x` is selected twice. Once as variable and once as the result of an
  // alias. This is not allowed.
  expectSelectQueryFails(
      "SELECT ?x (?y as ?x) WHERE { ?x ?y ?z }",
      contains(
          "The target ?x of an AS clause was already used in the query body."));

  // HAVING is not allowed without GROUP BY
  expectSelectQueryFails(
      "SELECT ?x WHERE { ?x ?y ?z } HAVING (?x < 3)",
      contains("HAVING clause is only supported in queries with GROUP BY"));

  // The target of the alias (`?y`) is already bound in the WHERE clause. This
  // is forbidden by the SPARQL standard.
  expectSelectQueryFails(
      "SELECT (?x AS ?y) WHERE { ?x <is-a> ?y }",
      contains(
          "The target ?y of an AS clause was already used in the query body."));

  // Datasets are not supported.
  expectSelectQueryFails("SELECT * FROM <defaultDataset> WHERE { ?x ?y ?z }",
                         contains("FROM clauses are currently not supported"));
}

TEST(SparqlParser, ConstructQuery) {
  auto contains = [](const std::string& s) { return ::testing::HasSubstr(s); };
  auto expectConstructQuery = ExpectCompleteParse<&Parser::constructQuery>{
      {{INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI}}};
  auto expectConstructQueryFails = ExpectParseFails<&Parser::constructQuery>{};
  expectConstructQuery(
      "CONSTRUCT { } WHERE { ?a ?b ?c }",
      m::ConstructQuery(
          {}, m::GraphPattern(m::Triples({{Var{"?a"}, "?b", Var{"?c"}}}))));
  expectConstructQuery(
      "CONSTRUCT { ?a <foo> ?c . } WHERE { ?a ?b ?c }",
      testing::AllOf(m::ConstructQuery(
          {{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}}},
          m::GraphPattern(m::Triples({{Var{"?a"}, "?b", Var{"?c"}}})))));
  expectConstructQuery(
      "CONSTRUCT { ?a <foo> ?c . <bar> ?b <baz> } WHERE { ?a ?b ?c . FILTER(?a "
      "> 0) .}",
      m::ConstructQuery(
          {{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}},
           {Iri{"<bar>"}, Var{"?b"}, Iri{"<baz>"}}},
          m::GraphPattern(false, {"(?a > 0)"},
                          m::Triples({{Var{"?a"}, "?b", Var{"?c"}}}))));
  expectConstructQuery(
      "CONSTRUCT { ?a <foo> ?c . } WHERE { ?a ?b ?c } ORDER BY ?a LIMIT 10",
      testing::AllOf(
          m::ConstructQuery(
              {{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}}},
              m::GraphPattern(m::Triples({{Var{"?a"}, "?b", Var{"?c"}}}))),
          m::pq::LimitOffset({10}), m::pq::OrderKeys({{Var{"?a"}, false}})));
  // This case of the grammar is not useful without Datasets, but we still
  // support it.
  expectConstructQuery("CONSTRUCT WHERE { ?a <foo> ?b }",
                       m::ConstructQuery({{Var{"?a"}, Iri{"<foo>"}, Var{"?b"}}},
                                         m::GraphPattern()));
  // Datasets are not supported.
  expectConstructQueryFails(
      "CONSTRUCT { } FROM <foo> WHERE { ?a ?b ?c }",
      contains("FROM clauses are currently not supported by QLever."));
  expectConstructQueryFails(
      "CONSTRUCT FROM <foo> WHERE { }",
      contains("FROM clauses are currently not supported by QLever."));

  // GROUP BY and ORDER BY, but the ordered variable is not grouped
  expectConstructQueryFails(
      "CONSTRUCT {?a <b> <c> } WHERE { ?a ?b ?c } GROUP BY ?a ORDER BY ?b",
      contains("Variable ?b was used in an ORDER BY clause, but is neither "
               "grouped nor created as an alias in the SELECT clause"));
}

TEST(SparqlParser, Query) {
  auto expectQuery = ExpectCompleteParse<&Parser::query>{
      {{INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI}}};
  auto expectQueryFails = ExpectParseFails<&Parser::query>{};
  // Test that `_originalString` is correctly set.
  expectQuery(
      "SELECT * WHERE { ?a <bar> ?foo }",
      testing::AllOf(m::SelectQuery(m::AsteriskSelect(),
                                    m::GraphPattern(m::Triples(
                                        {{Var{"?a"}, "<bar>", Var{"?foo"}}}))),
                     m::pq::OriginalString("SELECT * WHERE { ?a <bar> ?foo }"),
                     m::VisibleVariables({Var{"?a"}, Var{"?foo"}})));
  expectQuery("SELECT * WHERE { ?x ?y ?z }",
              m::pq::OriginalString("SELECT * WHERE { ?x ?y ?z }"));
  expectQuery(
      "SELECT ?x WHERE { ?x ?y ?z } GROUP BY ?x",
      m::pq::OriginalString("SELECT ?x WHERE { ?x ?y ?z } GROUP BY ?x"));
  expectQuery(
      "PREFIX a: <foo> SELECT (COUNT(?y) as ?a) WHERE { ?x ?y ?z } GROUP BY ?x",
      m::pq::OriginalString("PREFIX a: <foo> SELECT (COUNT(?y) as ?a) WHERE { "
                            "?x ?y ?z } GROUP BY ?x"));
  expectQuery(
      "CONSTRUCT { ?a <foo> ?c . } WHERE { ?a ?b ?c }",
      testing::AllOf(m::ConstructQuery({{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}}},
                                       m::GraphPattern(m::Triples(
                                           {{Var{"?a"}, "?b", Var{"?c"}}}))),
                     m::VisibleVariables({Var{"?a"}, Var{"?b"}, Var{"?c"}})));
  expectQuery(
      "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } LIMIT 10",
      testing::AllOf(
          m::ConstructQuery(
              {{Var{"?x"}, Iri{"<foo>"}, Iri{"<bar>"}}},
              m::GraphPattern(m::Triples({{Var{"?x"}, "?y", Var{"?z"}}}))),
          m::pq::OriginalString(
              "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } LIMIT 10"),
          m::pq::LimitOffset({10}),
          m::VisibleVariables({Var{"?x"}, Var{"?y"}, Var{"?z"}})));

  // Construct query with GROUP BY
  expectQuery(
      "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } GROUP BY ?x",
      testing::AllOf(
          m::ConstructQuery(
              {{Var{"?x"}, Iri{"<foo>"}, Iri{"<bar>"}}},
              m::GraphPattern(m::Triples({{Var{"?x"}, "?y", Var{"?z"}}}))),
          m::pq::OriginalString(
              "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } GROUP BY ?x"),
          m::VisibleVariables({Var{"?x"}, Var{"?y"}, Var{"?z"}})));
  // Construct query with GROUP BY, but a variable that is not grouped is used.
  expectQueryFails(
      "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } GROUP BY ?y");

  // Test that the prologue is parsed properly. We use `m::Service` here
  // because the parsing of a SERVICE clause is the only place where the
  // prologue is explicitly passed on to a `parsedQuery::` object.
  expectQuery(
      "PREFIX doof: <http://doof.org/> "
      "SELECT * WHERE { SERVICE <endpoint> { ?s ?p ?o } }",
      m::SelectQuery(m::AsteriskSelect(),
                     m::GraphPattern(m::Service(
                         Iri{"<endpoint>"}, {Var{"?s"}, Var{"?p"}, Var{"?o"}},
                         "{ ?s ?p ?o }", "PREFIX doof: <http://doof.org/>"))));

  // Describe and Ask Queries are not supported.
  expectQueryFails("DESCRIBE *");
  expectQueryFails("ASK WHERE { ?x <foo> <bar> }");
}

// Some helper matchers for the `builtInCall` test below.
namespace builtInCallTestHelpers {
using namespace sparqlExpression;
// Return a matcher that checks whether a given `SparqlExpression::Ptr` actually
// (via `dynamic_cast`) points to an object of type `Expression`, and that this
// `Expression` matches the `matcher`.
template <typename Expression, typename Matcher = decltype(testing::_)>
auto matchPtr(Matcher matcher = Matcher{})
    -> ::testing::Matcher<const sparqlExpression::SparqlExpression::Ptr&> {
  return testing::Pointee(
      testing::WhenDynamicCastTo<const Expression&>(matcher));
}

// Return a matcher  that checks whether a given `SparqlExpression::Ptr` points
// (via `dynamic_cast`) to an object of the same type that a call to the
// `makeFunction` yields. The matcher also checks that the expression's children
// match the `childrenMatchers`.
auto matchNaryWithChildrenMatchers(auto makeFunction,
                                   auto&&... childrenMatchers)
    -> ::testing::Matcher<const sparqlExpression::SparqlExpression::Ptr&> {
  using namespace sparqlExpression;
  auto typeIdLambda = [](const auto& ptr) {
    return std::type_index{typeid(*ptr)};
  };

  [[maybe_unused]] auto makeDummyChild = [](auto&&) -> SparqlExpression::Ptr {
    return std::make_unique<VariableExpression>(Variable{"?x"});
  };
  auto expectedTypeIndex =
      typeIdLambda(makeFunction(makeDummyChild(childrenMatchers)...));
  ::testing::Matcher<const SparqlExpression::Ptr&> typeIdMatcher =
      ::testing::ResultOf(typeIdLambda, ::testing::Eq(expectedTypeIndex));
  return ::testing::AllOf(typeIdMatcher,
                          ::testing::Pointee(AD_PROPERTY(
                              SparqlExpression, childrenForTesting,
                              ::testing::ElementsAre(childrenMatchers...))));
}

auto variableExpressionMatcher = [](const Variable& var) {
  return matchPtr<VariableExpression>(
      AD_PROPERTY(VariableExpression, value, testing::Eq(var)));
};

auto idExpressionMatcher = [](Id id) {
  return matchPtr<IdExpression>(
      AD_PROPERTY(IdExpression, value, testing::Eq(id)));
};

// Return a matcher  that checks whether a given `SparqlExpression::Ptr` points
// (via `dynamic_cast`) to an object of the same type that a call to the
// `makeFunction` yields. The matcher also checks that the expression's children
// are the `variables`.
auto matchNary(auto makeFunction,
               ad_utility::SimilarTo<Variable> auto&&... variables)
    -> ::testing::Matcher<const sparqlExpression::SparqlExpression::Ptr&> {
  using namespace sparqlExpression;
  return matchNaryWithChildrenMatchers(makeFunction,
                                       variableExpressionMatcher(variables)...);
}
auto matchUnary(auto makeFunction)
    -> ::testing::Matcher<const sparqlExpression::SparqlExpression::Ptr&> {
  return matchNary(makeFunction, Variable{"?x"});
}
}  // namespace builtInCallTestHelpers

// ___________________________________________________________________________
TEST(SparqlParser, builtInCall) {
  using namespace sparqlExpression;
  using namespace builtInCallTestHelpers;
  auto expectBuiltInCall = ExpectCompleteParse<&Parser::builtInCall>{};
  auto expectFails = ExpectParseFails<&Parser::builtInCall>{};
  expectBuiltInCall("StrLEN(?x)", matchUnary(&makeStrlenExpression));
  expectBuiltInCall("ucaSe(?x)", matchUnary(&makeUppercaseExpression));
  expectBuiltInCall("lCase(?x)", matchUnary(&makeLowercaseExpression));
  expectBuiltInCall("StR(?x)", matchUnary(&makeStrExpression));
  expectBuiltInCall("year(?x)", matchUnary(&makeYearExpression));
  expectBuiltInCall("month(?x)", matchUnary(&makeMonthExpression));
  expectBuiltInCall("day(?x)", matchUnary(&makeDayExpression));
  expectBuiltInCall("hours(?x)", matchUnary(&makeHoursExpression));
  expectBuiltInCall("minutes(?x)", matchUnary(&makeMinutesExpression));
  expectBuiltInCall("seconds(?x)", matchUnary(&makeSecondsExpression));
  expectBuiltInCall("abs(?x)", matchUnary(&makeAbsExpression));
  expectBuiltInCall("ceil(?x)", matchUnary(&makeCeilExpression));
  expectBuiltInCall("floor(?x)", matchUnary(&makeFloorExpression));
  expectBuiltInCall("round(?x)", matchUnary(&makeRoundExpression));
  expectBuiltInCall("RAND()", matchPtr<RandomExpression>());
  expectBuiltInCall("COALESCE(?x)", matchUnary(makeCoalesceExpressionVariadic));
  expectBuiltInCall("COALESCE()", matchNary(makeCoalesceExpressionVariadic));
  expectBuiltInCall("COALESCE(?x, ?y, ?z)",
                    matchNary(makeCoalesceExpressionVariadic, Var{"?x"},
                              Var{"?y"}, Var{"?z"}));
  expectBuiltInCall("CONCAT(?x)", matchUnary(makeConcatExpressionVariadic));
  expectBuiltInCall("concaT()", matchNary(makeConcatExpressionVariadic));
  expectBuiltInCall(
      "concat(?x, ?y, ?z)",
      matchNary(makeConcatExpressionVariadic, Var{"?x"}, Var{"?y"}, Var{"?z"}));

  expectBuiltInCall(
      "replace(?x, ?y, ?z)",
      matchNary(&makeReplaceExpression, Var{"?x"}, Var{"?y"}, Var{"?z"}));
  expectFails("replace(?x, ?y, ?z, \"i\")",
              ::testing::AllOf(::testing::ContainsRegex("regex"),
                               ::testing::ContainsRegex("flags")));
  expectBuiltInCall("IF(?a, ?h, ?c)", matchNary(&makeIfExpression, Var{"?a"},
                                                Var{"?h"}, Var{"?c"}));

  // The following three cases delegate to a separate parsing function, so we
  // only perform rather simple checks.
  expectBuiltInCall("COUNT(?x)", matchPtr<CountExpression>());
  expectBuiltInCall("regex(?x, \"ab\")", matchPtr<RegexExpression>());
  expectBuiltInCall("LANG(?x)", matchPtr<LangExpression>());
  expectFails("SHA512(?x)");
}

TEST(SparqlParser, unaryExpression) {
  using namespace sparqlExpression;
  using namespace builtInCallTestHelpers;
  auto expectUnary = ExpectCompleteParse<&Parser::unaryExpression>{};

  expectUnary("-?x", matchUnary(&makeUnaryMinusExpression));
  expectUnary("!?x", matchUnary(&makeUnaryNegateExpression));
}

TEST(SparqlParser, multiplicativeExpression) {
  using namespace sparqlExpression;
  using namespace builtInCallTestHelpers;
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};
  auto expectMultiplicative =
      ExpectCompleteParse<&Parser::multiplicativeExpression>{};
  expectMultiplicative("?x * ?y", matchNary(&makeMultiplyExpression, x, y));
  expectMultiplicative("?y / ?x", matchNary(&makeDivideExpression, y, x));
  expectMultiplicative(
      "?z * ?y / abs(?x)",
      matchNaryWithChildrenMatchers(&makeDivideExpression,
                                    matchNary(&makeMultiplyExpression, z, y),
                                    matchUnary(&makeAbsExpression)));
  expectMultiplicative(
      "?y / ?z * abs(?x)",
      matchNaryWithChildrenMatchers(&makeMultiplyExpression,
                                    matchNary(&makeDivideExpression, y, z),
                                    matchUnary(&makeAbsExpression)));
}

// Return a matcher for an `OperatorAndExpression`.
::testing::Matcher<const SparqlQleverVisitor::OperatorAndExpression&>
matchOperatorAndExpression(
    SparqlQleverVisitor::Operator op,
    const ::testing::Matcher<const sparqlExpression::SparqlExpression::Ptr&>&
        expressionMatcher) {
  using OpAndExp = SparqlQleverVisitor::OperatorAndExpression;
  return ::testing::AllOf(AD_FIELD(OpAndExp, operator_, ::testing::Eq(op)),
                          AD_FIELD(OpAndExp, expression_, expressionMatcher));
}

TEST(SparqlParser, multiplicativeExpressionLeadingSignButNoSpaceContext) {
  using namespace sparqlExpression;
  using namespace builtInCallTestHelpers;
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};
  using Op = SparqlQleverVisitor::Operator;
  auto expectMultiplicative = ExpectCompleteParse<
      &Parser::multiplicativeExpressionWithLeadingSignButNoSpace>{};
  auto matchVariableExpression = [](Variable var) {
    return matchPtr<VariableExpression>(
        AD_PROPERTY(VariableExpression, value, ::testing::Eq(var)));
  };
  auto matchIdExpression = [](Id id) {
    return matchPtr<IdExpression>(
        AD_PROPERTY(IdExpression, value, ::testing::Eq(id)));
  };

  expectMultiplicative("-3 * ?y",
                       matchOperatorAndExpression(
                           Op::Minus, matchNaryWithChildrenMatchers(
                                          &makeMultiplyExpression,
                                          matchIdExpression(Id::makeFromInt(3)),
                                          matchVariableExpression(y))));
  expectMultiplicative(
      "-3.7 / ?y",
      matchOperatorAndExpression(
          Op::Minus,
          matchNaryWithChildrenMatchers(
              &makeDivideExpression, matchIdExpression(Id::makeFromDouble(3.7)),
              matchVariableExpression(y))));

  expectMultiplicative("+5 * ?y",
                       matchOperatorAndExpression(
                           Op::Plus, matchNaryWithChildrenMatchers(
                                         &makeMultiplyExpression,
                                         matchIdExpression(Id::makeFromInt(5)),
                                         matchVariableExpression(y))));
  expectMultiplicative(
      "+3.9 / ?y", matchOperatorAndExpression(
                       Op::Plus, matchNaryWithChildrenMatchers(
                                     &makeDivideExpression,
                                     matchIdExpression(Id::makeFromDouble(3.9)),
                                     matchVariableExpression(y))));
  expectMultiplicative(
      "-3.2 / abs(?x) * ?y",
      matchOperatorAndExpression(
          Op::Minus, matchNaryWithChildrenMatchers(
                         &makeMultiplyExpression,
                         matchNaryWithChildrenMatchers(
                             &makeDivideExpression,
                             matchIdExpression(Id::makeFromDouble(3.2)),
                             matchUnary(&makeAbsExpression)),
                         matchVariableExpression(y))));
}

TEST(SparqlParser, FunctionCall) {
  using namespace sparqlExpression;
  using namespace builtInCallTestHelpers;
  auto expectFunctionCall = ExpectCompleteParse<&Parser::functionCall>{};
  auto expectFunctionCallFails = ExpectParseFails<&Parser::functionCall>{};
  auto geof = GEOF_PREFIX.second;
  auto math = MATH_PREFIX.second;

  // Correct function calls. Check that the parser picks the correct expression.
  expectFunctionCall(absl::StrCat(geof, "latitude>(?x)"),
                     matchUnary(&makeLatitudeExpression));
  expectFunctionCall(absl::StrCat(geof, "longitude>(?x)"),
                     matchUnary(&makeLongitudeExpression));
  expectFunctionCall(
      absl::StrCat(geof, "distance>(?a, ?b)"),
      matchNary(&makeDistExpression, Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(absl::StrCat(math, "log>(?x)"),
                     matchUnary(&makeLogExpression));
  expectFunctionCall(absl::StrCat(math, "exp>(?x)"),
                     matchUnary(&makeExpExpression));
  expectFunctionCall(absl::StrCat(math, "sqrt>(?x)"),
                     matchUnary(&makeSqrtExpression));
  expectFunctionCall(absl::StrCat(math, "sin>(?x)"),
                     matchUnary(&makeSinExpression));
  expectFunctionCall(absl::StrCat(math, "cos>(?x)"),
                     matchUnary(&makeCosExpression));
  expectFunctionCall(absl::StrCat(math, "tan>(?x)"),
                     matchUnary(&makeTanExpression));

  // Wrong number of arguments.
  expectFunctionCallFails(
      "<http://www.opengis.net/def/function/geosparql/distance>(?a)");
  // Unknown function with the `geof:` prefix.
  expectFunctionCallFails(
      "<http://www.opengis.net/def/function/geosparql/notExisting>()");
  // Prefix for which no function is known.
  expectFunctionCallFails(
      "<http://www.no-existing-prefixes.com/notExisting>()");
}

// ______________________________________________________________________________
TEST(SparqlParser, substringExpression) {
  using namespace sparqlExpression;
  using namespace builtInCallTestHelpers;
  using V = Variable;
  auto expectBuiltInCall = ExpectCompleteParse<&Parser::builtInCall>{};
  auto expectBuiltInCallFails = ExpectParseFails<&Parser::builtInCall>{};
  expectBuiltInCall("SUBSTR(?x, ?y, ?z)", matchNary(&makeSubstrExpression,
                                                    V{"?x"}, V{"?y"}, V{"?z"}));
  // Note: The large number (the default value for the length, which is
  // automatically truncated) is the largest integer that is representable by
  // QLever. Should this ever change, then this test has to be changed
  // accordingly.
  expectBuiltInCall(
      "SUBSTR(?x, 7)",
      matchNaryWithChildrenMatchers(&makeSubstrExpression,
                                    variableExpressionMatcher(V{"?x"}),
                                    idExpressionMatcher(IntId(7)),
                                    idExpressionMatcher(IntId(Id::maxInt))));
  // Too few arguments
  expectBuiltInCallFails("SUBSTR(?x)");
  // Too many arguments
  expectBuiltInCallFails("SUBSTR(?x), 3, 8, 12");
}

// _________________________________________________________
TEST(SparqlParser, binaryStringExpressions) {
  using namespace sparqlExpression;
  using namespace builtInCallTestHelpers;
  using V = Variable;
  auto expectBuiltInCall = ExpectCompleteParse<&Parser::builtInCall>{};
  auto expectBuiltInCallFails = ExpectParseFails<&Parser::builtInCall>{};

  auto makeMatcher = [](auto function) {
    return matchNary(function, V{"?x"}, V{"?y"});
  };

  expectBuiltInCall("STRSTARTS(?x, ?y)", makeMatcher(&makeStrStartsExpression));
  expectBuiltInCall("STRENDS(?x, ?y)", makeMatcher(&makeStrEndsExpression));
  expectBuiltInCall("CONTAINS(?x, ?y)", makeMatcher(&makeContainsExpression));
  expectBuiltInCall("STRAFTER(?x, ?y)", makeMatcher(&makeStrAfterExpression));
  expectBuiltInCall("STRBEFORE(?x, ?y)", makeMatcher(&makeStrBeforeExpression));
}
