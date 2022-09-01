// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2021-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
//   2022      Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gtest/gtest.h>

#include <iostream>
#include <type_traits>
#include <utility>

#include "SparqlAntlrParserTestHelpers.h"
#include "parser/SparqlParserHelpers.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"

using namespace sparqlParserHelpers;
using Parser = SparqlAutomaticParser;

template <auto F>
auto parse =
    [](const string& input, SparqlQleverVisitor::PrefixMap prefixes = {}) {
      ParserAndVisitor p{input, std::move(prefixes)};
      return p.parseTypesafe(F);
    };

auto parseBind = parse<&Parser::bind>;
auto parseBlankNode = parse<&Parser::blankNode>;
auto parseBlankNodePropertyList = parse<&Parser::blankNodePropertyList>;
auto parseCollection = parse<&Parser::collection>;
auto parseConstructTemplate = parse<&Parser::constructTemplate>;
auto parseConstructTriples = parse<&Parser::constructTriples>;
auto parseConstructQuery = parse<&Parser::constructQuery>;
auto parseDataBlock = parse<&Parser::dataBlock>;
auto parseExpression = parse<&Parser::expression>;
auto parseGraphNode = parse<&Parser::graphNode>;
auto parseGraphTerm = parse<&Parser::graphTerm>;
auto parseGroupClause = parse<&Parser::groupClause>;
auto parseGroupCondition = parse<&Parser::groupCondition>;
auto parseGroupGraphPattern = parse<&Parser::groupGraphPattern>;
auto parseHavingCondition = parse<&Parser::havingCondition>;
auto parseInlineData = parse<&Parser::inlineData>;
auto parseInteger = parse<&Parser::integer>;
auto parseIri = parse<&Parser::iri>;
auto parseIriref = parse<&Parser::iriref>;
auto parseLimitOffsetClause = parse<&Parser::limitOffsetClauses>;
auto parseNumericLiteral = parse<&Parser::numericLiteral>;
auto parseObjectList = parse<&Parser::objectList>;
auto parseOrderClause = parse<&Parser::orderClause>;
auto parseOrderCondition = parse<&Parser::orderCondition>;
auto parsePnameLn = parse<&Parser::pnameLn>;
auto parsePnameNs = parse<&Parser::pnameNs>;
auto parsePrefixDecl = parse<&Parser::prefixDecl>;
auto parsePrefixedName = parse<&Parser::prefixedName>;
auto parsePropertyList = parse<&Parser::propertyList>;
auto parsePropertyListNotEmpty = parse<&Parser::propertyListNotEmpty>;
auto parsePropertyListPathNotEmpty = parse<&Parser::propertyListPathNotEmpty>;
auto parseRdfLiteral = parse<&Parser::rdfLiteral>;
auto parseSelectClause = parse<&Parser::selectClause>;
auto parseSelectQuery = parse<&Parser::selectQuery>;
auto parseSolutionModifier = parse<&Parser::solutionModifier>;
auto parseTriplesSameSubject = parse<&Parser::triplesSameSubject>;
auto parseTriplesSameSubjectPath = parse<&Parser::triplesSameSubjectPath>;
auto parseVariable = parse<&Parser::var>;
auto parseVarOrTerm = parse<&Parser::varOrTerm>;
auto parseVerb = parse<&Parser::verb>;
auto parseVerbPathOrSimple = parse<&Parser::verbPathOrSimple>;

template <auto parseFunction,
          typename Value = decltype(parseFunction("").resultOfParse_)>
struct expectFunction {
  SparqlQleverVisitor::PrefixMap prefixMap_;

  auto operator()(const string& input, Value&& value) const {
    return expectCompleteParse(parseFunction(input, prefixMap_),
                               testing::Eq(value));
  };

  auto operator()(const string& input, const auto& matcher) const {
    return expectCompleteParse(parseFunction(input, prefixMap_), matcher);
  };

  auto operator()(const string& input, Value&& value,
                  SparqlQleverVisitor::PrefixMap&& prefixMap) const {
    return expectCompleteParse(parseFunction(input, std::move(prefixMap)),
                               testing::Eq(value));
  };

  auto operator()(const string& input, const auto& matcher,
                  SparqlQleverVisitor::PrefixMap&& prefixMap) const {
    return expectCompleteParse(parseFunction(input, std::move(prefixMap)),
                               matcher);
  };
};

template <auto parseFunction>
auto makeExpectCompleteParser(SparqlQleverVisitor::PrefixMap&& prefixMap = {}) {
  return expectFunction<parseFunction>{std::move(prefixMap)};
}

template <auto parseFunction, typename Exception = ParseException>
auto makeExpectParsingFails(SparqlQleverVisitor::PrefixMap prefixMap = {}) {
  // Capture `prefixMap` by value. See above comment.
  return [prefixMap = std::move(prefixMap)](const string& input) {
    EXPECT_THROW(parseFunction(input), Exception) << input;
  };
}

template <typename T>
void testNumericLiteral(const std::string& input, T target) {
  auto result = parseNumericLiteral(input);
  ASSERT_EQ(result.remainingText_.size(), 0);
  auto value = get<T>(result.resultOfParse_);

  if constexpr (std::is_floating_point_v<T>) {
    ASSERT_DOUBLE_EQ(target, value);
  } else {
    ASSERT_EQ(target, value);
  }
}

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

namespace {
template <typename Exception = ParseException>
void expectNumericLiteralFails(const string& input) {
  EXPECT_THROW(parseNumericLiteral(input), Exception) << input;
}
}  // namespace

TEST(SparqlParser, NumericLiterals) {
  testNumericLiteral("3.0", 3.0);
  testNumericLiteral("3.0e2", 300.0);
  testNumericLiteral("3.0e-2", 0.030);
  testNumericLiteral("3", (int64_t)3ll);
  testNumericLiteral("-3.0", -3.0);
  testNumericLiteral("-3", (int64_t)-3ll);
  testNumericLiteral("+3", (int64_t)3ll);
  testNumericLiteral("+3.02", 3.02);
  testNumericLiteral("+3.1234e12", 3123400000000.0);
  testNumericLiteral(".234", 0.234);
  testNumericLiteral("+.0123", 0.0123);
  testNumericLiteral("-.5123", -0.5123);
  testNumericLiteral(".234e4", 2340.0);
  testNumericLiteral("+.0123E-3", 0.0000123);
  testNumericLiteral("-.5123E12", -512300000000.0);
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
    p.visitor_.visitTypesafe(p.parser_.prefixDecl());
    auto prefixes = p.visitor_.prefixMap();
    ASSERT_EQ(prefixes.size(), 1);
    ASSERT_EQ(prefixes.at("wd"), "<www.wikidata.org/>");
  }
  expectCompleteParse(parsePrefixDecl("PREFIX wd: <www.wikidata.org/>"),
                      Eq(SparqlPrefix("wd", "<www.wikidata.org/>")));
  expectCompleteParse(parsePnameLn("wd:bimbam", prefixMap),
                      StrEq("<www.wikidata.org/bimbam>"));
  expectCompleteParse(parsePnameNs("wd:", prefixMap),
                      StrEq("<www.wikidata.org/>"));
  expectCompleteParse(parsePrefixedName("wd:bimbam", prefixMap),
                      StrEq("<www.wikidata.org/bimbam>"));
  {
    auto result = parseIriref("<somethingsomething> <rest>", prefixMap);
    ASSERT_EQ(result.resultOfParse_, "<somethingsomething>");
    ASSERT_EQ(result.remainingText_, "<rest>");
  }
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
  auto resultofParse = parseExpression(s);
  EXPECT_EQ(resultofParse.remainingText_.length(), 6);
  auto resultAsExpression = std::move(resultofParse.resultOfParse_);

  QueryExecutionContext* qec = nullptr;
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  IdTable table{alloc};
  ResultTable::LocalVocab localVocab;
  sparqlExpression::EvaluationContext input{*qec, map, table, alloc,
                                            localVocab};
  auto result = resultAsExpression->evaluate(&input);
  AD_CHECK(std::holds_alternative<double>(result));
  ASSERT_FLOAT_EQ(25.0, std::get<double>(result));
}

TEST(SparqlParser, ComplexConstructQuery) {
  string input =
      "CONSTRUCT { [?a ( ?b (?c) )] ?d [?e [?f ?g]] . "
      "<http://wallscope.co.uk/resource/olympics/medal/#something> a "
      "<http://wallscope.co.uk/resource/olympics/medal/#somethingelse> } "
      "WHERE {}";

  expectCompleteArrayParse(
      parseConstructQuery(input),
      ElementsAre(IsBlankNode(true, "0"), IsVariableVariant("?a"),
                  IsBlankNode(true, "3")),
      ElementsAre(IsBlankNode(true, "1"), IsIri(first), IsBlankNode(true, "2")),
      ElementsAre(IsBlankNode(true, "1"), IsIri(rest), IsIri(nil)),
      ElementsAre(IsBlankNode(true, "2"), IsIri(first),
                  IsVariableVariant("?c")),
      ElementsAre(IsBlankNode(true, "2"), IsIri(rest), IsIri(nil)),
      ElementsAre(IsBlankNode(true, "3"), IsIri(first),
                  IsVariableVariant("?b")),
      ElementsAre(IsBlankNode(true, "3"), IsIri(rest), IsBlankNode(true, "1")),
      ElementsAre(IsBlankNode(true, "0"), IsVariableVariant("?d"),
                  IsBlankNode(true, "4")),
      ElementsAre(IsBlankNode(true, "4"), IsVariableVariant("?e"),
                  IsBlankNode(true, "5")),
      ElementsAre(IsBlankNode(true, "5"), IsVariableVariant("?f"),
                  IsVariableVariant("?g")),
      ElementsAre(IsIri("<http://wallscope.co.uk/resource/olympics/medal/"
                        "#something>"),
                  IsIri(type),
                  IsIri("<http://wallscope.co.uk/resource/olympics/medal/"
                        "#somethingelse>")));
}

TEST(SparqlParser, GraphTerm) {
  auto expectGraphTerm = makeExpectCompleteParser<parseGraphTerm>();
  expectGraphTerm("1337", IsLiteral("1337"));
  expectGraphTerm("true", IsLiteral("true"));
  expectGraphTerm("[]", IsBlankNode(true, "0"));
  {
    const std::string iri = "<http://dummy-iri.com#fragment>";
    expectCompleteParse(parseGraphTerm(iri), IsIri(iri));
  }
  expectGraphTerm("\"abc\"", IsLiteral("\"abc\""));
  expectGraphTerm("()", IsIri(nil));
}

TEST(SparqlParser, RdfCollectionSingleVar) {
  expectCompleteParse(
      parseCollection("( ?a )"),
      Pair(IsBlankNode(true, "0"),
           ElementsAre(
               ElementsAre(IsBlankNode(true, "0"), IsIri(first),
                           IsVariableVariant("?a")),
               ElementsAre(IsBlankNode(true, "0"), IsIri(rest), IsIri(nil)))));
}

TEST(SparqlParser, RdfCollectionTripleVar) {
  expectCompleteParse(
      parseCollection("( ?a ?b ?c )"),
      Pair(IsBlankNode(true, "2"),
           ElementsAre(
               ElementsAre(IsBlankNode(true, "0"), IsIri(first),
                           IsVariableVariant("?c")),
               ElementsAre(IsBlankNode(true, "0"), IsIri(rest), IsIri(nil)),
               ElementsAre(IsBlankNode(true, "1"), IsIri(first),
                           IsVariableVariant("?b")),
               ElementsAre(IsBlankNode(true, "1"), IsIri(rest),
                           IsBlankNode(true, "0")),
               ElementsAre(IsBlankNode(true, "2"), IsIri(first),
                           IsVariableVariant("?a")),
               ElementsAre(IsBlankNode(true, "2"), IsIri(rest),
                           IsBlankNode(true, "1")))));
}

TEST(SparqlParser, BlankNodeAnonymous) {
  expectCompleteParse(parseBlankNode("[ \t\r\n]"), IsBlankNode(true, "0"));
}

TEST(SparqlParser, BlankNodeLabelled) {
  expectCompleteParse(parseBlankNode("_:label123"),
                      IsBlankNode(false, "label123"));
}

TEST(SparqlParser, ConstructTemplateEmpty) {
  expectCompleteParse(parseConstructTemplate("{}"), testing::Eq(std::nullopt));
}

TEST(SparqlParser, ConstructTriplesSingletonWithTerminator) {
  expectCompleteParse(
      parseConstructTriples("?a ?b ?c ."),
      ElementsAre(ElementsAre(IsVariableVariant("?a"), IsVariableVariant("?b"),
                              IsVariableVariant("?c"))));
}

TEST(SparqlParser, ConstructTriplesWithTerminator) {
  expectCompleteParse(
      parseConstructTriples("?a ?b ?c . ?d ?e ?f . ?g ?h ?i ."),
      ElementsAre(ElementsAre(IsVariableVariant("?a"), IsVariableVariant("?b"),
                              IsVariableVariant("?c")),
                  ElementsAre(IsVariableVariant("?d"), IsVariableVariant("?e"),
                              IsVariableVariant("?f")),
                  ElementsAre(IsVariableVariant("?g"), IsVariableVariant("?h"),
                              IsVariableVariant("?i"))));
}

TEST(SparqlParser, TriplesSameSubjectVarOrTerm) {
  expectCompleteParse(
      parseConstructTriples("?a ?b ?c"),
      ElementsAre(ElementsAre(IsVariableVariant("?a"), IsVariableVariant("?b"),
                              IsVariableVariant("?c"))));
}

TEST(SparqlParser, TriplesSameSubjectTriplesNodeWithPropertyList) {
  expectCompleteParse(
      parseTriplesSameSubject("(?a) ?b ?c"),
      ElementsAre(ElementsAre(IsBlankNode(true, "0"), IsIri(first),
                              IsVariableVariant("?a")),
                  ElementsAre(IsBlankNode(true, "0"), IsIri(rest), IsIri(nil)),
                  ElementsAre(IsBlankNode(true, "0"), IsVariableVariant("?b"),
                              IsVariableVariant("?c"))));
}

TEST(SparqlParser, TriplesSameSubjectTriplesNodeEmptyPropertyList) {
  expectCompleteParse(
      parseTriplesSameSubject("(?a)"),
      ElementsAre(
          ElementsAre(IsBlankNode(true, "0"), IsIri(first),
                      IsVariableVariant("?a")),
          ElementsAre(IsBlankNode(true, "0"), IsIri(rest), IsIri(nil))));
}

TEST(SparqlParser, PropertyList) {
  expectCompleteParse(
      parsePropertyList("a ?a"),
      Pair(ElementsAre(ElementsAre(IsIri(type), IsVariableVariant("?a"))),
           IsEmpty()));
}

TEST(SparqlParser, EmptyPropertyList) {
  expectCompleteParse(parsePropertyList(""), Pair(IsEmpty(), IsEmpty()));
}

TEST(SparqlParser, PropertyListNotEmptySingletonWithTerminator) {
  expectCompleteParse(
      parsePropertyListNotEmpty("a ?a ;"),
      Pair(ElementsAre(ElementsAre(IsIri(type), IsVariableVariant("?a"))),
           IsEmpty()));
}

TEST(SparqlParser, PropertyListNotEmptyWithTerminator) {
  expectCompleteParse(
      parsePropertyListNotEmpty("a ?a ; a ?b ; a ?c ;"),
      Pair(ElementsAre(ElementsAre(IsIri(type), IsVariableVariant("?a")),
                       ElementsAre(IsIri(type), IsVariableVariant("?b")),
                       ElementsAre(IsIri(type), IsVariableVariant("?c"))),
           IsEmpty()));
}

TEST(SparqlParser, VerbA) { expectCompleteParse(parseVerb("a"), IsIri(type)); }

TEST(SparqlParser, VerbVariable) {
  expectCompleteParse(parseVerb("?a"), IsVariableVariant("?a"));
}

TEST(SparqlParser, ObjectListSingleton) {
  expectCompleteParse(parseObjectList("?a"),
                      Pair(ElementsAre(IsVariableVariant("?a")), IsEmpty()));
}

TEST(SparqlParser, ObjectList) {
  expectCompleteParse(
      parseObjectList("?a , ?b , ?c"),
      Pair(ElementsAre(IsVariableVariant("?a"), IsVariableVariant("?b"),
                       IsVariableVariant("?c")),
           IsEmpty()));
}

TEST(SparqlParser, BlankNodePropertyList) {
  expectCompleteParse(
      parseBlankNodePropertyList("[ a ?a ; a ?b ; a ?c ]"),
      Pair(IsBlankNode(true, "0"),
           ElementsAre(ElementsAre(IsBlankNode(true, "0"), IsIri(type),
                                   IsVariableVariant("?a")),
                       ElementsAre(IsBlankNode(true, "0"), IsIri(type),
                                   IsVariableVariant("?b")),
                       ElementsAre(IsBlankNode(true, "0"), IsIri(type),
                                   IsVariableVariant("?c")))));
}

TEST(SparqlParser, GraphNodeVarOrTerm) {
  expectCompleteParse(parseGraphNode("?a"),
                      Pair(IsVariableVariant("?a"), IsEmpty()));
}

TEST(SparqlParser, GraphNodeTriplesNode) {
  expectCompleteParse(
      parseGraphNode("(?a)"),
      Pair(IsBlankNode(true, "0"),
           ElementsAre(
               ElementsAre(IsBlankNode(true, "0"), IsIri(first),
                           IsVariableVariant("?a")),
               ElementsAre(IsBlankNode(true, "0"), IsIri(rest), IsIri(nil)))));
}

TEST(SparqlParser, VarOrTermVariable) {
  expectCompleteParse(parseVarOrTerm("?a"), IsVariableVariant("?a"));
}

TEST(SparqlParser, VarOrTermGraphTerm) {
  expectCompleteParse(parseVarOrTerm("()"), IsIri(nil));
}

TEST(SparqlParser, Iri) {
  auto expectIri = makeExpectCompleteParser<parseIri>();
  expectIri("rdfs:label", "<http://www.w3.org/2000/01/rdf-schema#label>",
            {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}});
  expectIri(
      "rdfs:label", "<http://www.w3.org/2000/01/rdf-schema#label>",
      {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}, {"foo", "<bar#>"}});
  expectIri("<http://www.w3.org/2000/01/rdf-schema>",
            "<http://www.w3.org/2000/01/rdf-schema>", {});
  expectIri("@en@rdfs:label",
            "@en@<http://www.w3.org/2000/01/rdf-schema#label>",
            {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}});
  expectIri("@en@<http://www.w3.org/2000/01/rdf-schema>",
            "@en@<http://www.w3.org/2000/01/rdf-schema>", {});
}

TEST(SparqlParser, VarOrIriIri) {
  expectCompleteParse(parseVarOrTerm("<http://testiri>"),
                      IsIri("<http://testiri>"));
}

TEST(SparqlParser, VariableWithQuestionMark) {
  expectCompleteParse(parseVariable("?variableName"),
                      IsVariable("?variableName"));
}

TEST(SparqlParser, VariableWithDollarSign) {
  expectCompleteParse(parseVariable("$variableName"),
                      IsVariable("?variableName"));
}

TEST(SparqlParser, Bind) {
  auto expectBind = makeExpectCompleteParser<parseBind>();
  expectBind("BIND (10 - 5 as ?a)", IsBind("?a", "10-5"));
  expectBind("bInD (?age - 10 As ?s)", IsBind("?s", "?age-10"));
}

TEST(SparqlParser, Integer) {
  auto expectInteger = makeExpectCompleteParser<parseInteger>();
  auto expectIntegerFails = makeExpectParsingFails<parseInteger>();
  expectInteger("1931", 1931ull);
  expectInteger("0", 0ull);
  expectInteger("18446744073709551615", 18446744073709551615ull);
  expectIntegerFails("18446744073709551616");
  expectIntegerFails("10000000000000000000000000000000000000000");
  expectIntegerFails("-1");
}

TEST(SparqlParser, LimitOffsetClause) {
  auto expectLimitOffset = makeExpectCompleteParser<parseLimitOffsetClause>();
  auto expectLimitOffsetFails =
      makeExpectParsingFails<parseLimitOffsetClause>();
  expectLimitOffset("LIMIT 10", IsLimitOffset(10, 1, 0));
  expectLimitOffset("OFFSET 31 LIMIT 12 TEXTLIMIT 14",
                    IsLimitOffset(12, 14, 31));
  expectLimitOffset(
      "textlimit 999",
      IsLimitOffset(std::numeric_limits<uint64_t>::max(), 999, 0));
  expectLimitOffset("LIMIT      999", IsLimitOffset(999, 1, 0));
  expectLimitOffset("OFFSET 43",
                    IsLimitOffset(std::numeric_limits<uint64_t>::max(), 1, 43));
  expectLimitOffset("TEXTLIMIT 43 LIMIT 19", IsLimitOffset(19, 43, 0));
  expectLimitOffsetFails("LIMIT20");
  {
    auto limitOffset =
        parseLimitOffsetClause("Limit 10 TEXTLIMIT 20 offset 0 Limit 20");

    EXPECT_THAT(limitOffset.resultOfParse_, IsLimitOffset(10ull, 20ull, 0ull));
    EXPECT_EQ(limitOffset.remainingText_, "Limit 20");
  }
}

TEST(SparqlParser, OrderCondition) {
  auto expectOrderCondition = makeExpectCompleteParser<parseOrderCondition>();
  auto expectOrderConditionFails =
      makeExpectParsingFails<parseOrderCondition>();
  // var
  expectOrderCondition("?test", IsVariableOrderKeyVariant("?test", false));
  // brackettedExpression
  expectOrderCondition("DESC (?foo)", IsVariableOrderKeyVariant("?foo", true));
  expectOrderCondition("ASC (?bar)", IsVariableOrderKeyVariant("?bar", false));
  expectOrderCondition("ASC(?test - 5)",
                       IsExpressionOrderKey("?test-5", false));
  expectOrderCondition("DESC (10 || (5 && ?foo))",
                       IsExpressionOrderKey("10||(5&&?foo)", true));
  // constraint
  expectOrderCondition("(5 - ?mehr)", IsExpressionOrderKey("5-?mehr", false));
  expectOrderCondition("SUM(?i)", IsExpressionOrderKey("SUM(?i)", false));
  expectOrderConditionFails("ASC SCORE(?i)");
}

TEST(SparqlParser, OrderClause) {
  expectCompleteArrayParse(parseOrderClause("ORDER BY ?test DESC(?foo - 5)"),
                           IsVariableOrderKeyVariant("?test", false),
                           IsExpressionOrderKey("?foo-5", true));
}

TEST(SparqlParser, GroupCondition) {
  auto expectGroupCondition = makeExpectCompleteParser<parseGroupCondition>();
  // variable
  expectGroupCondition("?test", IsVariableGroupKey("?test"));
  // expression without binding
  expectGroupCondition("(?test)", IsExpressionGroupKey("?test"));
  // expression with binding
  expectGroupCondition("(?test AS ?mehr)", IsAliasGroupKey("?test", "?mehr"));
  // builtInCall
  expectGroupCondition("COUNT(?test)", IsExpressionGroupKey("COUNT(?test)"));
  // functionCall
  expectGroupCondition(
      "<http://www.opengis.net/def/function/geosparql/latitude> (?test)",
      IsExpressionGroupKey(
          "<http://www.opengis.net/def/function/geosparql/latitude>(?test)"));
}

TEST(SparqlParser, GroupClause) {
  expectCompleteArrayParse(
      parseGroupClause("GROUP BY ?test (?foo - 10 as ?bar) COUNT(?baz)"),
      IsVariableGroupKey("?test"), IsAliasGroupKey("?foo-10", "?bar"),
      IsExpressionGroupKey("COUNT(?baz)"));
}

TEST(SparqlParser, SolutionModifier) {
  auto expectSolutionModifier =
      [](const string& input,
         const vector<std::variant<std::string, Alias, Variable>>&
             groupByVariables = {},
         const vector<SparqlFilter>& havingClauses = {},
         const vector<std::variant<std::pair<std::string, bool>,
                                   VariableOrderKey>>& orderBy = {},
         const LimitOffsetClause limitOffset = {}) {
        expectCompleteParse(parseSolutionModifier(input),
                            IsSolutionModifier(groupByVariables, havingClauses,
                                               orderBy, limitOffset));
      };
  auto expectIncompleteParse = [](const string& input) {
    EXPECT_FALSE(parseSolutionModifier(input).remainingText_.empty());
  };
  using Var = Variable;
  using VOK = VariableOrderKey;

  expectSolutionModifier("", {}, {}, {}, {});
  // The following are no valid solution modifiers, because ORDER BY
  // has to appear before LIMIT.
  expectIncompleteParse("GROUP BY ?var LIMIT 10 ORDER BY ?var");
  expectSolutionModifier("LIMIT 10", {}, {}, {}, {10, 1, 0});
  expectSolutionModifier(
      "GROUP BY ?var (?b - 10) HAVING (?var != 10) ORDER BY ?var LIMIT 10 "
      "OFFSET 2",
      {Var{"?var"}, "?b-10"}, {{SparqlFilter::FilterType::NE, "?var", "10"}},
      {VOK{"?var", false}}, {10, 1, 2});
  expectSolutionModifier(
      "GROUP BY ?var HAVING (?foo < ?bar) ORDER BY (5 - ?var) TEXTLIMIT 21 "
      "LIMIT 2",
      {Var{"?var"}}, {{SparqlFilter::FilterType::LT, "?foo", "?bar"}},
      {std::pair{"5-?var", false}}, {2, 21, 0});
  expectSolutionModifier("GROUP BY (?var - ?bar) ORDER BY (5 - ?var)",
                         {"?var-?bar"}, {}, {std::pair{"5-?var", false}}, {});
}

TEST(SparqlParser, DataBlock) {
  auto expectDataBlock = makeExpectCompleteParser<parseDataBlock>();
  auto expectDataBlockFails = makeExpectParsingFails<parseDataBlock>();
  expectDataBlock(
      "?test { \"foo\" }",
      IsValues(vector<string>{"?test"}, vector<vector<string>>{{"\"foo\""}}));
  // These are not implemented yet in dataBlockValue
  // (numericLiteral/booleanLiteral)
  // TODO<joka921/qup42> implement
  expectDataBlockFails("?test { true }");
  expectDataBlockFails("?test { 10.0 }");
  expectDataBlockFails("?test { UNDEF }");
  expectDataBlock(R"(?foo { "baz" "bar" })",
                  IsValues({"?foo"}, {{"\"baz\""}, {"\"bar\""}}));
  // TODO<joka921/qup42> implement
  expectDataBlockFails(R"(( ) { })");
  expectDataBlockFails(R"(?foo { })");
  expectDataBlockFails(R"(( ?foo ) { })");
  expectDataBlockFails(R"(( ?foo ?bar ) { (<foo>) (<bar>) })");
  expectDataBlock(R"(( ?foo ?bar ) { (<foo> <bar>) })",
                  IsValues({"?foo", "?bar"}, {{"<foo>", "<bar>"}}));
  expectDataBlock(
      R"(( ?foo ?bar ) { (<foo> "m") ("1" <bar>) })",
      IsValues({"?foo", "?bar"}, {{"<foo>", "\"m\""}, {"\"1\"", "<bar>"}}));
  expectDataBlock(
      R"(( ?foo ?bar ) { (<foo> "m") (<bar> <e>) ("1" "f") })",
      IsValues({"?foo", "?bar"},
               {{"<foo>", "\"m\""}, {"<bar>", "<e>"}, {"\"1\"", "\"f\""}}));
  // TODO<joka921/qup42> implement
  expectDataBlockFails(R"(( ) { (<foo>) })");
}

TEST(SparqlParser, InlineData) {
  auto expectInlineData = makeExpectCompleteParser<parseInlineData>();
  auto expectInlineDataFails = makeExpectParsingFails<parseInlineData>();
  expectInlineData("VALUES ?test { \"foo\" }",
                   IsInlineData({"?test"}, {{"\"foo\""}}));
  // There must always be a block present for InlineData
  expectInlineDataFails("");
}

TEST(SparqlParser, propertyPaths) {
  auto expectPathOrVar = makeExpectCompleteParser<parseVerbPathOrSimple>();
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
  EXPECT_THROW(parseVerbPathOrSimple("b"), ParseException);
  expectPathOrVar("test:foo", Iri("<http://www.example.com/foo>"),
                  {{"test", "<http://www.example.com/>"}});
  expectPathOrVar("?bar", Variable{"?bar"});
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
      makeExpectCompleteParser<parsePropertyListPathNotEmpty>();
  auto expectPropertyListPathFails =
      makeExpectParsingFails<parsePropertyListPathNotEmpty>();
  auto Iri = &PropertyPath::fromIri;
  expectPropertyListPath("<bar> ?foo", {{Iri("<bar>"), Variable{"?foo"}}});
  expectPropertyListPath(
      "<bar> ?foo ; <mehr> ?f",
      {{Iri("<bar>"), Variable{"?foo"}}, {Iri("<mehr>"), Variable{"?f"}}});
  expectPropertyListPath(
      "<bar> ?foo , ?baz",
      {{Iri("<bar>"), Variable{"?foo"}}, {Iri("<bar>"), Variable{"?baz"}}});
  // Currently unsupported by QLever
  expectPropertyListPathFails("<bar> ( ?foo ?baz )");
  expectPropertyListPathFails("<bar> [ <foo> ?bar ]");
}

TEST(SparqlParser, triplesSameSubjectPath) {
  auto expectTriples = makeExpectCompleteParser<parseTriplesSameSubjectPath>();
  auto PathIri = &PropertyPath::fromIri;
  using Var = Variable;
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
        Literal("berlin freiburg")}});
}

TEST(SparqlParser, SelectClause) {
  auto expectSelectClause = makeExpectCompleteParser<parseSelectClause>();
  auto expectVariablesSelect = [&expectSelectClause](
                                   const string& input,
                                   const std::vector<std::string>& variables,
                                   bool distinct = false,
                                   bool reduced = false) {
    expectSelectClause(input, IsVariablesSelect(variables, distinct, reduced));
  };
  using Alias = std::pair<string, string>;
  auto expectSelect = [&expectSelectClause](
                          const string& input,
                          std::vector<std::variant<Variable, Alias>> selection,
                          bool distinct = false, bool reduced = false) {
    expectSelectClause(input,
                       IsSelect(distinct, reduced, std::move(selection)));
  };
  auto expectSelectFails = makeExpectParsingFails<parseSelectClause>();

  expectCompleteParse(parseSelectClause("SELECT *"),
                      IsAsteriskSelect(false, false));
  expectCompleteParse(parseSelectClause("SELECT DISTINCT *"),
                      IsAsteriskSelect(true, false));
  expectCompleteParse(parseSelectClause("SELECT REDUCED *"),
                      IsAsteriskSelect(false, true));
  expectSelectFails("SELECT DISTINCT REDUCED *");
  expectSelectFails("SELECT");
  expectVariablesSelect("SELECT ?foo", {"?foo"});
  expectVariablesSelect("SELECT ?foo ?baz ?bar", {"?foo", "?baz", "?bar"});
  expectVariablesSelect("SELECT DISTINCT ?foo ?bar", {"?foo", "?bar"}, true,
                        false);
  expectVariablesSelect("SELECT REDUCED ?foo ?bar ?baz",
                        {"?foo", "?bar", "?baz"}, false, true);
  expectSelect("SELECT (10 as ?foo) ?bar",
               {Alias{"10", "?foo"}, Variable{"?bar"}});
  expectSelect("SELECT DISTINCT (5 - 10 as ?m)", {Alias{"5-10", "?m"}}, true,
               false);
  expectSelect("SELECT (5 - 10 as ?m) ?foo (10 as ?bar)",
               {Alias{"5-10", "?m"}, Variable{"?foo"}, Alias{"10", "?bar"}});
}

TEST(SparqlParser, HavingCondition) {
  auto expectHavingCondition = makeExpectCompleteParser<parseHavingCondition>();
  auto expectHavingConditionFails =
      makeExpectParsingFails<parseHavingCondition>();

  expectHavingCondition("(?x <= 42.3)", {SparqlFilter::LE, "?x", "42.3"});
  expectHavingCondition("(?height > 1.7)",
                        {SparqlFilter::GT, "?height", "1.7"});
  expectHavingCondition("(?predicate < \"<Z\")",
                        {SparqlFilter::LT, "?predicate", "\"<Z\""});
  expectHavingConditionFails("(LANG(?x) = \"en\")");
}

TEST(SparqlParser, GroupGraphPattern) {
  // The types of arguments to gmock matchers are automatically deduced. We
  // explicitly specify the types here. This e.g. allows to write
  // `GraphPattern(false, {}, subMatcher1, subMatcher2)` instead of
  // `IsGraphPattern(false, vector<SparqlFilter>{}, std::tuple{subMatcher1,
  // subMatcher2})`.
  auto GraphPattern = [](bool optional, vector<SparqlFilter>&& filters,
                         const auto&... childMatchers) {
    return IsGraphPattern(optional, filters, std::tuple{childMatchers...});
  };

  auto expectGraphPattern = makeExpectCompleteParser<parseGroupGraphPattern>(
      {{INTERNAL_PREDICATE_PREFIX_NAME, INTERNAL_PREDICATE_PREFIX_IRI}});
  auto expectGroupGraphPatternFails =
      makeExpectParsingFails<parseGroupGraphPattern>();

  // Test the Components alone.
  expectGraphPattern("{ }", GraphPattern(false, {}));
  expectGraphPattern(
      "{ { ?a ?b ?c } }",
      GraphPattern(
          false, {},
          IsGroup(GraphPattern(false, {}, IsTriples({{"?a", "?b", "?c"}})))));
  expectGraphPattern(
      "{ { ?a ?b ?c } UNION { ?d ?e ?f } }",
      GraphPattern(
          false, {},
          IsUnion(GraphPattern(false, {}, IsTriples({{"?a", "?b", "?c"}})),
                  GraphPattern(false, {}, IsTriples({{"?d", "?e", "?f"}})))));
  expectGraphPattern(
      "{ { ?a ?b ?c } UNION { ?d ?e ?f } UNION { ?g ?h ?i } }",
      GraphPattern(
          false, {},
          IsUnion(GraphPattern(
                      false, {},
                      IsUnion(GraphPattern(false, {},
                                           IsTriples({{"?a", "?b", "?c"}})),
                              GraphPattern(false, {},
                                           IsTriples({{"?d", "?e", "?f"}})))),
                  GraphPattern(false, {}, IsTriples({{"?g", "?h", "?i"}})))));
  expectGraphPattern(
      "{ OPTIONAL { ?a <foo> <bar> } }",
      GraphPattern(
          false, {},
          OptionalGraphPattern({}, IsTriples({{"?a", "<foo>", "<bar>"}}))));
  expectGraphPattern(
      "{ MINUS { ?a <foo> <bar> } }",
      GraphPattern(
          false, {},
          MinusGraphPattern({}, IsTriples({{"?a", "<foo>", "<bar>"}}))));
  expectGraphPattern(
      "{ FILTER (?a = 10) }",
      GraphPattern(false, {{SparqlFilter::FilterType::EQ, "?a", "10"}}));
  expectGraphPattern("{ BIND (?f - ?b as ?c) }",
                     GraphPattern(false, {}, IsBind("?c", "?f-?b")));
  expectGraphPattern(
      "{ VALUES (?a ?b) { (<foo> <bar>) (<a> <b>) } }",
      GraphPattern(
          false, {},
          IsInlineData({"?a", "?b"}, {{"<foo>", "<bar>"}, {"<a>", "<b>"}})));
  expectGraphPattern("{ ?x ?y ?z }",
                     GraphPattern(false, {}, IsTriples({{"?x", "?y", "?z"}})));
  expectGraphPattern("{ SELECT *  WHERE { } }",
                     GraphPattern(false, {},
                                  IsSubSelect(IsAsteriskSelect(false, false),
                                              GraphPattern(false, {}))));
  // Test mixes of the components to make sure that they interact correctly.
  expectGraphPattern(
      "{ ?x ?y ?z ; ?f <bar> }",
      GraphPattern(false, {},
                   IsTriples({{"?x", "?y", "?z"}, {"?x", "?f", "<bar>"}})));
  expectGraphPattern(
      "{ ?x ?y ?z . <foo> ?f <bar> }",
      GraphPattern(false, {},
                   IsTriples({{"?x", "?y", "?z"}, {"<foo>", "?f", "<bar>"}})));
  expectGraphPattern(
      "{ ?x <is-a> <Actor> . FILTER(?x != ?y) . ?y <is-a> <Actor> . "
      "FILTER(?y < ?x) }",
      GraphPattern(false,
                   {{SparqlFilter::FilterType::NE, "?x", "?y"},
                    {SparqlFilter::FilterType::LT, "?y", "?x"}},
                   IsTriples({{"?x", "<is-a>", "<Actor>"},
                              {"?y", "<is-a>", "<Actor>"}})));
  expectGraphPattern(
      "{?x <is-a> <Actor> . FILTER(?x != ?y) . ?y <is-a> <Actor> . ?c "
      "ql:contains-entity ?x . ?c ql:contains-word \"coca* abuse\"}",
      GraphPattern(
          false, {{SparqlFilter::FilterType::NE, "?x", "?y"}},
          IsTriples({{"?x", "<is-a>", "<Actor>"},
                     {"?y", "<is-a>", "<Actor>"},
                     {"?c", CONTAINS_ENTITY_PREDICATE, "?x"},
                     {"?c", CONTAINS_WORD_PREDICATE, "coca* abuse"}})));
  expectGraphPattern(
      "{?x <is-a> <Actor> . BIND(10 - ?foo as ?y) }",
      GraphPattern(false, {}, IsTriples({{"?x", "<is-a>", "<Actor>"}}),
                   IsBind("?y", "10-?foo")));
  expectGraphPattern(
      "{?x <is-a> <Actor> . BIND(10 - ?foo as ?y) . ?a ?b ?c }",
      GraphPattern(false, {}, IsTriples({{"?x", "<is-a>", "<Actor>"}}),
                   IsBind("?y", "10-?foo"), IsTriples({{"?a", "?b", "?c"}})));
  expectGraphPattern(
      "{?x <is-a> <Actor> . OPTIONAL { ?x <foo> <bar> } }",
      GraphPattern(
          false, {}, IsTriples({{"?x", "<is-a>", "<Actor>"}}),
          OptionalGraphPattern({}, IsTriples({{"?x", "<foo>", "<bar>"}}))));
  expectGraphPattern("{ SELECT *  WHERE { } VALUES ?a { <a> <b> } }",
                     GraphPattern(false, {},
                                  IsSubSelect(IsAsteriskSelect(false, false),
                                              GraphPattern(false, {})),
                                  IsInlineData({"?a"}, {{"<a>"}, {"<b>"}})));
  // graphGraphPattern and serviceGraphPattern are not supported.
  expectGroupGraphPatternFails("{ GRAPH ?a { } }");
  expectGroupGraphPatternFails("{ GRAPH <foo> { } }");
  expectGroupGraphPatternFails("{ SERVICE <foo> { } }");
  expectGroupGraphPatternFails("{ SERVICE SILENT ?bar { } }");
}

TEST(SparqlParser, RDFLiteral) {
  auto expectRDFLiteral = makeExpectCompleteParser<parseRdfLiteral>(
      {{"xsd", "<http://www.w3.org/2001/XMLSchema#>"}});
  auto expectRDFLiteralFails = makeExpectParsingFails<parseRdfLiteral>();

  expectRDFLiteral("   \"Astronaut\"^^xsd:string  \t",
                   "\"Astronaut\"^^<http://www.w3.org/2001/XMLSchema#string>");
  // The conversion to the internal date format
  // (":v:date:0000000000000001950-01-01T00:00:00") is done by
  // TurtleStringParser<TokenizerCtre>::parseTripleObject(resultAsString) which
  // is only called at triplesBlock.
  expectRDFLiteral(
      "\"1950-01-01T00:00:00\"^^xsd:dateTime",
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>");
  expectRDFLiteralFails(R"(?a ?b "The \"Moon\""@en .)");
}
