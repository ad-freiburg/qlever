// Copyright 2021 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <iostream>
#include <type_traits>
#include <typeindex>
#include <utility>

#include "../QueryPlannerTestHelpers.h"
#include "../SparqlExpressionTestHelpers.h"
#include "../util/AllocatorTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "SparqlAntlrParserTestHelpers.h"
#include "global/RuntimeParameters.h"
#include "parser/ConstructClause.h"
#include "parser/SparqlParserHelpers.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "rdfTypes/Iri.h"
#include "util/SourceLocation.h"

namespace {
using namespace sparqlParserTestHelpers;
using std::string;

auto iri = ad_utility::testing::iri;

auto lit = ad_utility::testing::tripleComponentLiteral;

PropertyPath PathIri(std::string_view iri) {
  return PropertyPath::fromIri(
      ad_utility::triple_component::Iri::fromIriref(iri));
}

const EncodedIriManager* encodedIriManager() {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
}
}  // namespace

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
    static ad_utility::BlankNodeManager blankNodeManager;
    ParserAndVisitor p{&blankNodeManager, encodedIriManager(),
                       "PREFIX wd: <www.wikidata.org/>"};
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
  sparqlExpression::EvaluationContext input{
      *ad_utility::testing::getQec(),
      map,
      table,
      alloc,
      localVocab,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      sparqlExpression::EvaluationContext::TimePoint::max()};
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
           {Blank("2"), Iri(first), Blank("1")},
           {Blank("2"), Iri(rest), Iri(nil)},
           {Blank("1"), Iri(first), Var("?c")},
           {Blank("1"), Iri(rest), Iri(nil)},
           {Blank("3"), Iri(first), Var("?b")},
           {Blank("3"), Iri(rest), Blank("2")},
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
  expectGraphTerm("[]", m::InternalVariable("0"));
  auto expectGraphTermConstruct =
      ExpectCompleteParse<&Parser::graphTerm, true>{};
  expectGraphTermConstruct("[]", m::BlankNode(true, "0"));
  {
    const std::string iri = "<http://dummy-iri.com#fragment>";
    expectCompleteParse(parse<&Parser::graphTerm>(iri), m::Iri(iri));
  }
  expectGraphTerm("\"abc\"", m::Literal("\"abc\""));
  expectGraphTerm("()", m::Iri(nil));
}

TEST(SparqlParser, RdfCollectionSingleVar) {
  expectCompleteParse(
      parseCollectionConstruct("( ?a )"),
      Pair(m::BlankNode(true, "0"),
           ElementsAre(ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                                   m::VariableVariant("?a")),
                       ElementsAre(m::BlankNode(true, "0"), m::Iri(rest),
                                   m::Iri(nil)))));
  expectCompleteParse(
      parseCollection("( ?a )"),
      Pair(m::VariableVariant("?_QLever_internal_variable_0"),
           ElementsAre(
               ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                           m::Iri(first), m::VariableVariant("?a")),
               ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                           m::Iri(rest), m::Iri(nil)))));
}

TEST(SparqlParser, RdfCollectionTripleVar) {
  auto Var = m::VariableVariant;
  auto Blank = [](const std::string& label) {
    return m::BlankNode(true, label);
  };
  auto BlankVar = [](int number) {
    return m::VariableVariant(
        absl::StrCat("?_QLever_internal_variable_", number));
  };
  expectCompleteParse(
      parseCollectionConstruct("( ?a ?b ?c )"),
      Pair(m::BlankNode(true, "2"),
           ElementsAre(ElementsAre(Blank("0"), m::Iri(first), Var("?c")),
                       ElementsAre(Blank("0"), m::Iri(rest), m::Iri(nil)),
                       ElementsAre(Blank("1"), m::Iri(first), Var("?b")),
                       ElementsAre(Blank("1"), m::Iri(rest), Blank("0")),
                       ElementsAre(Blank("2"), m::Iri(first), Var("?a")),
                       ElementsAre(Blank("2"), m::Iri(rest), Blank("1")))));
  expectCompleteParse(
      parseCollection("( ?a ?b ?c )"),
      Pair(BlankVar(2),
           ElementsAre(ElementsAre(BlankVar(0), m::Iri(first), Var("?c")),
                       ElementsAre(BlankVar(0), m::Iri(rest), m::Iri(nil)),
                       ElementsAre(BlankVar(1), m::Iri(first), Var("?b")),
                       ElementsAre(BlankVar(1), m::Iri(rest), BlankVar(0)),
                       ElementsAre(BlankVar(2), m::Iri(first), Var("?a")),
                       ElementsAre(BlankVar(2), m::Iri(rest), BlankVar(1)))));
}

TEST(SparqlParser, BlankNodeAnonymous) {
  expectCompleteParse(parseBlankNodeConstruct("[ \t\r\n]"),
                      m::BlankNode(true, "0"));
  expectCompleteParse(parseBlankNode("[ \t\r\n]"), m::InternalVariable("0"));
}

TEST(SparqlParser, BlankNodeLabelled) {
  expectCompleteParse(parseBlankNodeConstruct("_:label123"),
                      m::BlankNode(false, "label123"));
  expectCompleteParse(parseBlankNode("_:label123"),
                      m::InternalVariable("label123"));
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
      parseTriplesSameSubjectConstruct("(?a) ?b ?c"),
      ElementsAre(
          ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                      m::VariableVariant("?a")),
          ElementsAre(m::BlankNode(true, "0"), m::Iri(rest), m::Iri(nil)),
          ElementsAre(m::BlankNode(true, "0"), m::VariableVariant("?b"),
                      m::VariableVariant("?c"))));
  expectCompleteParse(
      parseTriplesSameSubject("(?a) ?b ?c"),
      ElementsAre(
          ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                      m::Iri(first), m::VariableVariant("?a")),
          ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                      m::Iri(rest), m::Iri(nil)),
          ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                      m::VariableVariant("?b"), m::VariableVariant("?c"))));
}

TEST(SparqlParser, TriplesSameSubjectTriplesNodeEmptyPropertyList) {
  expectCompleteParse(
      parseTriplesSameSubjectConstruct("(?a)"),
      ElementsAre(
          ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                      m::VariableVariant("?a")),
          ElementsAre(m::BlankNode(true, "0"), m::Iri(rest), m::Iri(nil))));
  expectCompleteParse(
      parseTriplesSameSubject("(?a)"),
      ElementsAre(
          ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                      m::Iri(first), m::VariableVariant("?a")),
          ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                      m::Iri(rest), m::Iri(nil))));
}

TEST(SparqlParser, TriplesSameSubjectBlankNodePropertyList) {
  auto doTest = ad_utility::ApplyAsValueIdentity{[](auto allowPath) {
    auto input = "[ ?x ?y ] ?a ?b";
    auto [output, internal] = [&input, allowPath]() {
      if constexpr (allowPath) {
        return std::pair(parse<&Parser::triplesSameSubjectPath>(input),
                         m::InternalVariable("0"));
      } else {
        return std::pair(parse<&Parser::triplesSameSubject, true>(input),
                         m::BlankNode(true, "0"));
      }
    }();

    auto var = m::VariableVariant;
    expectCompleteParse(
        output, UnorderedElementsAre(
                    ::testing::FieldsAre(internal, var("?x"), var("?y")),
                    ::testing::FieldsAre(internal, var("?a"), var("?b"))));
  }};
  doTest.template operator()<true>();
  doTest.template operator()<false>();
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
  auto doMatch = ad_utility::ApplyAsValueIdentity{[](auto InsideConstruct) {
    const auto blank = [InsideConstruct] {
      if constexpr (InsideConstruct) {
        return m::BlankNode(true, "0");
      } else {
        return m::InternalVariable("0");
      }
    }();
    expectCompleteParse(
        parse<&Parser::blankNodePropertyList, InsideConstruct>(
            "[ a ?a ; a ?b ; a ?c ]"),
        Pair(blank,
             ElementsAre(
                 ElementsAre(blank, m::Iri(type), m::VariableVariant("?a")),
                 ElementsAre(blank, m::Iri(type), m::VariableVariant("?b")),
                 ElementsAre(blank, m::Iri(type), m::VariableVariant("?c")))));
  }};
  doMatch.template operator()<true>();
  doMatch.template operator()<false>();
}

TEST(SparqlParser, GraphNodeVarOrTerm) {
  expectCompleteParse(parseGraphNode("?a"),
                      Pair(m::VariableVariant("?a"), IsEmpty()));
}

TEST(SparqlParser, GraphNodeTriplesNode) {
  expectCompleteParse(
      parseGraphNodeConstruct("(?a)"),
      Pair(m::BlankNode(true, "0"),
           ElementsAre(ElementsAre(m::BlankNode(true, "0"), m::Iri(first),
                                   m::VariableVariant("?a")),
                       ElementsAre(m::BlankNode(true, "0"), m::Iri(rest),
                                   m::Iri(nil)))));
  expectCompleteParse(
      parseGraphNode("(?a)"),
      Pair(m::VariableVariant("?_QLever_internal_variable_0"),
           ElementsAre(
               ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                           m::Iri(first), m::VariableVariant("?a")),
               ElementsAre(m::VariableVariant("?_QLever_internal_variable_0"),
                           m::Iri(rest), m::Iri(nil)))));
}

TEST(SparqlParser, VarOrTermVariable) {
  expectCompleteParse(parseVarOrTerm("?a"), m::VariableVariant("?a"));
}

TEST(SparqlParser, VarOrTermGraphTerm) {
  expectCompleteParse(parseVarOrTerm("()"), m::Iri(nil));
}

TEST(SparqlParser, Iri) {
  auto iri = &TripleComponent::Iri::fromIriref;
  auto expectIri = ExpectCompleteParse<&Parser::iri>{};
  expectIri("rdfs:label", iri("<http://www.w3.org/2000/01/rdf-schema#label>"),
            {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}});
  expectIri(
      "rdfs:label", iri("<http://www.w3.org/2000/01/rdf-schema#label>"),
      {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}, {"foo", "<bar#>"}});
  expectIri("<http://www.w3.org/2000/01/rdf-schema>"s,
            iri("<http://www.w3.org/2000/01/rdf-schema>"),
            SparqlQleverVisitor::PrefixMap{});
  expectIri("@en@rdfs:label"s,
            iri("@en@<http://www.w3.org/2000/01/rdf-schema#label>"),
            {{"rdfs", "<http://www.w3.org/2000/01/rdf-schema#>"}});
  expectIri("@en@<http://www.w3.org/2000/01/rdf-schema>"s,
            iri("@en@<http://www.w3.org/2000/01/rdf-schema>"),
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
  expectLimitOffset("LIMIT 10", m::LimitOffset(10, std::nullopt, 0));
  expectLimitOffset("OFFSET 31 LIMIT 12 TEXTLIMIT 14",
                    m::LimitOffset(12, 14, 31));
  expectLimitOffset("textlimit 999", m::LimitOffset(std::nullopt, 999, 0));
  expectLimitOffset("LIMIT      999", m::LimitOffset(999, std::nullopt, 0));
  expectLimitOffset("OFFSET 43",
                    m::LimitOffset(std::nullopt, std::nullopt, 43));
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
                         m::SolutionModifier({}, {}, {}, {10, 0, 1}));
  expectSolutionModifier(
      "GROUP BY ?var (?b - 10) HAVING (?var != 10) ORDER BY ?var TEXTLIMIT 1 "
      "LIMIT 10 OFFSET 2",
      m::SolutionModifier({Var{"?var"}, "?b - 10"}, {{"(?var != 10)"}},
                          {VOK{Var{"?var"}, false}}, {10, 2, 1}));
  expectSolutionModifier(
      "GROUP BY ?var HAVING (?foo < ?bar) ORDER BY (5 - ?var) TEXTLIMIT 21 "
      "LIMIT 2",
      m::SolutionModifier({Var{"?var"}}, {{"(?foo < ?bar)"}},
                          {std::pair{"(5 - ?var)", false}}, {2, 0, 21}));
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
  expectDataBlock(
      R"(( ?foo ?bar ) { (<foo> <bar>) })",
      m::Values({Var{"?foo"}, Var{"?bar"}}, {{iri("<foo>"), iri("<bar>")}}));
  expectDataBlock(
      R"(( ?foo ?bar ) { (<foo> "m") ("1" <bar>) })",
      m::Values({Var{"?foo"}, Var{"?bar"}},
                {{iri("<foo>"), lit("\"m\"")}, {lit("\"1\""), iri("<bar>")}}));
  expectDataBlock(
      R"(( ?foo ?bar ) { (<foo> "m") (<bar> <e>) (1 "f") })",
      m::Values({Var{"?foo"}, Var{"?bar"}}, {{iri("<foo>"), lit("\"m\"")},
                                             {iri("<bar>"), iri("<e>")},
                                             {1, lit("\"f\"")}}));
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
  auto Iri = &PathIri;
  auto Sequence = &PropertyPath::makeSequence;
  auto Alternative = &PropertyPath::makeAlternative;
  auto Inverse = &PropertyPath::makeInverse;
  auto Negated = &PropertyPath::makeNegated;
  auto WithLength = &PropertyPath::makeWithLength;
  size_t max = std::numeric_limits<size_t>::max();
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
  expectPathOrVar("^a:a", Inverse(Iri("<http://www.example.com/a>")),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("!a:a", Negated({Iri("<http://www.example.com/a>")}),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("!(a:a)", Negated({Iri("<http://www.example.com/a>")}),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("!(a:a|^a:a)",
                  Negated({Iri("<http://www.example.com/a>"),
                           Inverse(Iri("<http://www.example.com/a>"))}),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("!(a:a|^a:b|a:c|a:d|^a:e)",
                  Negated({Iri("<http://www.example.com/a>"),
                           Inverse(Iri("<http://www.example.com/b>")),
                           Iri("<http://www.example.com/c>"),
                           Iri("<http://www.example.com/d>"),
                           Inverse(Iri("<http://www.example.com/e>"))}),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("(a:a)", Iri("<http://www.example.com/a>"),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("a:a+", WithLength(Iri("<http://www.example.com/a>"), 1, max),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("a:a?", WithLength(Iri("<http://www.example.com/a>"), 0, 1),
                  {{"a", "<http://www.example.com/>"}});
  expectPathOrVar("a:a*", WithLength(Iri("<http://www.example.com/a>"), 0, max),
                  {{"a", "<http://www.example.com/>"}});
  // Test a bigger example that contains everything.
  {
    PropertyPath expected = Alternative(
        {Sequence({
             Iri("<http://www.example.com/a/a>"),
             WithLength(Iri("<http://www.example.com/b/b>"), 0, max),
         }),
         Iri("<http://www.example.com/c/c>"),
         WithLength(
             Sequence({Iri("<http://www.example.com/a/a>"),
                       Iri("<http://www.example.com/b/b>"), Iri("<a/b/c>")}),
             1, max),
         Negated({Iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")}),
         Negated(
             {Inverse(Iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")),
              Iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
              Inverse(Iri("<http://www.example.com/a/a>"))})});
    expectPathOrVar("a:a/b:b*|c:c|(a:a/b:b/<a/b/c>)+|!a|!(^a|a|^a:a)", expected,
                    {{"a", "<http://www.example.com/a/>"},
                     {"b", "<http://www.example.com/b/>"},
                     {"c", "<http://www.example.com/c/>"}});
  }
}

// _____________________________________________________________________________
TEST(SparqlParser, propertyListPathNotEmpty) {
  auto expectPropertyListPath =
      ExpectCompleteParse<&Parser::propertyListPathNotEmpty>{};
  auto Iri = &PathIri;
  expectPropertyListPath("<bar> ?foo", {{{Iri("<bar>"), Var{"?foo"}}}, {}});
  expectPropertyListPath(
      "<bar> ?foo ; <mehr> ?f",
      {{{Iri("<bar>"), Var{"?foo"}}, {Iri("<mehr>"), Var{"?f"}}}, {}});
  expectPropertyListPath(
      "<bar> ?foo , ?baz",
      {{{Iri("<bar>"), Var{"?foo"}}, {Iri("<bar>"), Var{"?baz"}}}, {}});

  // A more complex example.
  auto V = m::VariableVariant;
  auto internal0 = m::InternalVariable("0");
  auto internal1 = m::InternalVariable("1");
  auto internal2 = m::InternalVariable("2");
  auto bar = m::Predicate(iri("<bar>"));
  expectPropertyListPath(
      "?x [?y ?z; <bar> ?b, ?p, [?d ?e], [<bar> ?e]]; ?u ?v",
      Pair(ElementsAre(Pair(V("?x"), internal0), Pair(V("?u"), V("?v"))),
           UnorderedElementsAre(
               ::testing::FieldsAre(internal0, V("?y"), V("?z")),
               ::testing::FieldsAre(internal0, bar, V("?b")),
               ::testing::FieldsAre(internal0, bar, V("?p")),
               ::testing::FieldsAre(internal0, bar, internal1),
               ::testing::FieldsAre(internal1, V("?d"), V("?e")),
               ::testing::FieldsAre(internal0, bar, internal2),
               ::testing::FieldsAre(internal2, bar, V("?e")))));
}

TEST(SparqlParser, triplesSameSubjectPath) {
  auto expectTriples = ExpectCompleteParse<&Parser::triplesSameSubjectPath>{};
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
  auto expectTriplesConstruct =
      ExpectCompleteParse<&Parser::triplesSameSubjectPath, true>{};
  expectTriplesConstruct("_:1 <bar> ?baz", {{BlankNode(false, "1"),
                                             PathIri("<bar>"), Var{"?baz"}}});
  expectTriples(
      "_:one <bar> ?baz",
      {{Var{absl::StrCat(QLEVER_INTERNAL_BLANKNODE_VARIABLE_PREFIX, "one")},
        PathIri("<bar>"), Var{"?baz"}}});
  expectTriples("10.0 <bar> true",
                {{Literal(10.0), PathIri("<bar>"), Literal(true)}});
  expectTriples(
      "<foo> "
      "<http://qlever.cs.uni-freiburg.de/builtin-functions/contains-word> "
      "\"Berlin Freiburg\"",
      {{Iri("<foo>"),
        PathIri("<http://qlever.cs.uni-freiburg.de/builtin-functions/"
                "contains-word>"),
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
  expectHavingCondition("(LANG(?x) = \"en\")",
                        m::stringMatchesFilter("(LANG(?x) = \"en\")"));
}

TEST(SparqlParser, GroupGraphPattern) {
  auto expectGraphPattern =
      ExpectCompleteParse<&Parser::groupGraphPattern>{defaultPrefixMap};
  auto expectGroupGraphPatternFails =
      ExpectParseFails<&Parser::groupGraphPattern>{{}};
  auto DummyTriplesMatcher = m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}}});

  // Empty GraphPatterns.
  expectGraphPattern("{ }", m::GraphPattern());
  expectGraphPattern(
      "{ SELECT *  WHERE { } }",
      m::GraphPattern(m::SubSelect(::testing::_, m::GraphPattern())));

  SparqlTriple abc{Var{"?a"}, Var{"?b"}, Var{"?c"}};
  SparqlTriple def{Var{"?d"}, Var{"?e"}, Var{"?f"}};
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
          m::GraphPattern(m::Triples({{Var{"?g"}, Var{"?h"}, Var{"?i"}}})))));
  expectGraphPattern("{ OPTIONAL { ?a <foo> <bar> } }",
                     m::GraphPattern(m::OptionalGraphPattern(m::Triples(
                         {{Var{"?a"}, iri("<foo>"), iri("<bar>")}}))));
  expectGraphPattern("{ MINUS { ?a <foo> <bar> } }",
                     m::GraphPattern(m::MinusGraphPattern(m::Triples(
                         {{Var{"?a"}, iri("<foo>"), iri("<bar>")}}))));
  expectGraphPattern(
      "{ FILTER (?a = 10) . ?x ?y ?z }",
      m::GraphPattern(false, {"(?a = 10)"}, DummyTriplesMatcher));
  expectGraphPattern("{ BIND (3 as ?c) }",
                     m::GraphPattern(m::Bind(Var{"?c"}, "3")));
  // The variables `?f` and `?b` have not been used before the BIND clause, but
  // this is valid according to the SPARQL standard.
  expectGraphPattern("{ BIND (?f - ?b as ?c) }",
                     m::GraphPattern(m::Bind(Var{"?c"}, "?f - ?b")));
  expectGraphPattern("{ VALUES (?a ?b) { (<foo> <bar>) (<a> <b>) } }",
                     m::GraphPattern(m::InlineData(
                         {Var{"?a"}, Var{"?b"}}, {{iri("<foo>"), iri("<bar>")},
                                                  {iri("<a>"), iri("<b>")}})));
  expectGraphPattern("{ ?x ?y ?z }", m::GraphPattern(DummyTriplesMatcher));
  expectGraphPattern(
      "{ SELECT *  WHERE { ?x ?y ?z } }",
      m::GraphPattern(m::SubSelect(m::AsteriskSelect(false, false),
                                   m::GraphPattern(DummyTriplesMatcher))));
  // Test mixes of the components to make sure that they interact correctly.
  expectGraphPattern(
      "{ ?x ?y ?z ; ?f <bar> }",
      m::GraphPattern(m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}},
                                  {Var{"?x"}, Var{"?f"}, iri("<bar>")}})));
  expectGraphPattern(
      "{ ?x ?y ?z . <foo> ?f <bar> }",
      m::GraphPattern(m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}},
                                  {iri("<foo>"), Var{"?f"}, iri("<bar>")}})));
  expectGraphPattern(
      "{ ?x <is-a> <Actor> . FILTER(?x != ?y) . ?y <is-a> <Actor> . "
      "FILTER(?y < ?x) }",
      m::GraphPattern(
          false, {"(?x != ?y)", "(?y < ?x)"},
          m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")},
                      {Var{"?y"}, iri("<is-a>"), iri("<Actor>")}})));
  expectGraphPattern(
      "{?x <is-a> \"Actor\" . FILTER(?x != ?y) . ?y <is-a> <Actor> . ?c "
      "ql:contains-entity ?x . ?c ql:contains-word \"coca* abuse\"}",
      m::GraphPattern(
          false, {"(?x != ?y)"},
          m::Triples({{Var{"?x"}, iri("<is-a>"), lit("\"Actor\"")},
                      {Var{"?y"}, iri("<is-a>"), iri("<Actor>")},
                      {Var{"?c"}, iri(CONTAINS_ENTITY_PREDICATE), Var{"?x"}},
                      {Var{"?c"}, iri(CONTAINS_WORD_PREDICATE),
                       lit("\"coca* abuse\"")}})));

  // Scoping of variables in combination with a BIND clause.
  expectGraphPattern(
      "{?x <is-a> <Actor> . BIND(10 - ?x as ?y) }",
      m::GraphPattern(m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}}),
                      m::Bind(Var{"?y"}, "10 - ?x")));
  expectGraphPattern(
      "{?x <is-a> <Actor> . BIND(10 - ?x as ?y) . ?a ?b ?c }",
      m::GraphPattern(m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}}),
                      m::Bind(Var{"?y"}, "10 - ?x"),
                      m::Triples({{Var{"?a"}, Var{"?b"}, Var{"?c"}}})));
  expectGroupGraphPatternFails("{?x <is-a> <Actor> . BIND(3 as ?x)}");
  expectGraphPattern(
      "{?x <is-a> <Actor> . {BIND(3 as ?x)} }",
      m::GraphPattern(m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}}),
                      m::GroupGraphPattern(m::Bind(Var{"?x"}, "3"))));
  expectGraphPattern(
      "{?x <is-a> <Actor> . OPTIONAL {BIND(3 as ?x)} }",
      m::GraphPattern(m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}}),
                      m::OptionalGraphPattern(m::Bind(Var{"?x"}, "3"))));
  expectGraphPattern("{ {?x <is-a> <Actor>} UNION { BIND (3 as ?x)}}",
                     m::GraphPattern(m::Union(
                         m::GraphPattern(m::Triples(
                             {{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}})),
                         m::GraphPattern(m::Bind(Var{"?x"}, "3")))));

  expectGraphPattern(
      "{?x <is-a> <Actor> . OPTIONAL { ?x <foo> <bar> } }",
      m::GraphPattern(m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}}),
                      m::OptionalGraphPattern(m::Triples(
                          {{Var{"?x"}, iri("<foo>"), iri("<bar>")}}))));
  expectGraphPattern(
      "{ SELECT *  WHERE { ?x ?y ?z } VALUES ?a { <a> <b> } }",
      m::GraphPattern(
          m::SubSelect(m::AsteriskSelect(false, false),
                       m::GraphPattern(DummyTriplesMatcher)),
          m::InlineData({Var{"?a"}}, {{iri("<a>")}, {iri("<b>")}})));
  expectGraphPattern("{ SERVICE <endpoint> { ?s ?p ?o } }",
                     m::GraphPattern(m::Service(
                         TripleComponent::Iri::fromIriref("<endpoint>"),
                         {Var{"?s"}, Var{"?p"}, Var{"?o"}}, "{ ?s ?p ?o }")));
  expectGraphPattern(
      "{ SERVICE <ep> { { SELECT ?s ?o WHERE { ?s ?p ?o } } } }",
      m::GraphPattern(m::Service(TripleComponent::Iri::fromIriref("<ep>"),
                                 {Var{"?s"}, Var{"?o"}},
                                 "{ { SELECT ?s ?o WHERE { ?s ?p ?o } } }")));

  expectGraphPattern(
      "{ SERVICE SILENT <ep> { { SELECT ?s ?o WHERE { ?s ?p ?o } } } }",
      m::GraphPattern(m::Service(
          TripleComponent::Iri::fromIriref("<ep>"), {Var{"?s"}, Var{"?o"}},
          "{ { SELECT ?s ?o WHERE { ?s ?p ?o } } }", "", true)));

  // SERVICE with a variable endpoint is not yet supported.
  expectGroupGraphPatternFails("{ SERVICE ?endpoint { ?s ?p ?o } }");

  expectGraphPattern(
      "{ GRAPH ?g { ?x <is-a> <Actor> }}",
      m::GraphPattern(m::GroupGraphPatternWithGraph(
          Variable("?g"),
          m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}}))));
  expectGraphPattern(
      "{ GRAPH <foo> { ?x <is-a> <Actor> }}",
      m::GraphPattern(m::GroupGraphPatternWithGraph(
          iri("<foo>"),
          m::Triples({{Var{"?x"}, iri("<is-a>"), iri("<Actor>")}}))));
}

TEST(SparqlParser, RDFLiteral) {
  auto expectRDFLiteral = ExpectCompleteParse<&Parser::rdfLiteral>{
      {{"xsd", "<http://www.w3.org/2001/XMLSchema#>"}}};
  auto expectRDFLiteralFails = ExpectParseFails<&Parser::rdfLiteral>();

  expectRDFLiteral("   \"Astronaut\"^^xsd:string  \t",
                   "\"Astronaut\"^^<http://www.w3.org/2001/XMLSchema#string>"s);
  // The conversion to the internal date format
  // (":v:date:0000000000000001950-01-01T00:00:00") is done by
  // RdfStringParser<TokenizerCtre>::parseTripleObject(resultAsString) which
  // is only called at triplesBlock.
  expectRDFLiteral(
      "\"1950-01-01T00:00:00\"^^xsd:dateTime",
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s);
  expectRDFLiteralFails(R"(?a ?b "The \"Moon\""@en .)");
}

TEST(SparqlParser, SelectQuery) {
  auto contains = [](const std::string& s) { return ::testing::HasSubstr(s); };
  auto expectSelectQuery =
      ExpectCompleteParse<&Parser::selectQuery>{defaultPrefixMap};
  auto expectSelectQueryFails = ExpectParseFails<&Parser::selectQuery>{};
  auto DummyGraphPatternMatcher =
      m::GraphPattern(m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}}}));
  using Graphs = ScanSpecificationAsTripleComponent::Graphs;

  // A matcher that matches the query `SELECT * { ?a <bar> ?foo}`, where the
  // FROM and FROM NAMED clauses can still be specified via arguments.
  auto selectABarFooMatcher = [](Graphs defaultGraphs = std::nullopt,
                                 Graphs namedGraphs = std::nullopt) {
    return m::SelectQuery(
        m::AsteriskSelect(),
        m::GraphPattern(m::Triples({{Var{"?a"}, iri("<bar>"), Var{"?foo"}}})),
        defaultGraphs, namedGraphs);
  };
  expectSelectQuery("SELECT * WHERE { ?a <bar> ?foo }", selectABarFooMatcher());

  expectSelectQuery(
      "SELECT * FROM <x> FROM NAMED <y> WHERE { ?a <bar> ?foo }",
      selectABarFooMatcher(m::Graphs{TripleComponent::Iri::fromIriref("<x>")},
                           m::Graphs{TripleComponent::Iri::fromIriref("<y>")}));

  expectSelectQuery(
      "SELECT * WHERE { ?x ?y ?z }",
      m::SelectQuery(m::AsteriskSelect(), DummyGraphPatternMatcher));
  expectSelectQuery(
      "SELECT ?x WHERE { ?x ?y ?z . FILTER(?x != <foo>) } LIMIT 10 TEXTLIMIT 5",
      testing::AllOf(
          m::SelectQuery(
              m::Select({Var{"?x"}}),
              m::GraphPattern(false, {"(?x != <foo>)"},
                              m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}}}))),
          m::pq::LimitOffset({10, 0, 5})));

  // ORDER BY
  expectSelectQuery("SELECT ?x WHERE { ?x ?y ?z } ORDER BY ?y ",
                    testing::AllOf(m::SelectQuery(m::Select({Var{"?x"}}),
                                                  DummyGraphPatternMatcher),
                                   m::pq::OrderKeys({{Var{"?y"}, false}})));

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
}

TEST(SparqlParser, ConstructQuery) {
  auto expectConstructQuery =
      ExpectCompleteParse<&Parser::constructQuery>{defaultPrefixMap};
  auto expectConstructQueryFails = ExpectParseFails<&Parser::constructQuery>{};
  expectConstructQuery(
      "CONSTRUCT { } WHERE { ?a ?b ?c }",
      m::ConstructQuery({}, m::GraphPattern(m::Triples(
                                {{Var{"?a"}, Var{"?b"}, Var{"?c"}}}))));
  expectConstructQuery(
      "CONSTRUCT { ?a <foo> ?c . } WHERE { ?a ?b ?c }",
      testing::AllOf(m::ConstructQuery(
          {{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}}},
          m::GraphPattern(m::Triples({{Var{"?a"}, Var{"?b"}, Var{"?c"}}})))));
  expectConstructQuery(
      "CONSTRUCT { ?a <foo> ?c . <bar> ?b <baz> } WHERE { ?a ?b ?c . FILTER(?a "
      "> 0) .}",
      m::ConstructQuery(
          {{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}},
           {Iri{"<bar>"}, Var{"?b"}, Iri{"<baz>"}}},
          m::GraphPattern(false, {"(?a > 0)"},
                          m::Triples({{Var{"?a"}, Var{"?b"}, Var{"?c"}}}))));
  expectConstructQuery(
      "CONSTRUCT { ?a <foo> ?c . } WHERE { ?a ?b ?c } ORDER BY ?a LIMIT 10",
      testing::AllOf(
          m::ConstructQuery(
              {{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}}},
              m::GraphPattern(m::Triples({{Var{"?a"}, Var{"?b"}, Var{"?c"}}}))),
          m::pq::LimitOffset({10}), m::pq::OrderKeys({{Var{"?a"}, false}})));
  // This case of the grammar is not useful without Datasets, but we still
  // support it.
  expectConstructQuery(
      "CONSTRUCT WHERE { ?a <foo> ?b }",
      m::ConstructQuery(
          {{Var{"?a"}, Iri{"<foo>"}, Var{"?b"}}},
          m::GraphPattern(m::Triples({{Var{"?a"}, iri("<foo>"), Var{"?b"}}}))));

  // Blank nodes turn into variables inside WHERE.
  expectConstructQuery(
      "CONSTRUCT WHERE { [] <foo> ?b }",
      m::ConstructQuery(
          {{BlankNode{true, "0"}, Iri{"<foo>"}, Var{"?b"}}},
          m::GraphPattern(m::Triples(
              {{Var{absl::StrCat(QLEVER_INTERNAL_BLANKNODE_VARIABLE_PREFIX,
                                 "g_0")},
                iri("<foo>"), Var{"?b"}}}))));

  // Test another variant to cover all cases.
  expectConstructQuery(
      "CONSTRUCT WHERE { <bar> ?foo \"Abc\"@en }",
      m::ConstructQuery(
          {{Iri{"<bar>"}, Var{"?foo"}, Literal{"\"Abc\"@en"}}},
          m::GraphPattern(m::Triples(
              {{iri("<bar>"), Var{"?foo"}, lit("\"Abc\"", "@en")}}))));
  // CONSTRUCT with datasets.
  expectConstructQuery(
      "CONSTRUCT { } FROM <foo> FROM NAMED <foo2> FROM NAMED <foo3> WHERE { }",
      m::ConstructQuery({}, m::GraphPattern(), m::Graphs{iri("<foo>")},
                        m::Graphs{iri("<foo2>"), iri("<foo3>")}));
}

// _____________________________________________________________________________
TEST(SparqlParser, ensureExceptionOnInvalidGraphTerm) {
  static ad_utility::BlankNodeManager blankNodeManager;
  SparqlQleverVisitor visitor{
      &blankNodeManager, encodedIriManager(), {}, std::nullopt};

  EXPECT_THROW(
      visitor.toGraphPattern({{Var{"?a"}, BlankNode{true, "0"}, Var{"?b"}}}),
      ad_utility::Exception);
  EXPECT_THROW(
      visitor.toGraphPattern({{Var{"?a"}, Literal{"\"Abc\""}, Var{"?b"}}}),
      ad_utility::Exception);
}

// Test that ASK queries are parsed as they should.
TEST(SparqlParser, AskQuery) {
  // Some helper functions and abbreviations.
  auto contains = [](const std::string& s) { return ::testing::HasSubstr(s); };
  auto expectAskQuery =
      ExpectCompleteParse<&Parser::askQuery>{defaultPrefixMap};
  auto expectAskQueryFails = ExpectParseFails<&Parser::askQuery>{};
  auto DummyGraphPatternMatcher =
      m::GraphPattern(m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}}}));
  using Graphs = ScanSpecificationAsTripleComponent::Graphs;

  // A matcher that matches the query `ASK { ?a <bar> ?foo}`, where the
  // FROM parts of the query can be specified via `defaultGraphs` and
  // the FROM NAMED parts can be specified via `namedGraphs`.
  auto selectABarFooMatcher = [](Graphs defaultGraphs = std::nullopt,
                                 Graphs namedGraphs = std::nullopt) {
    return m::AskQuery(
        m::GraphPattern(m::Triples({{Var{"?a"}, iri("<bar>"), Var{"?foo"}}})),
        defaultGraphs, namedGraphs);
  };
  expectAskQuery("ASK { ?a <bar> ?foo }", selectABarFooMatcher());

  // ASK query with both a FROM and a FROM NAMED clause.
  Graphs defaultGraphs;
  defaultGraphs.emplace();
  defaultGraphs->insert(TripleComponent::Iri::fromIriref("<x>"));
  Graphs namedGraphs;
  namedGraphs.emplace();
  namedGraphs->insert(TripleComponent::Iri::fromIriref("<y>"));
  expectAskQuery("ASK FROM <x> FROM NAMED <y> WHERE { ?a <bar> ?foo }",
                 selectABarFooMatcher(defaultGraphs, namedGraphs));

  // ASK whether there are any triples at all.
  expectAskQuery("ASK { ?x ?y ?z }", m::AskQuery(DummyGraphPatternMatcher));

  // ASK queries may contain neither of LIMIT, OFFSET, or TEXTLIMIT.
  expectAskQueryFails("ASK WHERE { ?x ?y ?z . FILTER(?x != <foo>) } LIMIT 10");
  expectAskQueryFails("ASK WHERE { ?x ?y ?z . FILTER(?x != <foo>) } OFFSET 20");
  expectAskQueryFails(
      "ASK WHERE { ?x ?y ?z . FILTER(?x != <foo>) } TEXTLIMIT 30");

  // ASK with ORDER BY is allowed (even though the ORDER BY does not change the
  // result).
  expectAskQuery("ASK { ?x ?y ?z } ORDER BY ?y ",
                 testing::AllOf(m::AskQuery(DummyGraphPatternMatcher),
                                m::pq::OrderKeys({{Var{"?y"}, false}})));

  // ASK with GROUP BY is allowed.
  expectAskQuery("ASK { ?x ?y ?z } GROUP BY ?x",
                 testing::AllOf(m::AskQuery(DummyGraphPatternMatcher),
                                m::pq::GroupKeys({Var{"?x"}})));
  expectAskQuery("ASK { ?x ?y ?z } GROUP BY ?x",
                 testing::AllOf(m::AskQuery(DummyGraphPatternMatcher),
                                m::pq::GroupKeys({Var{"?x"}})));

  // HAVING is not allowed without GROUP BY
  expectAskQueryFails(
      "ASK { ?x ?y ?z } HAVING (?x < 3)",
      contains("HAVING clause is only supported in queries with GROUP BY"));
}

// Tests for additional features of the SPARQL parser.
TEST(SparqlParser, Query) {
  auto expectQuery = ExpectCompleteParse<&Parser::query>{defaultPrefixMap};
  auto expectQueryFails = ExpectParseFails<&Parser::query>{};
  auto contains = [](const std::string& s) { return ::testing::HasSubstr(s); };

  // Test that `_originalString` is correctly set.
  expectQuery(
      "SELECT * WHERE { ?a <bar> ?foo }",
      testing::AllOf(
          m::SelectQuery(m::AsteriskSelect(),
                         m::GraphPattern(m::Triples(
                             {{Var{"?a"}, iri("<bar>"), Var{"?foo"}}}))),
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

  // Test that visible variables are correctly set.
  expectQuery(
      "CONSTRUCT { ?a <foo> ?c . } WHERE { ?a ?b ?c }",
      testing::AllOf(
          m::ConstructQuery(
              {{Var{"?a"}, Iri{"<foo>"}, Var{"?c"}}},
              m::GraphPattern(m::Triples({{Var{"?a"}, Var{"?b"}, Var{"?c"}}}))),
          m::VisibleVariables({Var{"?a"}, Var{"?b"}, Var{"?c"}})));
  expectQuery(
      "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } LIMIT 10",
      testing::AllOf(
          m::ConstructQuery(
              {{Var{"?x"}, Iri{"<foo>"}, Iri{"<bar>"}}},
              m::GraphPattern(m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}}}))),
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
              m::GraphPattern(m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}}}))),
          m::pq::OriginalString(
              "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } GROUP BY ?x"),
          m::VisibleVariables({Var{"?x"}, Var{"?y"}, Var{"?z"}})));

  // Construct query with GROUP BY, but a variable that is not grouped is used.
  expectQueryFails(
      "CONSTRUCT { ?x <foo> <bar> } WHERE { ?x ?y ?z } GROUP BY ?y");

  // The same two tests with `ASK` queries
  expectQuery(
      "ASK WHERE { ?x ?y ?z } GROUP BY ?x",
      testing::AllOf(
          m::AskQuery(

              m::GraphPattern(m::Triples({{Var{"?x"}, Var{"?y"}, Var{"?z"}}}))),
          m::pq::OriginalString("ASK WHERE { ?x ?y ?z } GROUP BY ?x"),
          m::VisibleVariables({Var{"?x"}, Var{"?y"}, Var{"?z"}})));

  // Test that the prologue is parsed properly. We use `m::Service` here
  // because the parsing of a SERVICE clause is the only place where the
  // prologue is explicitly passed on to a `parsedQuery::` object.
  expectQuery(
      "PREFIX doof: <http://doof.org/> "
      "SELECT * WHERE { SERVICE <endpoint> { ?s ?p ?o } }",
      m::SelectQuery(m::AsteriskSelect(),
                     m::GraphPattern(m::Service(
                         TripleComponent::Iri::fromIriref("<endpoint>"),
                         {Var{"?s"}, Var{"?p"}, Var{"?o"}}, "{ ?s ?p ?o }",
                         "PREFIX doof: <http://doof.org/>"))));

  // Tests around DESCRIBE.
  {
    // The tested DESCRIBE queries all describe `<x>`, `?y`, and `<z>`.
    using Resources = std::vector<parsedQuery::Describe::VarOrIri>;
    auto Iri = [](const auto& x) {
      return TripleComponent::Iri::fromIriref(x);
    };
    Resources xyz{Iri("<x>"), Var{"?y"}, Iri("<z>")};

    // A matcher for `?y <is-a> ?v`.
    auto graphPatternMatcher =
        m::GraphPattern(m::Triples({{Var{"?y"}, iri("<is-a>"), Var{"?v"}}}));

    // A matcher for the subquery `SELECT ?y { ?y <is-a> ?v }`, which we will
    // use to compute the values for `?y` that are to be described.
    auto selectQueryMatcher1 =
        m::SelectQuery(m::Select({Var{"?y"}}), graphPatternMatcher);

    // DESCRIBE with neither FROM nor FROM NAMED clauses.
    expectQuery("DESCRIBE <x> ?y <z> { ?y <is-a> ?v }",
                m::DescribeQuery(m::Describe(xyz, {}, selectQueryMatcher1)));

    // `DESCRIBE *` query that is equivalent to `DESCRIBE <x> ?y <z> { ... }`.
    auto selectQueryMatcher2 =
        m::SelectQuery(m::Select({Var{"?y"}, Var{"?v"}}), graphPatternMatcher);
    Resources yv{Var{"?y"}, Var{"?v"}};
    expectQuery("DESCRIBE * { ?y <is-a> ?v }",
                m::DescribeQuery(m::Describe(yv, {}, selectQueryMatcher2)));

    // DESCRIBE with FROM and FROM NAMED clauses.
    //
    // NOTE: The clauses are relevant *both* for the retrieval of the resources
    // to describe (the `Id`s matching `?y`), as well as for computing the
    // triples for each of these resources.
    using Graphs = ScanSpecificationAsTripleComponent::Graphs;
    Graphs expectedDefaultGraphs;
    Graphs expectedNamedGraphs;
    expectedDefaultGraphs.emplace({Iri("<default-graph>")});
    expectedNamedGraphs.emplace({Iri("<named-graph>")});
    parsedQuery::DatasetClauses expectedClauses{expectedDefaultGraphs,
                                                expectedNamedGraphs};
    expectQuery(
        "DESCRIBE <x> ?y <z> FROM <default-graph> FROM NAMED <named-graph>",
        m::DescribeQuery(m::Describe(xyz, expectedClauses, ::testing::_),
                         expectedDefaultGraphs, expectedNamedGraphs));
  }

  // Test the various places where warnings are added in a query.
  expectQuery("SELECT ?x {} GROUP BY ?x ORDER BY ?y",
              m::WarningsOfParsedQuery({"?x was used by GROUP BY",
                                        "?y was used in an ORDER BY clause"}));
  expectQuery("SELECT * { BIND (?a as ?b) }",
              m::WarningsOfParsedQuery(
                  {"?a was used in the expression of a BIND clause"}));
  expectQuery("SELECT * { } ORDER BY ?s",
              m::WarningsOfParsedQuery({"?s was used by ORDER BY"}));

  // Now test the same queries with exceptions instead of warnings.
  RuntimeParameters().set<"throw-on-unbound-variables">(true);
  expectQueryFails("SELECT ?x {} GROUP BY ?x",
                   contains("?x was used by GROUP BY"));
  expectQueryFails("SELECT * { BIND (?a as ?b) }",
                   contains("?a was used in the expression of a BIND clause"));
  expectQueryFails("SELECT * { } ORDER BY ?s",
                   contains("?s was used by ORDER BY"));

  // Revert this (global) setting to its original value.
  RuntimeParameters().set<"throw-on-unbound-variables">(false);
}

// _____________________________________________________________________________
TEST(SparqlParser, Exists) {
  auto expectBuiltInCall = ExpectCompleteParse<&Parser::builtInCall>{};

  // A matcher that matches the query `SELECT * { ?x <bar> ?foo }`, where the
  // FROM and FROM NAMED clauses can be specified as arguments.
  using Graphs = ScanSpecificationAsTripleComponent::Graphs;
  auto selectABarFooMatcher =
      [](Graphs defaultGraphs = std::nullopt, Graphs namedGraphs = std::nullopt,
         const std::optional<std::vector<std::string>>& variables =
             std::nullopt) {
        auto selectMatcher = variables.has_value()
                                 ? m::VariablesSelect(variables.value())
                                 : AllOf(m::AsteriskSelect(),
                                         m::VariablesSelect({"?a", "?foo"}));
        return m::SelectQuery(selectMatcher,
                              m::GraphPattern(m::Triples(
                                  {{Var{"?a"}, iri("<bar>"), Var{"?foo"}}})),
                              defaultGraphs, namedGraphs);
      };

  expectBuiltInCall("EXISTS {?a <bar> ?foo}",
                    m::Exists(selectABarFooMatcher()));
  expectBuiltInCall("NOT EXISTS {?a <bar> ?foo}",
                    m::NotExists(selectABarFooMatcher()));

  Graphs defaultGraphs{ad_utility::HashSet<TripleComponent>{iri("<blubb>")}};
  Graphs namedGraphs{ad_utility::HashSet<TripleComponent>{iri("<blabb>")}};

  // Now run the same tests, but with non-empty dataset clauses, that have to be
  // propagated to the `ParsedQuery` stored inside the `ExistsExpression`.
  ParsedQuery::DatasetClauses datasetClauses{defaultGraphs, namedGraphs};
  expectBuiltInCall("EXISTS {?a <bar> ?foo}",
                    m::Exists(selectABarFooMatcher()));
  expectBuiltInCall("NOT EXISTS {?a <bar> ?foo}",
                    m::NotExists(selectABarFooMatcher()));

  expectBuiltInCall("EXISTS {?a <bar> ?foo}",
                    m::Exists(selectABarFooMatcher(defaultGraphs, namedGraphs)),
                    datasetClauses);
  expectBuiltInCall(
      "NOT EXISTS {?a <bar> ?foo}",
      m::NotExists(selectABarFooMatcher(defaultGraphs, namedGraphs)),
      datasetClauses);

  auto expectGroupGraphPattern =
      ExpectCompleteParse<&Parser::groupGraphPattern>{};
  expectGroupGraphPattern("{ ?a ?b ?c . FILTER EXISTS {?a <bar> ?foo} }",
                          m::ContainsExistsFilter(selectABarFooMatcher(
                              std::nullopt, std::nullopt, {{"?a"}})));
  expectGroupGraphPattern("{ ?a ?b ?c . FILTER NOT EXISTS {?a <bar> ?foo} }",
                          m::ContainsExistsFilter(selectABarFooMatcher(
                              std::nullopt, std::nullopt, {{"?a"}})));
  expectGroupGraphPattern("{ FILTER EXISTS {?a <bar> ?foo} . ?a ?b ?c }",
                          m::ContainsExistsFilter(selectABarFooMatcher(
                              std::nullopt, std::nullopt, {{"?a"}})));
  expectGroupGraphPattern("{ FILTER NOT EXISTS {?a <bar> ?foo} . ?a ?b ?c }",
                          m::ContainsExistsFilter(selectABarFooMatcher(
                              std::nullopt, std::nullopt, {{"?a"}})));

  auto doesNotBindExists = [&]() {
    auto innerMatcher = m::ContainsExistsFilter(selectABarFooMatcher(
        std::nullopt, std::nullopt, std::vector<std::string>{}));
    using parsedQuery::GroupGraphPattern;
    return AD_FIELD(parsedQuery::GraphPattern, _graphPatterns,
                    ::testing::ElementsAre(
                        ::testing::VariantWith<GroupGraphPattern>(
                            AD_FIELD(GroupGraphPattern, _child, innerMatcher)),
                        ::testing::_));
  };

  expectGroupGraphPattern(
      "{ { FILTER EXISTS {?a <bar> ?foo} . ?d ?e ?f } . ?a ?b ?c }",
      doesNotBindExists());
  expectGroupGraphPattern(
      "{ { FILTER NOT EXISTS {?a <bar> ?foo} . ?d ?e ?f  } ?a ?b ?c }",
      doesNotBindExists());
}

TEST(SparqlParser, Quads) {
  auto expectQuads = ExpectCompleteParse<&Parser::quads>{defaultPrefixMap};
  auto expectQuadsFails = ExpectParseFails<&Parser::quads>{};
  auto Iri = [](std::string_view stringWithBrackets) {
    return TripleComponent::Iri::fromIriref(stringWithBrackets);
  };

  expectQuads("?a <b> <c>",
              m::Quads({{Var("?a"), ::Iri("<b>"), ::Iri("<c>")}}, {}));
  expectQuads("GRAPH <foo> { ?a <b> <c> }",
              m::Quads({}, {{Iri("<foo>"),
                             {{Var("?a"), ::Iri("<b>"), ::Iri("<c>")}}}}));
  expectQuads(
      "GRAPH <foo> { ?a <b> <c> } GRAPH <bar> { <d> <e> ?f }",
      m::Quads({},
               {{Iri("<foo>"), {{Var("?a"), ::Iri("<b>"), ::Iri("<c>")}}},
                {Iri("<bar>"), {{::Iri("<d>"), ::Iri("<e>"), Var("?f")}}}}));
  expectQuads(
      "GRAPH <foo> { ?a <b> <c> } . <d> <e> <f> . <g> <h> <i> ",
      m::Quads({{::Iri("<d>"), ::Iri("<e>"), ::Iri("<f>")},
                {::Iri("<g>"), ::Iri("<h>"), ::Iri("<i>")}},
               {{Iri("<foo>"), {{Var("?a"), ::Iri("<b>"), ::Iri("<c>")}}}}));
  expectQuads(
      "GRAPH <foo> { ?a <b> <c> } . <d> <e> <f> . <g> <h> <i> GRAPH <bar> { "
      "<j> <k> <l> }",
      m::Quads({{::Iri("<d>"), ::Iri("<e>"), ::Iri("<f>")},
                {::Iri("<g>"), ::Iri("<h>"), ::Iri("<i>")}},
               {{Iri("<foo>"), {{Var("?a"), ::Iri("<b>"), ::Iri("<c>")}}},
                {Iri("<bar>"), {{::Iri("<j>"), ::Iri("<k>"), ::Iri("<l>")}}}}));
}

TEST(SparqlParser, QuadData) {
  auto expectQuadData =
      ExpectCompleteParse<&Parser::quadData>{defaultPrefixMap};
  auto expectQuadDataFails = ExpectParseFails<&Parser::quadData>{};

  expectQuadData("{ <a> <b> <c> }",
                 Quads{{{Iri("<a>"), Iri("<b>"), Iri("<c>")}}, {}});
  expectQuadDataFails("{ <a> <b> ?c }");
  expectQuadDataFails("{ <a> <b> <c> . GRAPH <foo> { <d> ?e <f> } }");
  expectQuadDataFails("{ <a> <b> <c> . ?d <e> <f> } }");
  expectQuadDataFails("{ GRAPH ?foo { <a> <b> <c> } }");
}

TEST(SparqlParser, GraphOrDefault) {
  // Explicitly test this part, because all features that use it are not yet
  // supported.
  auto expectGraphOrDefault =
      ExpectCompleteParse<&Parser::graphOrDefault>{defaultPrefixMap};
  expectGraphOrDefault("DEFAULT", testing::VariantWith<DEFAULT>(testing::_));
  expectGraphOrDefault(
      "GRAPH <foo>",
      testing::VariantWith<GraphRef>(AD_PROPERTY(
          TripleComponent::Iri, toStringRepresentation, testing::Eq("<foo>"))));
}

TEST(SparqlParser, GraphRef) {
  auto expectGraphRefAll =
      ExpectCompleteParse<&Parser::graphRefAll>{defaultPrefixMap};
  expectGraphRefAll("DEFAULT", m::Variant<DEFAULT>());
  expectGraphRefAll("NAMED", m::Variant<NAMED>());
  expectGraphRefAll("ALL", m::Variant<ALL>());
  expectGraphRefAll("GRAPH <foo>", m::GraphRefIri("<foo>"));
}

TEST(SparqlParser, QuadsNotTriples) {
  auto expectQuadsNotTriples =
      ExpectCompleteParse<&Parser::quadsNotTriples>{defaultPrefixMap};
  auto expectQuadsNotTriplesFails =
      ExpectParseFails<&Parser::quadsNotTriples>{};
  const auto Iri = TripleComponent::Iri::fromIriref;
  auto GraphBlock = [](const ad_utility::sparql_types::VarOrIri& graph,
                       const ad_utility::sparql_types::Triples& triples)
      -> testing::Matcher<const Quads::GraphBlock&> {
    return testing::FieldsAre(testing::Eq(graph),
                              testing::ElementsAreArray(triples));
  };

  expectQuadsNotTriples(
      "GRAPH <foo> { <a> <b> <c> }",
      GraphBlock(Iri("<foo>"), {{::Iri("<a>"), ::Iri("<b>"), ::Iri("<c>")}}));
  expectQuadsNotTriples(
      "GRAPH ?f { <a> <b> <c> }",
      GraphBlock(Var("?f"), {{::Iri("<a>"), ::Iri("<b>"), ::Iri("<c>")}}));
  expectQuadsNotTriplesFails("GRAPH \"foo\" { <a> <b> <c> }");
  expectQuadsNotTriplesFails("GRAPH _:blankNode { <a> <b> <c> }");
}

TEST(SparqlParser, SourceSelector) {
  // This will be implemented soon, but for now we test the failure for the
  // coverage tool.
  auto expectSelector = ExpectCompleteParse<&Parser::sourceSelector>{};
  expectSelector("<x>", m::TripleComponentIri("<x>"));

  auto expectNamedGraph = ExpectCompleteParse<&Parser::namedGraphClause>{};
  expectNamedGraph("NAMED <x>", m::TripleComponentIri("<x>"));

  auto expectDefaultGraph = ExpectCompleteParse<&Parser::defaultGraphClause>{};
  expectDefaultGraph("<x>", m::TripleComponentIri("<x>"));
}

// _____________________________________________________________________________
TEST(ParserTest, propertyPathInCollection) {
  std::string query =
      "PREFIX : <http://example.org/>\n"
      "SELECT * { ?s ?p ([:p* 123] [^:r \"hello\"]) }";
  EncodedIriManager encodedIriManager;
  EXPECT_THAT(
      SparqlParser::parseQuery(&encodedIriManager, std::move(query)),
      m::SelectQuery(
          m::AsteriskSelect(),
          m::GraphPattern(m::Triples(
              {{Var{"?_QLever_internal_variable_2"},
                iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"),
                Var{"?_QLever_internal_variable_1"}},
               {Var{"?_QLever_internal_variable_2"},
                iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"),
                iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>")},
               {Var{"?_QLever_internal_variable_1"},
                PropertyPath::makeInverse(
                    PropertyPath::fromIri(iri("<http://example.org/r>"))),
                lit("\"hello\"")},
               {Var{"?_QLever_internal_variable_3"},
                iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"),
                Var{"?_QLever_internal_variable_0"}},
               {Var{"?_QLever_internal_variable_3"},
                iri("<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"),
                Var{"?_QLever_internal_variable_2"}},
               {Var{"?_QLever_internal_variable_0"},
                PropertyPath::makeWithLength(
                    PropertyPath::fromIri(iri("<http://example.org/p>")), 0,
                    std::numeric_limits<size_t>::max()),
                123},
               {Var{"?s"}, Var{"?p"}, Var{"?_QLever_internal_variable_3"}}}))));
}

TEST(SparqlParser, Datasets) {
  auto expectUpdate = ExpectCompleteParse<&Parser::update>{defaultPrefixMap};
  auto expectQuery = ExpectCompleteParse<&Parser::query>{defaultPrefixMap};
  auto expectAsk = ExpectCompleteParse<&Parser::askQuery>{defaultPrefixMap};
  auto expectConstruct =
      ExpectCompleteParse<&Parser::constructQuery>{defaultPrefixMap};
  auto expectDescribe =
      ExpectCompleteParse<&Parser::describeQuery>{defaultPrefixMap};
  auto Iri = [](std::string_view stringWithBrackets) {
    return TripleComponent::Iri::fromIriref(stringWithBrackets);
  };
  auto noGraph = std::monostate{};
  auto noGraphs = m::Graphs{};
  ScanSpecificationAsTripleComponent::Graphs datasets{{Iri("<g>")}};
  // Only checks `_filters` on the GraphPattern. We are not concerned with the
  // `_graphPatterns` here.
  auto filterGraphPattern = m::Filters(m::ExistsFilter(
      m::GraphPattern(m::Triples({{Var("?a"), Var{"?b"}, Var("?c")}})),
      datasets, noGraphs));
  // Check that datasets are propagated correctly into the different types of
  // operations.
  expectUpdate(
      "DELETE { ?x <b> <c> } USING <g> WHERE { ?x ?y ?z FILTER EXISTS {?a ?b "
      "?c} }",
      testing::ElementsAre(m::UpdateClause(
          m::GraphUpdate({{Var("?x"), Iri("<b>"), Iri("<c>"), noGraph}}, {}),
          filterGraphPattern, m::datasetClausesMatcher(datasets, noGraphs))));
  expectQuery("SELECT * FROM <g> WHERE { ?x ?y ?z FILTER EXISTS {?a ?b ?c} }",
              m::SelectQuery(m::AsteriskSelect(), filterGraphPattern, datasets,
                             noGraphs));
  expectAsk("ASK FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}",
            m::AskQuery(filterGraphPattern, datasets, noGraphs));
  expectConstruct(
      "CONSTRUCT {<a> <b> <c>} FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b?c}}",
      m::ConstructQuery(
          {std::array<GraphTerm, 3>{::Iri("<a>"), ::Iri("<b>"), ::Iri("<c>")}},
          filterGraphPattern, datasets, noGraphs));
  // See comment in visit function for `DescribeQueryContext`.
  expectDescribe(
      "Describe ?x FROM <g> { ?x ?y ?z FILTER EXISTS {?a ?b ?c}}",
      m::DescribeQuery(
          m::Describe({Var("?x")}, {datasets, {}},
                      m::SelectQuery(m::VariablesSelect({"?x"}, false, false),
                                     filterGraphPattern)),
          datasets, noGraphs));
}

// _____________________________________________________________________________
TEST(SparqlParser, EncodedIriManagerUsage) {
  using namespace sparqlParserTestHelpers;

  // Create a parse function that uses an `EncodedIriManager`.
  auto encodedIriManager = std::make_shared<EncodedIriManager>(
      std::vector<std::string>{"http://example.org/", "http://test.com/id/"});

  auto parseWithEncoding = [&](const std::string& input) {
    static ad_utility::BlankNodeManager blankNodeManager;
    return ParserAndVisitor{&blankNodeManager, encodedIriManager.get(), input}
        .parseTypesafe(&SparqlAutomaticParser::query);
  };

  auto encoded123 = TripleComponent{
      encodedIriManager->encode("<http://example.org/123>").value()};
  auto unencoded456 = PropertyPath::fromIri(
      TripleComponent::Iri::fromIriref("<http://example.org/456>"));
  auto encoded789 = TripleComponent{
      encodedIriManager->encode("<http://test.com/id/789>").value()};

  // Test that IRIs in SPARQL queries get encoded when they match prefixes. Note
  // that we currently only encode the subjects and predicates of triples
  // directly in the parser, as encoding predicates would require massive
  // changes to the `PropertyPath` and therefore the `QueryPlanner` class.
  {
    auto result = parseWithEncoding(
        "SELECT ?x WHERE { <http://example.org/123> <http://example.org/456> "
        "<http://test.com/id/789> }");
    EXPECT_THAT(
        result.resultOfParse_,
        m::SelectQuery(m::VariablesSelect({"?x"}),
                       m::GraphPattern(m::OrderedTriples(
                           {{{encoded123, unencoded456, encoded789}}}))));
  }

  {
    // CONSTRUCT WHERE syntax uses the same pattern for both template and WHERE
    // clause. Test that the encoding also works properly in these cases.
    std::string constructWhereQuery =
        "CONSTRUCT WHERE { <http://example.org/123> <http://example.org/456> "
        "<http://test.com/id/789> }";

    auto result = parseWithEncoding(constructWhereQuery);
    EXPECT_TRUE(result.remainingText_.empty())
        << "CONSTRUCT WHERE query should parse completely";

    EXPECT_THAT(
        result.resultOfParse_,
        m::ConstructQuery(
            {{Iri{"<http://example.org/123>"}, Iri{"<http://example.org/456>"},
              Iri{"<http://test.com/id/789>"}}},
            m::GraphPattern(m::OrderedTriples(
                {{{encoded123, unencoded456, encoded789}}}))));
  }
}
