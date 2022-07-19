//
// Created by johannes on 08.05.21.
//

#include <antlr4-runtime.h>
#include <gtest/gtest.h>

#include <iostream>
#include <type_traits>

#include "../../src/parser/sparqlParser/SparqlQleverVisitor.h"
#include "../src/parser/SparqlParserHelpers.h"
#include "../src/parser/data/Types.h"
#include "../src/parser/sparqlParser/generated/SparqlAutomaticLexer.h"
#include "../src/util/antlr/ANTLRErrorHandling.h"
#include "SparqlAntlrParserTestHelpers.h"

using namespace antlr4;
using namespace sparqlParserHelpers;

template <typename T>
void testNumericLiteral(const std::string& input, T target) {
  ParserAndVisitor p(input);
  auto literalContext = p.parser_.numericLiteral();
  auto result = p.visitor_.visitNumericLiteral(literalContext).as<T>();

  if constexpr (std::is_floating_point_v<T>) {
    ASSERT_FLOAT_EQ(target, result);
  } else {
    ASSERT_EQ(target, result);
  }
}

auto nil = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
auto first = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
auto rest = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
auto type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::SizeIs;

TEST(SparqlParser, NumericLiterals) {
  testNumericLiteral("3.0", 3.0);
  testNumericLiteral("3.0e2", 300.0);
  testNumericLiteral("3.0e-2", 0.030);
  testNumericLiteral("3", 3ull);
  testNumericLiteral("-3.0", -3.0);
  testNumericLiteral("-3", -3ll);

  // TODO<joka921> : Unit tests with random numbers
}

TEST(SparqlParser, Prefix) {
  {
    string s = "PREFIX wd: <www.wikidata.org/>";
    ParserAndVisitor p{s};
    auto context = p.parser_.prefixDecl();
    p.visitor_.visitPrefixDecl(context);
    const auto& m = p.visitor_.prefixMap();
    ASSERT_EQ(2ul, m.size());
    ASSERT_TRUE(m.at("wd") == "<www.wikidata.org/>");
    ASSERT_EQ(m.at(""), "<>");
  }
  {
    string s = "wd:bimbam";
    ParserAndVisitor p{s};
    auto& m = p.visitor_.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser_.pnameLn();
    auto result = p.visitor_.visitPnameLn(context).as<string>();
    ASSERT_EQ(result, "<www.wikidata.org/bimbam>");
  }
  {
    string s = "wd:";
    ParserAndVisitor p{s};
    auto& m = p.visitor_.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser_.pnameNs();
    auto result = p.visitor_.visitPnameNs(context).as<string>();
    ASSERT_EQ(result, "<www.wikidata.org/>");
  }
  {
    string s = "wd:bimbam";
    ParserAndVisitor p{s};
    auto& m = p.visitor_.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser_.prefixedName();
    auto result = p.visitor_.visitPrefixedName(context).as<string>();
    ASSERT_EQ(result, "<www.wikidata.org/bimbam>");
  }
  {
    string s = "<somethingsomething> <rest>";
    ParserAndVisitor p{s};
    auto& m = p.visitor_.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser_.iriref();
    auto result = p.visitor_.visitIriref(context).as<string>();
    auto sz = context->getText().size();

    ASSERT_EQ(result, "<somethingsomething>");
    ASSERT_EQ(sz, 20u);
  }
}

TEST(SparqlExpressionParser, First) {
  string s = "(5 * 5 ) bimbam";
  ParserAndVisitor p{s};
  auto context = p.parser_.expression();
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
  auto resultAsAny = p.visitor_.visitExpression(context);
  auto resultAsExpression =
      std::move(resultAsAny.as<sparqlExpression::SparqlExpression::Ptr>());

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
  ParserAndVisitor p{input};

  auto triples = p.parser_.constructQuery()
                     ->accept(&p.visitor_)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_THAT(triples, SizeIs(11));
  auto something =
      "<http://wallscope.co.uk/resource/olympics/medal/#something>";
  auto somethingElse =
      "<http://wallscope.co.uk/resource/olympics/medal/#somethingelse>";

  EXPECT_THAT(triples[0], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsVariable("?a"),        //
                                      IsBlankNode(true, "3")));

  EXPECT_THAT(triples[1], ElementsAre(IsBlankNode(true, "1"),  //
                                      IsIri(first),            //
                                      IsBlankNode(true, "2")));

  EXPECT_THAT(triples[2], ElementsAre(IsBlankNode(true, "1"),  //
                                      IsIri(rest),             //
                                      IsIri(nil)));

  EXPECT_THAT(triples[3], ElementsAre(IsBlankNode(true, "2"),  //
                                      IsIri(first),            //
                                      IsVariable("?c")));

  EXPECT_THAT(triples[4], ElementsAre(IsBlankNode(true, "2"),  //
                                      IsIri(rest),             //
                                      IsIri(nil)));

  EXPECT_THAT(triples[5], ElementsAre(IsBlankNode(true, "3"),  //
                                      IsIri(first),            //
                                      IsVariable("?b")));

  EXPECT_THAT(triples[6], ElementsAre(IsBlankNode(true, "3"),  //
                                      IsIri(rest),             //
                                      IsBlankNode(true, "1")));

  EXPECT_THAT(triples[7], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsVariable("?d"),        //
                                      IsBlankNode(true, "4")));

  EXPECT_THAT(triples[8], ElementsAre(IsBlankNode(true, "4"),  //
                                      IsVariable("?e"),        //
                                      IsBlankNode(true, "5")));

  EXPECT_THAT(triples[9], ElementsAre(IsBlankNode(true, "5"),  //
                                      IsVariable("?f"),        //
                                      IsVariable("?g")));

  EXPECT_THAT(triples[10], ElementsAre(IsIri(something),  //
                                       IsIri(type),       //
                                       IsIri(somethingElse)));
}

TEST(SparqlParser, GraphTermNumericLiteral) {
  string input = "1337";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.graphTerm()->accept(&p.visitor_).as<GraphTerm>();
  EXPECT_THAT(graphTerm, IsLiteral("1337"));
}

TEST(SparqlParser, GraphTermBooleanLiteral) {
  string input = "true";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.graphTerm()->accept(&p.visitor_).as<GraphTerm>();
  EXPECT_THAT(graphTerm, IsLiteral(input));
}

TEST(SparqlParser, GraphTermBlankNode) {
  string input = "[]";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.graphTerm()->accept(&p.visitor_).as<GraphTerm>();
  EXPECT_THAT(graphTerm, IsBlankNode(true, "0"));
}

TEST(SparqlParser, GraphTermIri) {
  string input = "<http://dummy-iri.com#fragment>";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.graphTerm()->accept(&p.visitor_).as<GraphTerm>();
  EXPECT_THAT(graphTerm, IsIri(input));
}

TEST(SparqlParser, GraphTermRdfLiteral) {
  string input = "\"abc\"";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.graphTerm()->accept(&p.visitor_).as<GraphTerm>();
  EXPECT_THAT(graphTerm, IsLiteral(input));
}

TEST(SparqlParser, GraphTermRdfNil) {
  string input = "()";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.graphTerm()->accept(&p.visitor_).as<GraphTerm>();
  EXPECT_THAT(graphTerm, IsIri(nil));
}

TEST(SparqlParser, RdfCollectionSingleVar) {
  string input = "( ?a )";
  ParserAndVisitor p{input};

  const auto [node, triples] = p.parser_.collection()
                                   ->accept(&p.visitor_)
                                   .as<ad_utility::sparql_types::Node>();

  EXPECT_THAT(node, IsBlankNode(true, "0"));

  ASSERT_THAT(triples, SizeIs(2));

  EXPECT_THAT(triples[0], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(first),            //
                                      IsVariable("?a")));

  EXPECT_THAT(triples[1], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(rest),             //
                                      IsIri(nil)));
}

TEST(SparqlParser, RdfCollectionTripleVar) {
  string input = "( ?a ?b ?c )";
  ParserAndVisitor p{input};

  const auto [node, triples] = p.parser_.collection()
                                   ->accept(&p.visitor_)
                                   .as<ad_utility::sparql_types::Node>();

  EXPECT_THAT(node, IsBlankNode(true, "2"));

  ASSERT_THAT(triples, SizeIs(6));

  EXPECT_THAT(triples[0], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(first),            //
                                      IsVariable("?c")));

  EXPECT_THAT(triples[1], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(rest),             //
                                      IsIri(nil)));

  EXPECT_THAT(triples[2], ElementsAre(IsBlankNode(true, "1"),  //
                                      IsIri(first),            //
                                      IsVariable("?b")));

  EXPECT_THAT(triples[3], ElementsAre(IsBlankNode(true, "1"),  //
                                      IsIri(rest),             //
                                      IsBlankNode(true, "0")));

  EXPECT_THAT(triples[4], ElementsAre(IsBlankNode(true, "2"),  //
                                      IsIri(first),            //
                                      IsVariable("?a")));

  EXPECT_THAT(triples[5], ElementsAre(IsBlankNode(true, "2"),  //
                                      IsIri(rest),             //
                                      IsBlankNode(true, "1")));
}

TEST(SparqlParser, BlankNodeAnonymous) {
  string input = "[ \t\r\n]";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.blankNode()->accept(&p.visitor_).as<BlankNode>();
  EXPECT_THAT(graphTerm, IsBlankNode(true, "0"));
}

TEST(SparqlParser, BlankNodeLabelled) {
  string input = "_:label123";
  ParserAndVisitor p{input};

  auto graphTerm = p.parser_.blankNode()->accept(&p.visitor_).as<BlankNode>();
  EXPECT_THAT(graphTerm, IsBlankNode(false, "label123"));
}

TEST(SparqlParser, ConstructTemplateEmpty) {
  string input = "{}";
  ParserAndVisitor p{input};

  auto triples = p.parser_.constructTemplate()
                     ->accept(&p.visitor_)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_THAT(triples, IsEmpty());
}

TEST(SparqlParser, ConstructTriplesSingletonWithTerminator) {
  string input = "?a ?b ?c .";
  ParserAndVisitor p{input};

  auto triples = p.parser_.constructTriples()
                     ->accept(&p.visitor_)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_THAT(triples, SizeIs(1));

  EXPECT_THAT(triples[0], ElementsAre(IsVariable("?a"),  //
                                      IsVariable("?b"),  //
                                      IsVariable("?c")));
}

TEST(SparqlParser, ConstructTriplesWithTerminator) {
  string input = "?a ?b ?c . ?d ?e ?f . ?g ?h ?i .";
  ParserAndVisitor p{input};

  auto triples = p.parser_.constructTriples()
                     ->accept(&p.visitor_)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_THAT(triples, SizeIs(3));

  EXPECT_THAT(triples[0], ElementsAre(IsVariable("?a"),  //
                                      IsVariable("?b"),  //
                                      IsVariable("?c")));

  EXPECT_THAT(triples[1], ElementsAre(IsVariable("?d"),  //
                                      IsVariable("?e"),  //
                                      IsVariable("?f")));

  EXPECT_THAT(triples[2], ElementsAre(IsVariable("?g"),  //
                                      IsVariable("?h"),  //
                                      IsVariable("?i")));
}

TEST(SparqlParser, TriplesSameSubjectVarOrTerm) {
  string input = "?a ?b ?c";
  ParserAndVisitor p{input};

  auto triples = p.parser_.constructTriples()
                     ->accept(&p.visitor_)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_THAT(triples, SizeIs(1));

  EXPECT_THAT(triples[0], ElementsAre(IsVariable("?a"),  //
                                      IsVariable("?b"),  //
                                      IsVariable("?c")));
}

TEST(SparqlParser, TriplesSameSubjectTriplesNodeWithPropertyList) {
  string input = "(?a) ?b ?c";
  ParserAndVisitor p{input};

  auto triples = p.parser_.triplesSameSubject()
                     ->accept(&p.visitor_)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_THAT(triples, SizeIs(3));

  EXPECT_THAT(triples[0], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(first),            //
                                      IsVariable("?a")));

  EXPECT_THAT(triples[1], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(rest),             //
                                      IsIri(nil)));

  EXPECT_THAT(triples[2], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsVariable("?b"),        //
                                      IsVariable("?c")));
}

TEST(SparqlParser, TriplesSameSubjectTriplesNodeEmptyPropertyList) {
  string input = "(?a)";
  ParserAndVisitor p{input};

  auto triples = p.parser_.triplesSameSubject()
                     ->accept(&p.visitor_)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_THAT(triples, SizeIs(2));

  EXPECT_THAT(triples[0], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(first),            //
                                      IsVariable("?a")));

  EXPECT_THAT(triples[1], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(rest),             //
                                      IsIri(nil)));
}

TEST(SparqlParser, PropertyList) {
  string input = "a ?a";
  ParserAndVisitor p{input};

  const auto [tuples, triples] =
      p.parser_.propertyList()
          ->accept(&p.visitor_)
          .as<ad_utility::sparql_types::PropertyList>();

  EXPECT_THAT(triples, IsEmpty());

  ASSERT_THAT(tuples, SizeIs(1));

  EXPECT_THAT(tuples[0], ElementsAre(IsIri(type), IsVariable("?a")));
}

TEST(SparqlParser, EmptyPropertyList) {
  ParserAndVisitor p{""};

  const auto [tuples, triples] =
      p.parser_.propertyList()
          ->accept(&p.visitor_)
          .as<ad_utility::sparql_types::PropertyList>();
  ASSERT_THAT(tuples, IsEmpty());
  ASSERT_THAT(triples, IsEmpty());
}

TEST(SparqlParser, PropertyListNotEmptySingletonWithTerminator) {
  string input = "a ?a ;";
  ParserAndVisitor p{input};

  const auto [tuples, triples] =
      p.parser_.propertyListNotEmpty()
          ->accept(&p.visitor_)
          .as<ad_utility::sparql_types::PropertyList>();
  EXPECT_THAT(triples, IsEmpty());

  ASSERT_THAT(tuples, SizeIs(1));

  EXPECT_THAT(tuples[0], ElementsAre(IsIri(type), IsVariable("?a")));
}

TEST(SparqlParser, PropertyListNotEmptyWithTerminator) {
  string input = "a ?a ; a ?b ; a ?c ;";
  ParserAndVisitor p{input};

  const auto [tuples, triples] =
      p.parser_.propertyListNotEmpty()
          ->accept(&p.visitor_)
          .as<ad_utility::sparql_types::PropertyList>();
  EXPECT_THAT(triples, IsEmpty());

  ASSERT_THAT(tuples, SizeIs(3));

  EXPECT_THAT(tuples[0], ElementsAre(IsIri(type), IsVariable("?a")));
  EXPECT_THAT(tuples[1], ElementsAre(IsIri(type), IsVariable("?b")));
  EXPECT_THAT(tuples[2], ElementsAre(IsIri(type), IsVariable("?c")));
}

TEST(SparqlParser, VerbA) {
  string input = "a";
  ParserAndVisitor p{input};

  auto varOrTerm = p.parser_.verb()->accept(&p.visitor_).as<VarOrTerm>();
  ASSERT_THAT(varOrTerm, IsIri(type));
}

TEST(SparqlParser, VerbVariable) {
  string input = "?a";
  ParserAndVisitor p{input};

  auto varOrTerm = p.parser_.verb()->accept(&p.visitor_).as<VarOrTerm>();
  ASSERT_THAT(varOrTerm, IsVariable("?a"));
}

TEST(SparqlParser, ObjectListSingleton) {
  string input = "?a";
  ParserAndVisitor p{input};

  const auto [objects, triples] =
      p.parser_.objectList()
          ->accept(&p.visitor_)
          .as<ad_utility::sparql_types::ObjectList>();
  EXPECT_THAT(triples, IsEmpty());

  ASSERT_THAT(objects, SizeIs(1));
  ASSERT_THAT(objects[0], IsVariable("?a"));
}

TEST(SparqlParser, ObjectList) {
  string input = "?a , ?b , ?c";
  ParserAndVisitor p{input};

  const auto [objects, triples] =
      p.parser_.objectList()
          ->accept(&p.visitor_)
          .as<ad_utility::sparql_types::ObjectList>();
  EXPECT_THAT(triples, IsEmpty());

  ASSERT_THAT(objects, SizeIs(3));
  ASSERT_THAT(objects[0], IsVariable("?a"));
  ASSERT_THAT(objects[1], IsVariable("?b"));
  ASSERT_THAT(objects[2], IsVariable("?c"));
}

TEST(SparqlParser, BlankNodePropertyList) {
  string input = "[ a ?a ; a ?b ; a ?c ]";
  ParserAndVisitor p{input};

  const auto [node, triples] = p.parser_.blankNodePropertyList()
                                   ->accept(&p.visitor_)
                                   .as<ad_utility::sparql_types::Node>();
  EXPECT_THAT(node, IsBlankNode(true, "0"));

  ASSERT_THAT(triples, SizeIs(3));

  EXPECT_THAT(triples[0], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(type),             //
                                      IsVariable("?a")));

  EXPECT_THAT(triples[1], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(type),             //
                                      IsVariable("?b")));

  EXPECT_THAT(triples[2], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(type),             //
                                      IsVariable("?c")));
}

TEST(SparqlParser, GraphNodeVarOrTerm) {
  string input = "?a";
  ParserAndVisitor p{input};

  const auto [node, triples] = p.parser_.graphNode()
                                   ->accept(&p.visitor_)
                                   .as<ad_utility::sparql_types::Node>();
  EXPECT_THAT(node, IsVariable("?a"));
  EXPECT_THAT(triples, IsEmpty());
}

TEST(SparqlParser, GraphNodeTriplesNode) {
  string input = "(?a)";
  ParserAndVisitor p{input};

  const auto [node, triples] = p.parser_.graphNode()
                                   ->accept(&p.visitor_)
                                   .as<ad_utility::sparql_types::Node>();
  EXPECT_THAT(node, IsBlankNode(true, "0"));

  ASSERT_THAT(triples, SizeIs(2));

  EXPECT_THAT(triples[0], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(first),            //
                                      IsVariable("?a")));

  EXPECT_THAT(triples[1], ElementsAre(IsBlankNode(true, "0"),  //
                                      IsIri(rest),             //
                                      IsIri(nil)));
}

TEST(SparqlParser, VarOrTermVariable) {
  string input = "?a";
  ParserAndVisitor p{input};

  auto varOrTerm = p.parser_.varOrTerm()->accept(&p.visitor_).as<VarOrTerm>();
  EXPECT_THAT(varOrTerm, IsVariable("?a"));
}

TEST(SparqlParser, VarOrTermGraphTerm) {
  string input = "()";
  ParserAndVisitor p{input};

  auto varOrTerm = p.parser_.varOrTerm()->accept(&p.visitor_).as<VarOrTerm>();
  EXPECT_THAT(varOrTerm, IsIri(nil));
}

TEST(SparqlParser, VarOrIriVariable) {
  string input = "?a";
  ParserAndVisitor p{input};

  auto varOrTerm = p.parser_.varOrIri()->accept(&p.visitor_).as<VarOrTerm>();
  EXPECT_THAT(varOrTerm, IsVariable("?a"));
}

TEST(SparqlParser, VarOrIriIri) {
  string input = "<http://testiri>";
  ParserAndVisitor p{input};

  auto varOrTerm = p.parser_.varOrIri()->accept(&p.visitor_).as<VarOrTerm>();
  EXPECT_THAT(varOrTerm, IsIri(input));
}

TEST(SparqlParser, VariableWithQuestionMark) {
  string input = "?variableName";
  ParserAndVisitor p{input};

  auto variable = p.parser_.var()->accept(&p.visitor_).as<Variable>();
  EXPECT_THAT(variable, IsVariable(input));
}

TEST(SparqlParser, VariableWithDollarSign) {
  string input = "$variableName";
  ParserAndVisitor p{input};

  auto variable = p.parser_.var()->accept(&p.visitor_).as<Variable>();
  EXPECT_THAT(variable, IsVariable("?variableName"));
}

TEST(SparqlParser, Bind) {
  {
    string input = "BIND (10 - 5 as ?a)";
    auto bindAndText = parseBind(input, {});

    expectCompleteParse(bindAndText, IsBind("?a", "10-5"));
  }

  {
    string input = "bInD (?age - 10 As ?s)";
    auto bindAndText = parseBind(input, {});

    expectCompleteParse(bindAndText, IsBind("?s", "?age-10"));
  }
}

TEST(SparqlParser, Integer) {
  {
    string input = "1931";
    ParserAndVisitor p{input};

    unsigned long long result =
        p.parser_.integer()->accept(&p.visitor_).as<unsigned long long>();
    EXPECT_EQ(result, 1931ull);
  }

  {
    string input = "0";
    ParserAndVisitor p{input};

    unsigned long long result =
        p.parser_.integer()->accept(&p.visitor_).as<unsigned long long>();
    EXPECT_EQ(result, 0ull);
  }

  {
    string input = "18446744073709551615";
    ParserAndVisitor p{input};

    unsigned long long result =
        p.parser_.integer()->accept(&p.visitor_).as<unsigned long long>();
    EXPECT_EQ(result, 18446744073709551615ull);
  }

  {
    string input = "18446744073709551616";
    ParserAndVisitor p{input};

    EXPECT_THROW(p.parser_.integer()->accept(&p.visitor_), ParseException);
  }

  {
    string input = "10000000000000000000000000000000000000000";
    ParserAndVisitor p{input};

    EXPECT_THROW(p.parser_.integer()->accept(&p.visitor_), ParseException);
  }

  {
    string input = "-1";
    ParserAndVisitor p{input};

    EXPECT_THROW(p.parser_.integer()->accept(&p.visitor_),
                 antlr4::ParseCancellationException);
  }
}

TEST(SparqlParser, LimitOffsetClause) {
  {
    string input = "LIMIT 10";

    auto limitOffset = parseLimitOffsetClause(input, {});

    expectCompleteParse(limitOffset, IsLimitOffset(10ull, 1ull, 0ull));
  }

  {
    string input = "OFFSET 31 LIMIT 12 TEXTLIMIT 14";

    auto limitOffset = parseLimitOffsetClause(input, {});

    expectCompleteParse(limitOffset, IsLimitOffset(12ull, 14ull, 31ull));
  }

  {
    string input = "textlimit 999";

    auto limitOffset = parseLimitOffsetClause(input, {});

    expectCompleteParse(
        limitOffset,
        IsLimitOffset(std::numeric_limits<uint64_t>::max(), 999ull, 0ull));
  }

  {
    string input = "LIMIT      999";

    auto limitOffset = parseLimitOffsetClause(input, {});

    expectCompleteParse(limitOffset, IsLimitOffset(999ull, 1ull, 0ull));
  }

  {
    string input = "OFFSET 43";

    auto limitOffset = parseLimitOffsetClause(input, {});

    expectCompleteParse(
        limitOffset,
        IsLimitOffset(std::numeric_limits<uint64_t>::max(), 1ull, 43ull));
  }

  {
    string input = "TEXTLIMIT 43 LIMIT 19";

    auto limitOffset = parseLimitOffsetClause(input, {});

    expectCompleteParse(limitOffset, IsLimitOffset(19ull, 43ull, 0ull));
  }

  {
    string input = "LIMIT20";

    // parse* catches antlr4::ParseCancellationException and throws a
    // std::runtime_error so this has to be checked instead.
    EXPECT_THROW(parseLimitOffsetClause(input, {}), std::runtime_error);
  }

  {
    string input = "Limit 10 TEXTLIMIT 20 offset 0 Limit 20";

    auto limitOffset = parseLimitOffsetClause(input, {});

    EXPECT_THAT(limitOffset.resultOfParse_, IsLimitOffset(10ull, 20ull, 0ull));
    EXPECT_EQ(limitOffset.remainingText_, "Limit 20");
  }
}

TEST(SparqlParser, OrderCondition) {
  auto parseOrderCondition = [](const std::string& input) {
    ParserAndVisitor p{input};
    return p.parse<OrderKey>(input, "order condition",
                             &SparqlAutomaticParser::orderCondition);
  };
  auto expectParseVariable = [&parseOrderCondition](const string& input,
                                                    const string& variable,
                                                    bool isDescending) {
    expectCompleteParse(parseOrderCondition(input),
                        IsVariableOrderKey(variable, isDescending));
  };
  auto expectParseExpression = [&parseOrderCondition](const string& input,
                                                      const string& expression,
                                                      bool isDescending) {
    expectCompleteParse(parseOrderCondition(input),
                        IsExpressionOrderKey(expression, isDescending));
  };
  // var
  expectParseVariable("?test", "?test", false);
  // brackettedExpression
  expectParseVariable("DESC (?foo)", "?foo", true);
  expectParseVariable("ASC (?bar)", "?bar", false);
  expectParseExpression("ASC(?test - 5)", "?test-5", false);
  expectParseExpression("DESC (10 || (5 && ?foo))", "10||(5&&?foo)", true);
  // constraint
  expectParseExpression("(5 - ?mehr)", "5-?mehr", false);
  expectParseExpression("SUM(?i)", "SUM(?i)", false);
  EXPECT_THROW(parseOrderCondition("ASC SCORE(?i)"), ParseException);
}

TEST(SparqlParser, OrderClause) {
  {
    string input = "ORDER BY ?test DESC(?foo - 5)";
    auto orderKeys = parseOrderClause(input, {});
    expectCompleteArrayParse(orderKeys, IsVariableOrderKey("?test", false),
                             IsExpressionOrderKey("?foo-5", true));
  }
}

TEST(SparqlParser, GroupCondition) {
  auto parseGroupCondition = [](const std::string& input) {
    ParserAndVisitor p{input};
    return p.parse<GroupKey>(input, "group condition",
                             &SparqlAutomaticParser::groupCondition);
  };
  auto expectParseVariable = [&parseGroupCondition](const string& input,
                                                    const string& variable) {
    expectCompleteParse(parseGroupCondition(input),
                        IsVariableGroupKey(variable));
  };
  auto expectParseExpression =
      [&parseGroupCondition](const string& input, const string& expression) {
        expectCompleteParse(parseGroupCondition(input),
                            IsExpressionGroupKey(expression));
      };
  auto expectParseExpressionAlias =
      [&parseGroupCondition](const string& input, const string& expression,
                             const string& variable) {
        expectCompleteParse(parseGroupCondition(input),
                            IsAliasGroupKey(expression, variable));
      };
  // variable
  expectParseVariable("?test", "?test");
  // expression without binding
  expectParseExpression("(?test)", "?test");
  // expression with binding
  expectParseExpressionAlias("(?test AS ?mehr)", "?test", "?mehr");
  // builtInCall
  expectParseExpression("COUNT(?test)", "COUNT(?test)");
  // functionCall
  expectParseExpression(
      "<http://www.opengis.net/def/function/geosparql/latitude> (?test)",
      "<http://www.opengis.net/def/function/geosparql/latitude>(?test)");
}

TEST(SparqlParser, GroupClause) {
  {
    string input = "GROUP BY ?test (?foo - 10 as ?bar) COUNT(?baz)";
    auto groupings = parseGroupClause(input, {});
    expectCompleteArrayParse(groupings, IsVariableGroupKey("?test"),
                             IsAliasGroupKey("?foo-10", "?bar"),
                             IsExpressionGroupKey("COUNT(?baz)"));
  }
}

namespace {
template <typename Exception = ParseException>
void expectDataBlockFails(const string& input) {
  EXPECT_THROW(parseDataBlock(input, {}), Exception) << input;
}
}  // namespace

TEST(SparqlParser, DataBlock) {
  auto expectDataBlock = [](const string& input,
                            const vector<string>& expectedVars,
                            const vector<vector<string>>& expectedVals) {
    expectCompleteParse(parseDataBlock(input, {}),
                        IsValues(expectedVars, expectedVals));
  };
  expectDataBlock("?test { \"foo\" }", {"?test"}, {{"\"foo\""}});
  // These are not implemented yet in dataBlockValue
  // (numericLiteral/booleanLiteral)
  expectDataBlockFails("?test { true }");
  expectDataBlockFails("?test { 10.0 }");
  expectDataBlockFails("?test { UNDEF }");
  expectDataBlock(R"(?foo { "baz" "bar" })", {"?foo"},
                  {{"\"baz\""}, {"\"bar\""}});
  expectDataBlockFails(R"(( ) { })");
  expectDataBlockFails(R"(?foo { })");
  expectDataBlockFails(R"(( ?foo ) { })");
  expectDataBlockFails(R"(( ?foo ?bar ) { (<foo>) (<bar>) })");
  expectDataBlock(R"(( ?foo ?bar ) { (<foo> <bar>) })", {"?foo", "?bar"},
                  {{"<foo>", "<bar>"}});
  expectDataBlock(R"(( ?foo ?bar ) { (<foo> "m") ("1" <bar>) })",
                  {"?foo", "?bar"}, {{"<foo>", "\"m\""}, {"\"1\"", "<bar>"}});
  expectDataBlock(R"(( ?foo ?bar ) { (<foo> "m") (<bar> <e>) ("1" "f") })",
                  {"?foo", "?bar"},
                  {{"<foo>", "\"m\""}, {"<bar>", "<e>"}, {"\"1\"", "\"f\""}});
  expectDataBlockFails(R"(( ) { (<foo>) })");
}

namespace {
template <typename Exception = ParseException>
void expectInlineDataFails(const string& input) {
  EXPECT_THROW(parseInlineData(input, {}), Exception) << input;
}
}  // namespace

TEST(SparqlParser, InlineData) {
  auto expectInlineData = [](const string& input,
                             const vector<string>& expectedVars,
                             const vector<vector<string>>& expectedVals) {
    expectCompleteParse(parseInlineData(input, {}),
                        IsValues(expectedVars, expectedVals));
  };
  expectInlineData("VALUES ?test { \"foo\" }", {"?test"}, {{"\"foo\""}});
  // There must always be a block present for InlineData
  expectInlineDataFails<std::runtime_error>("");
}

TEST(SparqlParser, propertyPaths) {
  // Test all the base cases.
  // "a" is a special case. It is a valid PropertyPath.
  // It is short for "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".
  expectCompleteParse(
      parseVerbPathOrSimple("a", {}),
      IsPropertyPath(PropertyPath::fromIri(
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")));
  EXPECT_THROW(parseVerbPathOrSimple("b", {}), std::runtime_error);
  expectCompleteParse(
      parseVerbPathOrSimple("test:foo",
                            {{"test", "<http://www.example.com/>"}}),
      IsPropertyPath(PropertyPath::fromIri("<http://www.example.com/foo>")));
  expectCompleteParse(parseVerbPathOrSimple("?bar", {}),
                      IsPropertyPath(PropertyPath::fromIri("?bar")));
  expectCompleteParse(
      parseVerbPathOrSimple(":", {{"", "<http://www.example.com/>"}}),
      IsPropertyPath(PropertyPath::fromIri("<http://www.example.com/>")));
  expectCompleteParse(
      parseVerbPathOrSimple("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                            {}),
      IsPropertyPath(PropertyPath::fromIri(
          "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")));
  // Test the basic combinators / | (...) + * ?.
  expectCompleteParse(
      parseVerbPathOrSimple("a:a / a:b", {{"a", "<http://www.example.com/>"}}),
      IsPropertyPath(PropertyPath::makeSequence(
          {PropertyPath::fromIri("<http://www.example.com/a>"),
           PropertyPath::fromIri("<http://www.example.com/b>")})));
  expectCompleteParse(
      parseVerbPathOrSimple("a:a | a:b", {{"a", "<http://www.example.com/>"}}),
      IsPropertyPath(PropertyPath::makeAlternative(
          {PropertyPath::fromIri("<http://www.example.com/a>"),
           PropertyPath::fromIri("<http://www.example.com/b>")})));
  expectCompleteParse(
      parseVerbPathOrSimple("(a:a)", {{"a", "<http://www.example.com/>"}}),
      IsPropertyPath(
          PropertyPath(PropertyPath::fromIri("<http://www.example.com/a>"))));
  expectCompleteParse(
      parseVerbPathOrSimple("a:a+", {{"a", "<http://www.example.com/>"}}),
      IsPropertyPath(PropertyPath::makeTransitiveMin(
          {PropertyPath::fromIri("<http://www.example.com/a>")}, 1)));
  {
    PropertyPath expected = PropertyPath::makeTransitiveMax(
        {PropertyPath::fromIri("<http://www.example.com/a>")}, 1);
    expected._can_be_null = true;
    expectCompleteParse(
        parseVerbPathOrSimple("a:a?", {{"a", "<http://www.example.com/>"}}),
        IsPropertyPath(expected));
  }
  {
    PropertyPath expected = PropertyPath::makeTransitive(
        {PropertyPath::fromIri("<http://www.example.com/a>")});
    expected._can_be_null = true;
    expectCompleteParse(
        parseVerbPathOrSimple("a:a*", {{"a", "<http://www.example.com/>"}}),
        IsPropertyPath(expected));
  }
  // Test a bigger example that contains everything.
  {
    PropertyPath expected = PropertyPath::makeAlternative(
        {PropertyPath::makeSequence({
             PropertyPath::fromIri("<http://www.example.com/a/a>"),
             PropertyPath::makeTransitive(
                 {PropertyPath::fromIri("<http://www.example.com/b/b>")}),
         }),
         PropertyPath::fromIri("<http://www.example.com/c/c>"),
         PropertyPath::makeTransitiveMin(
             {PropertyPath::makeSequence(
                 {PropertyPath::fromIri("<http://www.example.com/a/a>"),
                  PropertyPath::fromIri("<http://www.example.com/b/b>"),
                  PropertyPath::fromIri("<a/b/c>")})},
             1)});
    expected.computeCanBeNull();
    expected._can_be_null = false;
    expectCompleteParse(
        parseVerbPathOrSimple("a:a/b:b*|c:c|(a:a/b:b/<a/b/c>)+",
                              {{"a", "<http://www.example.com/a/>"},
                               {"b", "<http://www.example.com/b/>"},
                               {"c", "<http://www.example.com/c/>"}}),
        IsPropertyPath(expected));
  }
}

namespace {
template <typename Exception = ParseException>
void expectSelectFails(const string& input) {
  EXPECT_THROW(parseSelectClause(input, {}), Exception) << input;
}
}  // namespace

TEST(SparqlParser, SelectClause) {
  auto expectVariablesSelect = [](const string& input,
                                  std::vector<std::string> variables,
                                  bool distinct = false, bool reduced = false) {
    expectCompleteParse(
        parseSelectClause(input, {}),
        IsVariablesSelect(distinct, reduced, std::move(variables)));
  };
  using Alias = std::pair<string, string>;
  auto expectSelect = [](const string& input,
                         std::vector<std::variant<Variable, Alias>> selection,
                         bool distinct = false, bool reduced = false) {
    expectCompleteParse(parseSelectClause(input, {}),
                        IsSelect(distinct, reduced, std::move(selection)));
  };

  expectCompleteParse(parseSelectClause("SELECT *", {}),
                      IsAsteriskSelect(false, false));
  expectCompleteParse(parseSelectClause("SELECT DISTINCT *", {}),
                      IsAsteriskSelect(true, false));
  expectCompleteParse(parseSelectClause("SELECT REDUCED *", {}),
                      IsAsteriskSelect(false, true));
  expectSelectFails("SELECT DISTINCT REDUCED *");
  expectSelectFails<std::runtime_error>(
      "SELECT");  // Lexer throws the error instead of the parser
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
