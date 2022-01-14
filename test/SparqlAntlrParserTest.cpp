//
// Created by johannes on 08.05.21.
//

#include <antlr4-runtime.h>
#include <gtest/gtest.h>

#include <iostream>
#include <type_traits>

#include "../../src/parser/sparqlParser/SparqlQleverVisitor.h"
#include "../src/parser/data/Types.h"
#include "../src/parser/sparqlParser/generated/SparqlAutomaticLexer.h"
#include "SparqlAntlrParserTestHelpers.h"

using namespace antlr4;

struct ParserAndVisitor {
 private:
  string input;
  ANTLRInputStream stream{input};
  SparqlAutomaticLexer lexer{&stream};
  CommonTokenStream tokens{&lexer};

 public:
  SparqlAutomaticParser parser{&tokens};
  SparqlQleverVisitor visitor;
  explicit ParserAndVisitor(string toParse) : input{std::move(toParse)} {}
};

template <typename T>
void testNumericLiteral(const std::string& input, T target) {
  ParserAndVisitor p(input);
  auto literalContext = p.parser.numericLiteral();
  auto result = p.visitor.visitNumericLiteral(literalContext).as<T>();

  if constexpr (std::is_floating_point_v<T>) {
    ASSERT_FLOAT_EQ(target, result);
  } else {
    ASSERT_EQ(target, result);
  }
}

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
    auto context = p.parser.prefixDecl();
    p.visitor.visitPrefixDecl(context);
    const auto& m = p.visitor.prefixMap();
    ASSERT_EQ(2ul, m.size());
    ASSERT_TRUE(m.at("wd") == "<www.wikidata.org/>");
    ASSERT_EQ(m.at(""), "<>");
  }
  {
    string s = "wd:bimbam";
    ParserAndVisitor p{s};
    auto& m = p.visitor.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser.pnameLn();
    auto result = p.visitor.visitPnameLn(context).as<string>();
    ASSERT_EQ(result, "<www.wikidata.org/bimbam>");
  }
  {
    string s = "wd:";
    ParserAndVisitor p{s};
    auto& m = p.visitor.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser.pnameNs();
    auto result = p.visitor.visitPnameNs(context).as<string>();
    ASSERT_EQ(result, "<www.wikidata.org/>");
  }
  {
    string s = "wd:bimbam";
    ParserAndVisitor p{s};
    auto& m = p.visitor.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser.prefixedName();
    auto result = p.visitor.visitPrefixedName(context).as<string>();
    ASSERT_EQ(result, "<www.wikidata.org/bimbam>");
  }
  {
    string s = "<somethingsomething> <rest>";
    ParserAndVisitor p{s};
    auto& m = p.visitor.prefixMap();
    m["wd"] = "<www.wikidata.org/>";

    auto context = p.parser.iriref();
    auto result = p.visitor.visitIriref(context).as<string>();
    auto sz = context->getText().size();

    ASSERT_EQ(result, "<somethingsomething>");
    ASSERT_EQ(sz, 20u);
  }
}

TEST(SparqlExpressionParser, First) {
  string s = "(5 * 5 ) bimbam";
  ParserAndVisitor p{s};
  auto context = p.parser.expression();
  // This is an example on how to access a certain parsed substring.
  /*
  LOG(INFO) << context->getText() << std::endl;
  LOG(INFO) << p.parser.getTokenStream()
                   ->getTokenSource()
                   ->getInputStream()
                   ->toString()
            << std::endl;
  LOG(INFO) << p.parser.getCurrentToken()->getStartIndex() << std::endl;
   */
  auto resultAsAny = p.visitor.visitExpression(context);
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

  auto triples = p.parser.constructQuery()
                     ->accept(&p.visitor)
                     .as<ad_utility::sparql_types::Triples>();
  ASSERT_EQ(triples.size(), 11u);
  auto nil = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
  auto first = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
  auto rest = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
  auto something =
      "<http://wallscope.co.uk/resource/olympics/medal/#something>";
  auto type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
  auto somethingElse =
      "<http://wallscope.co.uk/resource/olympics/medal/#somethingelse>";

  using ::testing::ElementsAre;

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
