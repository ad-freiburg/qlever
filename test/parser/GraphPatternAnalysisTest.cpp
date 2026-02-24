// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "parser/GraphPatternAnalysis.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"

using namespace graphPatternAnalysis;

// _____________________________________________________________________________
TEST(BasicGraphPatternsInvariantToTest, Bind) {
  BasicGraphPatternsInvariantTo invariantTo{{Variable{"?x"}, Variable{"?y"}}};

  // Test that BIND is invariant when its target variable is not in our set.
  parsedQuery::Bind bind1{
      sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
          Variable{"?a"}),
      Variable{"?z"}};
  EXPECT_TRUE(invariantTo(bind1));

  // Test that BIND is not invariant when its target variable is in our set.
  parsedQuery::Bind bind2{
      sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
          Variable{"?a"}),
      Variable{"?x"}};

  EXPECT_FALSE(invariantTo(bind2));

  // Test that BIND is invariant when we have no variables to check.
  BasicGraphPatternsInvariantTo invariantTo2{{}};
  parsedQuery::Bind bind{
      sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
          Variable{"?a"}),
      Variable{"?x"}};
  EXPECT_TRUE(invariantTo2(bind));
}

// _____________________________________________________________________________
TEST(BasicGraphPatternsInvariantToTest, Values) {
  BasicGraphPatternsInvariantTo invariantTo{{Variable{"?x"}, Variable{"?y"}}};

  // Test VALUES with exactly one row and no variable overlap.
  parsedQuery::Values values;
  values._inlineValues._variables = {Variable{"?a"}, Variable{"?b"}};
  values._inlineValues._values = {
      {TripleComponent::Iri::fromIriref("<value1>"),
       TripleComponent::Iri::fromIriref("<value2>")}};
  EXPECT_TRUE(invariantTo(values));

  // Test VALUES with one row but with variable overlap.
  parsedQuery::Values values2;
  values2._inlineValues._variables = {Variable{"?x"}, Variable{"?b"}};
  values2._inlineValues._values = {
      {TripleComponent::Iri::fromIriref("<value1>"),
       TripleComponent::Iri::fromIriref("<value2>")}};
  EXPECT_FALSE(invariantTo(values2));

  // Test VALUES with multiple rows (not invariant even without variable
  // overlap).
  parsedQuery::Values values3;
  values3._inlineValues._variables = {Variable{"?a"}};
  values3._inlineValues._values = {
      {TripleComponent::Iri::fromIriref("<value1>")},
      {TripleComponent::Iri::fromIriref("<value2>")}};
  EXPECT_FALSE(invariantTo(values3));

  // Test VALUES with zero rows (not invariant).
  parsedQuery::Values values4;
  values4._inlineValues._variables = {Variable{"?a"}};
  values4._inlineValues._values = {};
  EXPECT_FALSE(invariantTo(values4));
}

// _____________________________________________________________________________
TEST(BasicGraphPatternsInvariantToTest, NotInvariant) {
  // Test the base case: is not invariant.
  BasicGraphPatternsInvariantTo invariantTo{{Variable{"?x"}}};

  SparqlTripleSimple example{Variable{"?s"}, Variable{"?p"}, Variable{"?o"}};
  auto triple = SparqlTriple::fromSimple(example);
  parsedQuery::BasicGraphPattern bgp{{triple}};
  EXPECT_FALSE(invariantTo(bgp));

  parsedQuery::Optional optional{parsedQuery::GraphPattern{}};
  EXPECT_FALSE(invariantTo(optional));
}
