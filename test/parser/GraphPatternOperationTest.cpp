// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"

// _____________________________________________________________________________
TEST(GraphPatternOperationTest, BasicPatternContainedVars) {
  SparqlTripleSimple example1{Variable{"?s"}, Variable{"?p"}, Variable{"?o"}};
  SparqlTripleSimple example2{
      ad_utility::triple_component::Iri::fromIriref("<s>"),
      ad_utility::triple_component::Iri::fromIriref("<p>"), Variable{"?o2"}};

  auto triple1 = SparqlTriple::fromSimple(example1);
  auto triple2 = SparqlTriple::fromSimple(example2);

  parsedQuery::BasicGraphPattern bgp{{triple1, triple2}};

  ad_utility::HashSet<Variable> vars;
  bgp.collectAllContainedVariables(vars);
  auto expectedVarsMatcher = ::testing::UnorderedElementsAre(
      ::testing::Eq(Variable{"?s"}), ::testing::Eq(Variable{"?p"}),
      ::testing::Eq(Variable{"?o"}), ::testing::Eq(Variable{"?o2"}));
  EXPECT_THAT(vars, expectedVarsMatcher);

  parsedQuery::Bind bind{
      sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
          Variable{"?x"}),
      Variable{"?y"}};
  std::vector<parsedQuery::GraphPatternOperation> graphPatterns{bind, bgp};
  EXPECT_THAT(getVariablesPresentInFirstBasicGraphPattern(graphPatterns),
              expectedVarsMatcher);
}
