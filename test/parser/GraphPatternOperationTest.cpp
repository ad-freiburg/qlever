// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "rdfTypes/Variable.h"

namespace {

using V = Variable;

// _____________________________________________________________________________
TEST(GraphPatternOperationTest, BasicPatternContainedVars) {
  SparqlTripleSimple example1{V{"?s"}, V{"?p"}, V{"?o"}};
  SparqlTripleSimple example2{
      ad_utility::triple_component::Iri::fromIriref("<s>"),
      ad_utility::triple_component::Iri::fromIriref("<p>"), V{"?o2"}};

  auto triple1 = SparqlTriple::fromSimple(example1);
  auto triple2 = SparqlTriple::fromSimple(example2);

  parsedQuery::BasicGraphPattern bgp{{triple1, triple2}};

  ad_utility::HashSet<V> vars;
  bgp.collectAllContainedVariables(vars);
  auto expectedVarsMatcher = ::testing::UnorderedElementsAreArray(
      std::vector{V{"?s"}, V{"?p"}, V{"?o"}, V{"?o2"}});
  EXPECT_THAT(vars, expectedVarsMatcher);
}

}  // namespace
