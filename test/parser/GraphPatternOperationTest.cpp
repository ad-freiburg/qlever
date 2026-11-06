// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "rdfTypes/Variable.h"

namespace {

using V = Variable;

// Test that `collectAllContainedVariables` collects the variables of all
// triples of a `BasicGraphPattern`.
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

// Test that `Bind::getConstant` returns the constant of a `BIND` of an IRI, a
// literal, or a defined `Id`, and `std::nullopt` for any other expression.
TEST(GraphPatternOperationTest, BindGetConstant) {
  using namespace sparqlExpression;
  using ::testing::Eq;
  using ::testing::Optional;
  auto bind = [](std::shared_ptr<SparqlExpression> expression) {
    return parsedQuery::Bind{
        SparqlExpressionPimpl{std::move(expression), "constant"}, V{"?x"}};
  };

  // An IRI, a literal, and a defined `Id` are constants.
  auto iri = ad_utility::triple_component::Iri::fromIriref("<a>");
  auto literal =
      ad_utility::triple_component::Literal::fromStringRepresentation("\"a\"");
  auto id = Id::makeFromInt(42);
  EXPECT_THAT(bind(std::make_shared<IriExpression>(iri)).getConstant(),
              Optional(Eq(TripleComponent{iri})));
  EXPECT_THAT(
      bind(std::make_shared<StringLiteralExpression>(literal)).getConstant(),
      Optional(Eq(TripleComponent{literal})));
  EXPECT_THAT(bind(std::make_shared<IdExpression>(id)).getConstant(),
              Optional(Eq(TripleComponent{id})));

  // An undefined `Id` and a variable are not.
  EXPECT_EQ(
      bind(std::make_shared<IdExpression>(Id::makeUndefined())).getConstant(),
      std::nullopt);
  EXPECT_EQ(bind(std::make_shared<VariableExpression>(V{"?y"})).getConstant(),
            std::nullopt);
}

}  // namespace
