//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author:

#include <gtest/gtest.h>

#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

// Common things used in multiple tests.
namespace {
const std::string ttl = R"(
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
<x> <y> "anXsdString"^^xsd:string,
        "someType"^^<someType>,
        "noType".
  )";
struct TestContextWithGivenTTl {
  std::string turtleInput;
  QueryExecutionContext* qec = ad_utility::testing::getQec(turtleInput);
  VariableToColumnMap varToColMap;
  LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{
      *qec,
      varToColMap,
      table,
      qec->getAllocator(),
      localVocab,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      sparqlExpression::EvaluationContext::TimePoint::max()};
  std::function<Id(const std::string&)> getId =
      ad_utility::testing::makeGetId(qec->getIndex());
  TestContextWithGivenTTl(std::string turtle)
      : turtleInput{std::move(turtle)} {}
};

}  // namespace

TEST(LiteralValueGetter, OperatorWithId) {
  TestContextWithGivenTTl testContext{ttl};
  sparqlExpression::detail::LiteralValueGetter literalValueGetter;

  Id idLiteral = testContext.getId("\"noType\"");  // literal with no datatype
  auto literal = literalValueGetter(idLiteral, &testContext.context);
  ASSERT_TRUE(literal.has_value());
  ASSERT_EQ(asStringViewUnsafe(literal.value().getContent()), "noType");
  // TODO<Annika> Also check that the datatype is empty.

  idLiteral =
      testContext.getId("\"someType\"^^<someType>");  // literal datatype
  literal = literalValueGetter(idLiteral, &testContext.context);
  ASSERT_TRUE(literal.has_value());
  ASSERT_EQ(asStringViewUnsafe(literal.value().getContent()), "someType");
  // TODO<Annika> Also check the datatype.
  // TODO<Annika> The above tests are very repetitive, write helper functions or
  // matchers.

  // TODO: Test Ids that hold a iri.
}

TEST(LiteralValueGetter, OperatorWithLiteralOrIri) {
  // TODO: Test LiteralOrIris that hold a literal and that hold a iri.
}

TEST(LiteralValueGetterWithXsdStringFilter, OperatorWithId) {
  // TODO: Test Ids that hold literals with 'xsd:string' datatype, literals with
  // no datatype and literals with other datatypes/ iris (should not be
  // returned)
}

TEST(LiteralValueGetterWithXsdStringFilter, OperatorWithLiteralOrIri) {
  // TODO: Test LiteralOrIris that hold literals with 'xsd:string' datatype,
  // literals with no datatype and literals with other datatypes/iris (should
  // not be returned)
}
