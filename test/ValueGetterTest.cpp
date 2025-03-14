//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author:

#include <gtest/gtest.h>

#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

TEST(LiteralValueGetter, OperatorWithId) {
  sparqlExpression::TestContext testContext{};
  sparqlExpression::detail::LiteralValueGetter literalValueGetter;
  Id idLiteral = testContext().alpha; //literal with no datatype
  auto literal = literalValueGetter(idLiteral, &testContext.context);
  ASSERT_TRUE(literal.has_value());
  ASSERT_EQ(literal.value().getContent(), "alpha");
  //TODO: Test Ids that hold a iri.
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
