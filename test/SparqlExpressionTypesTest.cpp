//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/AllocatorTestHelpers.h"
#include "util/GTestHelpers.h"

using namespace sparqlExpression;

TEST(SparqlExpressionTypes, expressionResult) {
  ExpressionResult a = Id::makeFromDouble(42.3);
  ExpressionResult b = Id::makeFromDouble(1.0);
  ASSERT_NO_THROW(b = copyExpressionResult(a));
  ASSERT_EQ(a, b);

  a = VectorWithMemoryLimit<Id>(ad_utility::testing::makeAllocator());
  a.emplace<Id>(Id::makeFromDouble(42.0));
  ASSERT_NO_THROW(b = copyExpressionResult(a));
  ASSERT_EQ(a, b);

  auto c = VectorWithMemoryLimit<Id>(ad_utility::testing::makeAllocator());
  c.emplace_back(Id::makeFromDouble(42.0));
  ASSERT_EQ(c.size(), 1);
  copyExpressionResult(static_cast<ExpressionResult>(std::move(c)));
  ASSERT_TRUE(c.empty());
}

TEST(SparqlExpressionTypes, printIdOrString) {
  using namespace ad_utility::triple_component;
  std::stringstream str;

  IdOrLiteralOrIri idOrString{Id::makeUndefined()};
  PrintTo(idOrString, &str);
  ASSERT_EQ(str.str(), "U:0");
  idOrString = LiteralOrIri::literalWithoutQuotes("bimm");
  // Clear the stringstream.
  str.str({});
  PrintTo(idOrString, &str);
  ASSERT_EQ(str.str(), "\"bimm\"");
}
