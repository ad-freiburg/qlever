//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/AllocatorTestHelpers.h"
#include "util/GTestHelpers.h"

using namespace sparqlExpression;

TEST(SparqlExpressionTypes, expressionResultCopyIfNotVector) {
  ExpressionResult a = Id::makeFromDouble(42.3);
  ExpressionResult b = Id::makeFromDouble(1.0);
  ASSERT_NO_THROW(b = copyExpressionResultIfNotVector(a));
  ASSERT_EQ(a, b);

  a = VectorWithMemoryLimit<Id>(ad_utility::testing::makeAllocator());
  ASSERT_ANY_THROW(b = copyExpressionResultIfNotVector(a));
  AD_EXPECT_THROW_WITH_MESSAGE(
      b = copyExpressionResultIfNotVector(a),
      ::testing::StartsWith(
          "Tried to copy an expression result that is a vector."));
}

TEST(SparqlExpressionTypes, printIdOrString) {
  std::stringstream str;
  IdOrString idOrString{Id::makeUndefined()};
  PrintTo(idOrString, &str);
  ASSERT_EQ(str.str(), "Undefined:Undefined");
  idOrString = "bimm";
  // Clear the stringstream.
  str.str({});
  PrintTo(idOrString, &str);
  ASSERT_EQ(str.str(), "bimm");
}
