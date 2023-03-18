//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;

TEST(SparqlExpressionTypes, expressionResultCopyIfNotVector) {
  ExpressionResult a = 42.3;
  ExpressionResult b = 1.0;
  ASSERT_NO_THROW(b = copyExpressionResultIfNotVector(a));
  ASSERT_EQ(a, b);

  a = VectorWithMemoryLimit<double>(ad_utility::testing::makeAllocator());
  ASSERT_ANY_THROW(b = copyExpressionResultIfNotVector(a));
  AD_EXPECT_THROW_WITH_MESSAGE(
      b = copyExpressionResultIfNotVector(a),
      ::testing::StartsWith(
          "Tried to copy an expression result that is a vector."));
}
