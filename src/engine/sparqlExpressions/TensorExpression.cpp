// Copyright 2021 - 2025
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "backports/type_traits.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Constants.h"

namespace sparqlExpression {
namespace detail {
  NARY_EXPRESSION(
      TensorFromListExpression, 1,
      FV<TensorValueGetter, TensorValueGetter>);
  NARY_EXPRESSION(
      TensorFromStringExpression, 1,
      FV<TensorFromStringValueGetter, TensorValueGetter>);
  NARY_EXPRESSION(
      TensorFromRdfTermExpression, 1,
      FV<TensorFromRdfTermValueGetter, TensorValueGetter>);
  NARY_EXPRESSION(
      TensorAddExpression, 2,
      FV<TensorAddValueGetter, TensorValueGetter>);
  NARY_EXPRESSION(
      TensorSubtractExpression, 2,
      FV<TensorSubtractValueGetter, TensorValueGetter>);
  NARY_EXPRESSION(
      TensorDotProductExpression, 2,
      FV<NumericIdWrapper<TensorDotProductValueGetter, true>, TensorValueGetter>);
  NARY_EXPRESSION(
      TensorNormExpression, 1,
      FV<NumericIdWrapper<TensorNormValueGetter, true>, TensorValueGetter>);
  NARY_EXPRESSION(
      TensorCosineSimilarityExpression, 1,
      FV<NumericIdWrapper<TensorCosineSimilarityValueGetter, true>, TensorValueGetter>);

}  // namespace detail

using namespace detail;
using Expr = SparqlExpression::Ptr;

Expr makeTensorFromListExpression(Expr child) {
  return std::make_unique<TensorFromListExpression>(std::move(child));
}

Expr makeTensorFromStringExpression(Expr child) {
  return std::make_unique<TensorFromStringExpression>(std::move(child));
}

Expr makeTensorFromRdfTermExpression(Expr child) {
  return std::make_unique<TensorFromRdfTermExpression>(std::move(child));
}

Expr makeTensorAddExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorAddExpression>(std::move(child1), std::move(child2));
}
Expr makeTensorSubtractExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorSubtractExpression>(std::move(child1), std::move(child2));
}
Expr makeTensorDotProductExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorDotProductExpression>(std::move(child1), std::move(child2));
}
Expr makeTensorNormExpression(Expr child) {
  return std::make_unique<TensorNormExpression>(std::move(child));
}
Expr makeTensorCosineSimilarityExpression(Expr child) {
  return std::make_unique<TensorCosineSimilarityExpression>(std::move(child));
}
