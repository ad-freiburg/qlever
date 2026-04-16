// Copyright 2026
// Benedikt Kantz, TUGRAZ/UNIPD

#include "backports/type_traits.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Constants.h"
using TensorData = ad_utility::TensorData;
namespace sparqlExpression {
namespace detail {
NARY_EXPRESSION(TensorFromListExpression, 1,
                FV<TensorValueGetter, TensorValueGetter>);

struct TensorFromStringValueGetter {
  IdOrLiteralOrIri operator()(std::optional<std::string> str) const {
    if (str.has_value()) {
      return TensorData::parseFromString(str.value()).toLiteral();
    }
    return Id::makeUndefined();
  }
};
NARY_EXPRESSION(TensorFromStringExpression, 1,
                FV<TensorFromStringValueGetter, StringValueGetter>);

template <auto BinaryTensorFunction>
struct BinaryFloatTensorFunctionImpl {
  float operator()(std::optional<TensorData> tensor1,
                   std::optional<TensorData> tensor2) const {
    if (!tensor1.has_value() || !tensor2.has_value()) {
      // set to NaN
      return std::numeric_limits<float>::quiet_NaN();
    }
    return std::invoke(BinaryTensorFunction, tensor1.value(), tensor2.value());
  }
};
template <auto BinaryTensorFunction>
struct BinaryTensorFunctionImpl {
  IdOrLiteralOrIri operator()(std::optional<TensorData> tensor1,
                              std::optional<TensorData> tensor2) const {
    if (!tensor1.has_value() || !tensor2.has_value()) {
      return Id::makeUndefined();
    }
    return std::invoke(BinaryTensorFunction, tensor1.value(), tensor2.value())
        .toLiteral();
  }
};

NARY_EXPRESSION(
    TensorAddExpression, 2,
    FV<BinaryTensorFunctionImpl<&TensorData::add>, TensorValueGetter>);

NARY_EXPRESSION(
    TensorSubtractExpression, 2,
    FV<BinaryTensorFunctionImpl<&TensorData::subtract>, TensorValueGetter>);

NARY_EXPRESSION(
    TensorDotProductExpression, 2,
    FV<NumericIdWrapper<BinaryFloatTensorFunctionImpl<&TensorData::dot>, true>,
       TensorValueGetter>);
NARY_EXPRESSION(
    TensorEuclideanDistanceExpression, 2,
    FV<NumericIdWrapper<BinaryFloatTensorFunctionImpl<&TensorData::euclideanDistance>, true>,
       TensorValueGetter>);
NARY_EXPRESSION(
    TensorCosineSimilarityExpression, 2,
    FV<NumericIdWrapper<
           BinaryFloatTensorFunctionImpl<&TensorData::cosineSimilarity>, true>,
       TensorValueGetter>)

struct NormTensorFunctionImpl {
  float operator()(std::optional<TensorData> tensor1) const {
    return tensor1.has_value() ? TensorData::norm(tensor1.value())
                               : std::numeric_limits<float>::quiet_NaN();
  }
};

NARY_EXPRESSION(
    TensorNormExpression, 1,
    FV<NumericIdWrapper<NormTensorFunctionImpl, true>, TensorValueGetter>);

}  // namespace detail

using namespace detail;
using Expr = SparqlExpression::Ptr;

// Expr makeTensorFromListExpression(Expr child) {
//   return std::make_unique<TensorFromListExpression>(std::move(child));
// }

Expr makeTensorFromStringExpression(Expr child) {
  return std::make_unique<TensorFromStringExpression>(std::move(child));
}

// Expr makeTensorFromRdfTermExpression(Expr child) {
//   return std::make_unique<TensorFromRdfTermExpression>(std::move(child));
// }

Expr makeTensorAddExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorAddExpression>(std::move(child1),
                                               std::move(child2));
}
Expr makeTensorSubtractExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorSubtractExpression>(std::move(child1),
                                                    std::move(child2));
}
Expr makeTensorDotProductExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorDotProductExpression>(std::move(child1),
                                                      std::move(child2));
}
Expr makeTensorEuclideanDistanceExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorEuclideanDistanceExpression>(std::move(child1),
                                                            std::move(child2));
}
Expr makeTensorCosineSimilarityExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorCosineSimilarityExpression>(std::move(child1),
                                                            std::move(child2));
}
Expr makeTensorNormExpression(Expr child) {
  return std::make_unique<TensorNormExpression>(std::move(child));
}
}  // namespace sparqlExpression
