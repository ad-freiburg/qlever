// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

// The tensor functions `dtf:*` (see `TENSOR_FUNCTION_PREFIX`) and the
// factories for the tensor aggregates `dta:*` (see `TensorAggregate.h`).
//
// Semantics: All functions return UNDEF if one of the arguments is not a
// (valid) tensor, or if the shapes of the two arguments of a binary function
// differ. The elementwise arithmetic follows IEEE semantics (e.g. `x / 0`), the
// numeric functions return UNDEF for a NaN or infinite result.

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "engine/tensor/aggregate/TensorAggregate.h"
#include "rdfTypes/TensorData.h"

namespace sparqlExpression {
namespace detail {
using ad_utility::TensorData;
using OptTensor = std::optional<TensorData>;

// dtf:tensor(?string): Parse the (string value of the) argument as the JSON
// representation of a tensor.
struct TensorFromStringImpl {
  IdOrLiteralOrIri operator()(const std::optional<std::string>& str) const {
    if (str.has_value()) {
      if (auto tensor = TensorData::tryParseFromString(str.value())) {
        return tensor.value().toLiteral();
      }
    }
    return Id::makeUndefined();
  }
};
NARY_EXPRESSION(TensorFromStringExpression, 1,
                FV<TensorFromStringImpl, StringValueGetter>);

// Return true iff both tensors are defined and have the same shape.
inline bool compatible(const OptTensor& tensor1, const OptTensor& tensor2) {
  return tensor1.has_value() && tensor2.has_value() &&
         tensor1->shape() == tensor2->shape();
}

// Elementwise binary arithmetic. The first argument is taken by value, so that
// its storage is reused for the result if the value getter produced a
// temporary (no additional allocation).
template <auto InPlaceOperation>
struct BinaryTensorFunctionImpl {
  IdOrLiteralOrIri operator()(OptTensor tensor1,
                              const OptTensor& tensor2) const {
    if (!compatible(tensor1, tensor2)) {
      return Id::makeUndefined();
    }
    std::invoke(InPlaceOperation, tensor1.value(), tensor2.value());
    return tensor1.value().toLiteral();
  }
};

// Binary functions with a numeric result. Return NaN (which is converted to
// UNDEF by the `NumericIdWrapper` below) for incompatible arguments.
template <auto Function>
struct BinaryNumericTensorFunctionImpl {
  double operator()(const OptTensor& tensor1, const OptTensor& tensor2) const {
    if (!compatible(tensor1, tensor2)) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return static_cast<double>(
        std::invoke(Function, tensor1.value(), tensor2.value()));
  }
};

template <auto Function>
using NumericTensorFunction =
    NumericIdWrapper<BinaryNumericTensorFunctionImpl<Function>, true>;

NARY_EXPRESSION(
    TensorAddExpression, 2,
    FV<BinaryTensorFunctionImpl<&TensorData::addInPlace>, TensorValueGetter>);
NARY_EXPRESSION(TensorSubtractExpression, 2,
                FV<BinaryTensorFunctionImpl<&TensorData::subtractInPlace>,
                   TensorValueGetter>);
NARY_EXPRESSION(TensorMultiplyExpression, 2,
                FV<BinaryTensorFunctionImpl<&TensorData::multiplyInPlace>,
                   TensorValueGetter>);
NARY_EXPRESSION(TensorDivideExpression, 2,
                FV<BinaryTensorFunctionImpl<&TensorData::divideInPlace>,
                   TensorValueGetter>);

NARY_EXPRESSION(TensorDotProductExpression, 2,
                FV<NumericTensorFunction<&TensorData::dot>, TensorValueGetter>);
NARY_EXPRESSION(
    TensorEuclideanDistanceExpression, 2,
    FV<NumericTensorFunction<&TensorData::euclideanDistance>,
       TensorValueGetter>);
NARY_EXPRESSION(
    TensorCosineSimilarityExpression, 2,
    FV<NumericTensorFunction<&TensorData::cosineSimilarity>,
       TensorValueGetter>);

// dtf:norm2(?t): The euclidean (L2) norm.
struct NormTensorFunctionImpl {
  double operator()(const OptTensor& tensor) const {
    return tensor.has_value()
               ? static_cast<double>(TensorData::norm(tensor.value()))
               : std::numeric_limits<double>::quiet_NaN();
  }
};
NARY_EXPRESSION(
    TensorNormExpression, 1,
    FV<NumericIdWrapper<NormTensorFunctionImpl, true>, TensorValueGetter>);

}  // namespace detail

using namespace detail;
using Expr = SparqlExpression::Ptr;

// _____________________________________________________________________________
Expr makeTensorFromStringExpression(Expr child) {
  return std::make_unique<TensorFromStringExpression>(std::move(child));
}
Expr makeTensorAddExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorAddExpression>(std::move(child1),
                                               std::move(child2));
}
Expr makeTensorSubtractExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorSubtractExpression>(std::move(child1),
                                                    std::move(child2));
}
Expr makeTensorMultiplyExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorMultiplyExpression>(std::move(child1),
                                                    std::move(child2));
}
Expr makeTensorDivideExpression(Expr child1, Expr child2) {
  return std::make_unique<TensorDivideExpression>(std::move(child1),
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

// _____________________________________________________________________________
Expr makeTensorSumExpr(Expr child, bool distinct) {
  return std::make_unique<TensorSumExpression>(distinct, std::move(child));
}
Expr makeTensorAvgExpr(Expr child, bool distinct) {
  return std::make_unique<TensorAvgExpression>(distinct, std::move(child));
}
Expr makeTensorVarExpr(Expr child, bool distinct) {
  return std::make_unique<TensorVarExpression>(distinct, std::move(child));
}
Expr makeTensorStdExpr(Expr child, bool distinct) {
  return std::make_unique<TensorStdExpression>(distinct, std::move(child));
}
}  // namespace sparqlExpression
