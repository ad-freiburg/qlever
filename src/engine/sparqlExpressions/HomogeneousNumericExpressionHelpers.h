// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "engine/CallFixedSize.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"
#include "util/TypeIdentity.h"

namespace sparqlExpression::detail::homogeneousNumeric {

// Helpers for evaluating binary numeric expressions whose operands are
// homogeneous with respect to their numeric datatype.
//
// The generic numeric value getters return variants and therefore require
// variant dispatch for every result row. For operands that contain only
// integers or only doubles, these helpers first classify the complete operand
// and then evaluate the expression using the corresponding primitive C++
// types. Mixed or non-numeric operands are handled by the generic
// `BinaryExpression` path.

// The numeric type shared by all elements of an operand. `Other` represents
// mixed numeric types as well as non-numeric values.
enum class HomogeneousNumericType {
  Int,
  Double,
  Other,
};

// Map homogeneous numeric datatypes to their primitive C++ types.
inline constexpr auto homogeneousNumericTypeMap =
    std::tuple{std::pair{HomogeneousNumericType::Int,
                         ad_utility::use_type_identity::ti<int64_t>},
               std::pair{HomogeneousNumericType::Double,
                         ad_utility::use_type_identity::ti<double>}};

// Whether a value getter can participate in the homogeneous numeric fast path.
template <typename ValueGetter>
inline constexpr bool supportsHomogeneousNumericFastPath =
    ad_utility::SameAsAny<ValueGetter, NumericValueGetter,
                          NumericOrDateValueGetter>;

// Return whether `Operand` has a representation that can be inspected directly
// by the homogeneous numeric classifier. Currently this is restricted to
// `ValueId` constants and vectors whose elements are `ValueId`s.
template <typename Operand>
constexpr bool supportsHomogeneousNumericOperand() {
  using OperandType = std::decay_t<Operand>;

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return true;
  } else if constexpr (isVectorResult<OperandType>) {
    using ElementType = ql::ranges::range_value_t<OperandType>;
    return ad_utility::isSimilar<ElementType, ValueId>;
  } else {
    return false;
  }
}

// Classify all values in a span as integer, double, or other.
inline HomogeneousNumericType classifyNumericOperand(
    ql::span<const ValueId> values, EvaluationContext* context) {
  // An empty vector has no meaningful homogeneous numeric type.
  if (values.empty()) {
    return HomogeneousNumericType::Other;
  }

  bool allInt = true;
  bool allDouble = true;

  // Deliberately scan the complete span without an early exit. Using the
  // breakable `chunkedForLoop` for failed classifications was benchmarked and
  // caused a significant regression for homogeneous inputs.
  ad_utility::chunkedForLoop<1000>(
      0, values.size(),
      [&](size_t i) {
        const auto type = values[i].getDatatype();

        allInt &= type == Datatype::Int;
        allDouble &= type == Datatype::Double;
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  if (allInt) {
    return HomogeneousNumericType::Int;
  }

  if (allDouble) {
    return HomogeneousNumericType::Double;
  }

  return HomogeneousNumericType::Other;
}

// Classify a single `ValueId` by its numeric datatype.
inline HomogeneousNumericType classifyNumericOperand(ValueId value) {
  switch (value.getDatatype()) {
    case Datatype::Int:
      return HomogeneousNumericType::Int;
    case Datatype::Double:
      return HomogeneousNumericType::Double;
    default:
      return HomogeneousNumericType::Other;
  }
}

// Classify a supported operand representation. Constants are classified
// directly, while vector-like operands are viewed as spans of `ValueId`.
template <typename Operand>
inline HomogeneousNumericType classifyNumericOperand(
    const Operand& operand, EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  static_assert(supportsHomogeneousNumericOperand<Operand>(),
                "Unsupported operand representation for homogeneous numeric "
                "classification");

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return classifyNumericOperand(operand);
  } else {
    return classifyNumericOperand(
        ql::span<const ValueId>{operand.data(), operand.size()}, context);
  }
}

// Classify all operands by their homogeneous numeric datatype.
template <typename... Operands>
inline auto classifyNumericOperands(EvaluationContext* context,
                                    const Operands&... operands) {
  return std::array<HomogeneousNumericType, sizeof...(Operands)>{
      classifyNumericOperand(operands, context)...};
}

// Extract the primitive numeric value from a `ValueId` whose datatype was
// already established by homogeneous classification.
template <typename NumericType>
NumericType getHomogeneousNumericValue(ValueId value) {
  if constexpr (ql::concepts::same_as<NumericType, int64_t>) {
    return value.getInt();
  } else if constexpr (ql::concepts::same_as<NumericType, double>) {
    return value.getDouble();
  } else {
    static_assert(ad_utility::alwaysFalse<NumericType>,
                  "Unsupported homogeneous numeric type");
  }
}

// Return an indexed getter that extracts already-classified primitive numeric
// values from either a vector-like operand or a constant.
template <typename NumericType, typename Operand>
auto makeHomogeneousNumericGetter(const Operand& operand) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    return [&operand](size_t i) {
      return getHomogeneousNumericValue<NumericType>(operand[i]);
    };
  } else {
    static_assert(ad_utility::isSimilar<OperandType, ValueId>,
                  "Homogeneous numeric fast path currently supports "
                  "ValueId constants");

    const auto value = getHomogeneousNumericValue<NumericType>(operand);

    return [value]([[maybe_unused]] size_t index) { return value; };
  }
}

// Select the function used for evaluation on primitive numeric types.
// By default, use the original function directly for primitive numeric inputs.
template <typename Function>
struct RawNumericFunction {
  using type = Function;
};

// Unwrap `MakeNumericExpression` so that the fast path operates directly on
// primitive numeric types while preserving numeric `ValueId` construction.
template <typename Function, bool NanOrInfToUndef>
struct RawNumericFunction<MakeNumericExpression<Function, NanOrInfToUndef>> {
  using type = NumericIdWrapper<Function, NanOrInfToUndef>;
};

// The function type used by the homogeneous numeric evaluation loop.
template <typename Function>
using RawNumericFunctionT = typename RawNumericFunction<Function>::type;

// Map a homogeneous numeric type to the corresponding compile-time index used
// for dispatching to the primitive C++ numeric type.
template <size_t I = 0>
inline int homogeneousNumericTypeToIndex(HomogeneousNumericType type) {
  if constexpr (I == std::tuple_size_v<decltype(homogeneousNumericTypeMap)>) {
    AD_FAIL();
  } else {
    if (std::get<I>(homogeneousNumericTypeMap).first == type) {
      return static_cast<int>(I);
    }
    return homogeneousNumericTypeToIndex<I + 1>(type);
  }
}

// Dispatch runtime homogeneous numeric types to compile-time primitive numeric
// types and invoke the supplied function with those types.
template <size_t N, typename Function>
decltype(auto) dispatchHomogeneousNumericTypes(
    const std::array<HomogeneousNumericType, N>& types, Function&& function) {
  std::array<int, N> indices;

  ql::ranges::transform(types, indices.begin(),
                        [](HomogeneousNumericType type) {
                          return homogeneousNumericTypeToIndex(type);
                        });

  constexpr int maxIndex =
      std::tuple_size_v<decltype(homogeneousNumericTypeMap)> - 1;

  return ad_utility::callFixedSizeVi<maxIndex>(
      indices, [function = AD_FWD(function)](auto... typeIndices) {
        return function(
            std::get<decltype(typeIndices)::value>(homogeneousNumericTypeMap)
                .second...);
      });
}

// Check that a vector-like operand has the expected size. Constant operands
// don't require a size check.
template <typename Operand>
void checkHomogeneousNumericOperandSize(const Operand& operand,
                                        EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;
  if constexpr (isVectorResult<OperandType>) {
    AD_CORRECTNESS_CHECK(operand.size() == context->size());
  }
}

// Apply the size check to all operands of a homogeneous numeric operation.
template <typename... Operands>
void checkHomogeneousNumericOperandSizes(
    const std::tuple<Operands...>& operands, EvaluationContext* context) {
  std::apply(
      [context](const auto&... operand) {
        (checkHomogeneousNumericOperandSize(operand, context), ...);
      },
      operands);
}

// Evaluate a homogeneous numeric operation when at least one operand is
// non-constant. For the currently supported operand representations, this
// means that at least one operand is vector-like.
template <typename Function, typename... NumericTypes, typename... Operands>
ExpressionResult evaluateHomogeneousNumericOperation(
    std::tuple<Operands...> operands, EvaluationContext* context) {
  static_assert(sizeof...(NumericTypes) == sizeof...(Operands));
  static_assert((... || isVectorResult<std::decay_t<Operands>>),
                "At least one operand must be vector-like");

  // Check the size of every vector-like operand.
  checkHomogeneousNumericOperandSizes(operands, context);

  using FastFunction = RawNumericFunctionT<Function>;
  FastFunction function;

  // Create one primitive numeric getter for every operand.
  auto getters = std::apply(
      [](const auto&... operand) {
        return std::tuple{
            makeHomogeneousNumericGetter<NumericTypes>(operand)...};
      },
      operands);

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(context->size());

  ad_utility::chunkedForLoop<1000>(
      0, context->size(),
      [&](size_t i) {
        std::apply(
            [&](const auto&... getter) {
              result.push_back(function(getter(i)...));
            },
            getters);
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return result;
}

}  // namespace sparqlExpression::detail::homogeneousNumeric

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H
