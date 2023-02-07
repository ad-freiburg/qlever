// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

// Nary expressions

#ifndef QLEVER_NARYEXPRESSION_H
#define QLEVER_NARYEXPRESSION_H

#include "../../util/GeoSparqlHelpers.h"
#include "./SparqlExpression.h"
#include "./SparqlExpressionGenerators.h"

namespace sparqlExpression {
namespace detail {

/**
 * @brief A sequence of binary operations, which is executed from left to
right, for example (?a or ?b), (?a and ?b ?and ?c), (3 * 5 / 7 * ?x) . Different
operations in the same expression, like (?a + ?b - ?c) are
supported by passing in multiple operations as the `BinaryOperations` template
parameter and by choosing the corresponding operation for each sub-expression
via the `tags` argument to the `create` function (see there).
 * @tparam ValueGetter A callable type that takes a
 * double/int64_t/Bool/string/StrongIdWithResultType and extracts the actual
input to the operation. Can be
 * used to perform type conversions before the actual operation.
 * @tparam BinaryOperations The actual binary operations. They must be callable
with
 * the result types of the `ValueGetter`.
 */

template <typename NaryOperation>
requires(isOperation<NaryOperation>) class NaryExpression
    : public SparqlExpression {
 public:
  static constexpr size_t N = NaryOperation::N;
  using Children = std::array<SparqlExpression::Ptr, N>;

  /// The actual constructor. It is private; to construct an object of this
  /// class, use the static `create` function (which uses this private
  /// constructor).
  NaryExpression(Children&& children);

 public:
  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override;

  // _________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

 private:
  /// Evaluate the `naryOperation` on the `operands` using the `context`.
  static inline auto evaluateOnChildrenOperands =
      []<SingleExpressionResult... Operands>(
          NaryOperation naryOperation, EvaluationContext* context,
          Operands&&... operands) -> ExpressionResult {
    // Perform a more efficient calculation if a specialized function exists
    // that matches all operands.
    if (isAnySpecializedFunctionPossible(naryOperation._specializedFunctions,
                                         operands...)) {
      auto optionalResult = evaluateOnSpecializedFunctionsIfPossible(
          naryOperation._specializedFunctions,
          std::forward<Operands>(operands)...);
      AD_CONTRACT_CHECK(optionalResult);
      return std::move(optionalResult.value());
    }

    // We have to first determine the number of results we will produce.
    auto targetSize = getResultSize(*context, operands...);

    // The result is a constant iff all the results are constants.
    constexpr static bool resultIsConstant =
        (... && isConstantResult<Operands>);

    // The generator for the result of the operation.
    auto resultGenerator = applyOperation(targetSize, naryOperation, context,
                                          std::move(operands)...);

    // Compute the result.
    using ResultType = typename decltype(resultGenerator)::value_type;
    VectorWithMemoryLimit<ResultType> result{context->_allocator};
    result.reserve(targetSize);
    for (auto&& singleResult : resultGenerator) {
      result.push_back(std::forward<decltype(singleResult)>(singleResult));
    }

    if constexpr (resultIsConstant) {
      AD_CONTRACT_CHECK(result.size() == 1);
      return std::move(result[0]);
    } else {
      return result;
    }
  };
  Children _children;
};

/// Three short aliases to make the instantiations more readable.
template <typename... T>
using FV = FunctionAndValueGetters<T...>;

template <size_t N, typename X, typename... T>
using NARY = NaryExpression<Operation<N, X, T...>>;

// True iff all types `Ts` are `SetOfIntervals`.
inline auto areAllSetOfIntervals = []<typename... Ts>(const Ts&...) constexpr {
  return (... && ad_utility::isSimilar<Ts, ad_utility::SetOfIntervals>);
};
template <typename F>
using SET = SpecializedFunction<F, decltype(areAllSetOfIntervals)>;

using ad_utility::SetOfIntervals;

/// The types for the concrete MultiBinaryExpressions and UnaryExpressions.
inline auto orLambda = [](bool a, bool b) -> Bool { return a || b; };
using OrExpression =
    NARY<2, FV<decltype(orLambda), EffectiveBooleanValueGetter>,
         SET<SetOfIntervals::Union>>;

inline auto andLambda = [](bool a, bool b) -> Bool { return a && b; };
using AndExpression =
    NARY<2, FV<decltype(andLambda), EffectiveBooleanValueGetter>,
         SET<SetOfIntervals::Intersection>>;

/// Unary Negation
inline auto unaryNegate = [](bool a) -> Bool { return !a; };
using UnaryNegateExpression =
    NARY<1, FV<decltype(unaryNegate), EffectiveBooleanValueGetter>,
         SET<SetOfIntervals::Complement>>;

/// Unary Minus, currently all results are converted to double
inline auto unaryMinus = [](auto a) -> double { return -a; };
using UnaryMinusExpression =
    NARY<1, FV<decltype(unaryMinus), NumericValueGetter>>;

/// Multiplication.
inline auto multiply = [](const auto& a, const auto& b) -> double {
  return a * b;
};
using MultiplyExpression = NARY<2, FV<decltype(multiply), NumericValueGetter>>;

/// Division.
// TODO<joka921> If `b == 0` this is technically undefined behavior and
// should lead to an expression error in SPARQL. Fix this as soon as we
// introduce the proper semantics for expression errors.
inline auto divide = [](const auto& a, const auto& b) -> double {
  return static_cast<double>(a) / b;
};
using DivideExpression = NARY<2, FV<decltype(divide), NumericValueGetter>>;

/// Addition and subtraction, currently all results are converted to double.
inline auto add = [](const auto& a, const auto& b) -> double { return a + b; };
using AddExpression = NARY<2, FV<decltype(add), NumericValueGetter>>;

inline auto subtract = [](const auto& a, const auto& b) -> double {
  return a - b;
};
using SubtractExpression = NARY<2, FV<decltype(subtract), NumericValueGetter>>;

// Basic GeoSPARQL functions (code in util/GeoSparqlHelpers.h).
using LongitudeExpression =
    NARY<1, FV<decltype(ad_utility::wktLongitude), StringValueGetter>>;
using LatitudeExpression =
    NARY<1, FV<decltype(ad_utility::wktLatitude), StringValueGetter>>;
using DistExpression =
    NARY<2, FV<decltype(ad_utility::wktDist), StringValueGetter>>;

}  // namespace detail

using detail::AddExpression;
using detail::AndExpression;
using detail::DistExpression;
using detail::DivideExpression;
using detail::LatitudeExpression;
using detail::LongitudeExpression;
using detail::MultiplyExpression;
using detail::OrExpression;
using detail::SubtractExpression;
using detail::UnaryMinusExpression;
using detail::UnaryNegateExpression;

}  // namespace sparqlExpression

#endif  // QLEVER_NARYEXPRESSION_H
