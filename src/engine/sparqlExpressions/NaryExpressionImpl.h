//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <ranges>

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

namespace sparqlExpression::detail {
template <typename NaryOperation>
requires(isOperation<NaryOperation>)
class NaryExpression : public SparqlExpression {
 public:
  static constexpr size_t N = NaryOperation::N;
  using Children = std::array<SparqlExpression::Ptr, N>;

 private:
  Children children_;

 public:
  // Construct from an array of `N` child expressions.
  explicit NaryExpression(Children&& children);

  // Construct from `N` child expressions. Each of the children must have a type
  // `std::unique_ptr<SubclassOfSparqlExpression>`.
  explicit NaryExpression(
      std::convertible_to<SparqlExpression::Ptr> auto... children)
      requires(sizeof...(children) == N)
      : NaryExpression{Children{std::move(children)...}} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override;

  // _________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

 private:
  // Evaluate the `naryOperation` on the `operands` using the `context`.
  template <SingleExpressionResult... Operands>
  static ExpressionResult evaluateOnChildrenOperands(
      NaryOperation naryOperation, EvaluationContext* context,
      Operands&&... operands) {
    // Perform a more efficient calculation if a specialized function exists
    // that matches all operands.
    if (isAnySpecializedFunctionPossible(naryOperation._specializedFunctions,
                                         operands...)) {
      auto optionalResult = evaluateOnSpecializedFunctionsIfPossible(
          naryOperation._specializedFunctions,
          std::forward<Operands>(operands)...);
      AD_CORRECTNESS_CHECK(optionalResult);
      return std::move(optionalResult.value());
    }

    // We have to first determine the number of results we will produce.
    auto targetSize = getResultSize(*context, operands...);

    // The result is a constant iff all the results are constants.
    constexpr static bool resultIsConstant =
        (... && isConstantResult<Operands>);

    // The generator for the result of the operation.
    auto resultGenerator =
        applyOperation(targetSize, naryOperation, context, AD_FWD(operands)...);

    // Compute the result.
    using ResultType = typename decltype(resultGenerator)::value_type;
    VectorWithMemoryLimit<ResultType> result{context->_allocator};
    result.reserve(targetSize);
    std::ranges::move(resultGenerator, std::back_inserter(result));

    if constexpr (resultIsConstant) {
      AD_CORRECTNESS_CHECK(result.size() == 1);
      return std::move(result[0]);
    } else {
      return result;
    }
  }
};

// Takes a `Function` that returns a numeric value (integral or floating point)
// and converts it to a function, that takes the same arguments and returns the
// same result, but the return type is the `NumericValue` variant.
template <typename Function, bool nanToUndef = false>
struct NumericIdWrapper {
  // Note: Sonarcloud suggests `[[no_unique_address]]` for the following member,
  // but adding it causes an internal compiler error in Clang 16.
  Function function_{};
  Id operator()(auto&&... args) const {
    return makeNumericId<nanToUndef>(function_(AD_FWD(args)...));
  }
};

// Takes a `Function` that takes and returns numeric values (integral or
// floating point) and converts it to a function, that takes the same arguments
// and returns the same result, but the arguments and the return type are the
// `NumericValue` variant.
template <typename Function>
inline auto makeNumericExpression() {
  return [](const std::same_as<NumericValue> auto&... args) {
    auto visitor = []<typename... Ts>(const Ts&... t) {
      if constexpr ((... || std::is_same_v<NotNumeric, Ts>)) {
        return Id::makeUndefined();
      } else {
        return makeNumericId(Function{}(t...));
      }
    };
    return std::visit(visitor, args...);
  };
}

// Two short aliases to make the instantiations more readable.
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

// The types for the concrete MultiBinaryExpressions and UnaryExpressions.
using TernaryBool = EffectiveBooleanValueGetter::Result;

// _____________________________________________________________________________
template <typename Op>
requires(isOperation<Op>)
NaryExpression<Op>::NaryExpression(Children&& children)
    : children_{std::move(children)} {}

// _____________________________________________________________________________

template <typename NaryOperation>
requires(isOperation<NaryOperation>)
ExpressionResult NaryExpression<NaryOperation>::evaluate(
    EvaluationContext* context) const {
  auto resultsOfChildren = ad_utility::applyFunctionToEachElementOfTuple(
      [context](const auto& child) { return child->evaluate(context); },
      children_);

  // Bind the `evaluateOnChildrenOperands` to a lambda.
  auto evaluateOnChildOperandsAsLambda = [](auto&&... args) {
    return evaluateOnChildrenOperands(AD_FWD(args)...);
  };

  // A function that only takes several `ExpressionResult`s,
  // and evaluates the expression.
  auto evaluateOnChildrenResults = std::bind_front(
      ad_utility::visitWithVariantsAndParameters,
      evaluateOnChildOperandsAsLambda, NaryOperation{}, context);

  return std::apply(evaluateOnChildrenResults, std::move(resultsOfChildren));
}

// _____________________________________________________________________________
template <typename Op>
requires(isOperation<Op>)
std::span<SparqlExpression::Ptr> NaryExpression<Op>::children() {
  return {children_.data(), children_.size()};
}

// __________________________________________________________________________
template <typename Op>
requires(isOperation<Op>) [[nodiscard]] string NaryExpression<Op>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  string key = typeid(*this).name();
  key += ad_utility::lazyStrJoin(
      children_ | std::views::transform([&varColMap](const auto& child) {
        return child->getCacheKey(varColMap);
      }),
      "");
  return key;
}

// Define a class `Name` that is a strong typedef (via inheritance) from
// `NaryExpresssion<N, X, ...>`. The strong typedef (vs. a simple `using`
// declaration) is used to improve compiler messages as the resulting class has
// a short and descriptive name.
#define NARY_EXPRESSION(Name, N, X, ...)                                     \
  class Name : public NaryExpression<detail::Operation<N, X, __VA_ARGS__>> { \
    using Base = NaryExpression<Operation<N, X, __VA_ARGS__>>;               \
    using Base::Base;                                                        \
  };

}  // namespace sparqlExpression::detail
