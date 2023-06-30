// Copyright 2021 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <charconv>
#include <concepts>
#include <cstdlib>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/Conversions.h"
#include "util/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {

// TODO: This comment is out of date. It refers to `BinaryOperations`,
// `ValueGetter`, and `create`, none of which can be found in this file.
//
// A sequence of binary operations, which is executed from left to right, for
// example (?a or ?b), (?a and ?b ?and ?c), (3 * 5 / 7 * ?x) . Different
// operations in the same expression, like (?a + ?b - ?c) are supported by
// passing in multiple operations as the `BinaryOperations` template parameter
// and by choosing the corresponding operation for each sub-expression via the
// `tags` argument to the `create` function (see there).
//
// @tparam ValueGetter A callable type that takes a
// double/int64_t/Bool/string/StrongIdWithResultType and extracts the actual
// input to the operation. Can be used to perform type conversions before the
// actual operation.
//
// @tparam BinaryOperations The actual binary operations. They must be callable
// with the result types of the `ValueGetter`.
template <typename NaryOperation>
requires(isOperation<NaryOperation>)
class NaryExpression : public SparqlExpression {
 public:
  static constexpr size_t N = NaryOperation::N;
  using Children = std::array<SparqlExpression::Ptr, N>;

  // Construct from an array of `N` child expressions.
  explicit NaryExpression(Children&& children);

  // Construct from `N` child expressions. Each of the children must have a type
  // `std::unique_ptr<SubclassOfSparqlExpression>`.
  explicit NaryExpression(
      std::convertible_to<SparqlExpression::Ptr> auto... children)
      requires(sizeof...(children) == N)
      : NaryExpression{Children{std::move(children)...}} {}

 public:
  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override;

  // _________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

 private:
  // Evaluate the `naryOperation` on the `operands` using the `context`.
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
    auto resultGenerator =
        applyOperation(targetSize, naryOperation, context, AD_FWD(operands)...);

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
inline auto orLambda = [](bool a, bool b) -> Bool { return a || b; };
using OrExpression =
    NARY<2, FV<decltype(orLambda), EffectiveBooleanValueGetter>,
         SET<SetOfIntervals::Union>>;

inline auto andLambda = [](bool a, bool b) -> Bool { return a && b; };
using AndExpression =
    NARY<2, FV<decltype(andLambda), EffectiveBooleanValueGetter>,
         SET<SetOfIntervals::Intersection>>;

// Unary Negation
inline auto unaryNegate = [](bool a) -> Bool { return !a; };
using UnaryNegateExpression =
    NARY<1, FV<decltype(unaryNegate), EffectiveBooleanValueGetter>,
         SET<SetOfIntervals::Complement>>;

// Unary Minus, currently all results are converted to double
inline auto unaryMinus = [](auto a) -> double { return -a; };
using UnaryMinusExpression =
    NARY<1, FV<decltype(unaryMinus), NumericValueGetter>>;

// Multiplication.
inline auto multiply = [](const auto& a, const auto& b) -> double {
  return a * b;
};
using MultiplyExpression = NARY<2, FV<decltype(multiply), NumericValueGetter>>;

// Division.
//
// TODO<joka921> If `b == 0` this is technically undefined behavior and
// should lead to an expression error in SPARQL. Fix this as soon as we
// introduce the proper semantics for expression errors.
inline auto divide = [](const auto& a, const auto& b) -> double {
  return static_cast<double>(a) / b;
};
using DivideExpression = NARY<2, FV<decltype(divide), NumericValueGetter>>;

// Addition and subtraction, currently all results are converted to double.
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

// Date functions.
//
inline auto extractYear = [](std::optional<DateOrLargeYear> d) {
  if (!d.has_value()) {
    return Id::makeUndefined();
  } else {
    return Id::makeFromInt(d->getYear());
  }
};

inline auto extractMonth = [](std::optional<DateOrLargeYear> d) {
  // TODO<C++23> Use the monadic operations for std::optional
  if (!d.has_value()) {
    return Id::makeUndefined();
  }
  auto optionalMonth = d.value().getMonth();
  if (!optionalMonth.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(optionalMonth.value());
};

inline auto extractDay = [](std::optional<DateOrLargeYear> d) {
  // TODO<C++23> Use the monadic operations for `std::optional`.
  if (!d.has_value()) {
    return Id::makeUndefined();
  }
  auto optionalDay = d.value().getDay();
  if (!optionalDay.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(optionalDay.value());
};

using YearExpression = NARY<1, FV<decltype(extractYear), DateValueGetter>>;
using MonthExpression = NARY<1, FV<decltype(extractMonth), DateValueGetter>>;
using DayExpression = NARY<1, FV<decltype(extractDay), DateValueGetter>>;

// String functions.
using StrExpression = NARY<1, FV<std::identity, StringValueGetter>>;

// Compute string length.
inline auto strlen = [](const auto& s) -> int64_t { return s.size(); };
using StrlenExpression = NARY<1, FV<decltype(strlen), StringValueGetter>>;

}  // namespace detail

using detail::AddExpression;
using detail::AndExpression;
using detail::DivideExpression;
using detail::MultiplyExpression;
using detail::OrExpression;
using detail::SubtractExpression;
using detail::UnaryMinusExpression;
using detail::UnaryNegateExpression;

using detail::DistExpression;
using detail::LatitudeExpression;
using detail::LongitudeExpression;

using detail::DayExpression;
using detail::MonthExpression;
using detail::YearExpression;

using detail::StrExpression;
using detail::StrlenExpression;

}  // namespace sparqlExpression
