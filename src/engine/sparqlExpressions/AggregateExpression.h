// Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_AGGREGATEEXPRESSION_H
#define QLEVER_AGGREGATEEXPRESSION_H

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "global/ValueIdComparators.h"

namespace sparqlExpression {

// This can be used as the `FinalOperation` parameter to an
// `AggregateExpression` if there is nothing to be done on the final result.
inline auto noop = []<typename T>(T&& result, size_t) {
  return std::forward<T>(result);
};

// An expression that aggregates its input using the `AggregateOperation` and
// then executes the `FinalOperation` (possibly the `noop` lambda from above) on
// the result.
namespace detail {
template <typename AggregateOperation, typename FinalOperation = decltype(noop)>
class AggregateExpression : public SparqlExpression {
 public:
  // __________________________________________________________________________
  AggregateExpression(bool distinct, Ptr&& child,
                      AggregateOperation aggregateOp = AggregateOperation{});

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override;

  // _________________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override;

  // __________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // __________________________________________________________________________
  [[nodiscard]] std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCount() const override;

  // This is the visitor for the `evaluateAggregateExpression` function below.
  // It works on a `SingleExpressionResult` rather than on the
  // `ExpressionResult` variant.
  inline static const auto evaluateOnChildOperand =
      []<SingleExpressionResult Operand>(
          const AggregateOperation& aggregateOperation,
          const FinalOperation& finalOperation, EvaluationContext* context,
          bool distinct, Operand&& operand) -> ExpressionResult {
    // Perform the more efficient calculation on `SetOfInterval`s if it is
    // possible.
    if (isAnySpecializedFunctionPossible(
            aggregateOperation._specializedFunctions, operand)) {
      auto optionalResult = evaluateOnSpecializedFunctionsIfPossible(
          aggregateOperation._specializedFunctions,
          std::forward<Operand>(operand));
      AD_CHECK(optionalResult);
      return std::move(optionalResult.value());
    }

    // The number of inputs we aggregate over.
    auto inputSize = getResultSize(*context, operand);

    // Aggregates are unary expressions, therefore we have only one value getter
    // for the single operand. But since the aggregating operation is binary,
    // there are two identical value getters for technical reasons
    {
      using V = typename AggregateOperation::ValueGetters;
      static_assert(std::tuple_size_v<V> == 2);
      static_assert(std::is_same_v<std::tuple_element_t<0, V>,
                                   std::tuple_element_t<1, V>>);
    }

    const auto& valueGetter = std::get<0>(aggregateOperation._valueGetters);

    if (!distinct) {
      auto values = detail::valueGetterGenerator(
          inputSize, context, std::forward<Operand>(operand), valueGetter);
      auto it = values.begin();
      // Unevaluated operation to get the proper `ResultType`. With `auto`, we
      // would get the operand type, which is not necessarily the `ResultType`.
      // For example, in the COUNT aggregate we calculate a sum of boolean
      // values, but the result is not boolean.
      using ResultType = std::decay_t<decltype(aggregateOperation._function(
          std::move(*it), *it))>;
      ResultType result = *it;
      for (++it; it != values.end(); ++it) {
        result =
            aggregateOperation._function(std::move(result), std::move(*it));
      }
      result = finalOperation(std::move(result), inputSize);
      return result;
    } else {
      // The operands *without* applying the `valueGetter`.
      auto operands =
          makeGenerator(std::forward<Operand>(operand), inputSize, context);

      // For distinct we must put the operands into the hash set before
      // applying the `valueGetter`. For example, COUNT(?x), where ?x matches
      // three different strings, the value getter always returns `1`, but
      // we still have three distinct inputs.
      auto it = operands.begin();
      // Unevaluated operation to get the proper `ResultType`. With `auto`, we
      // would get the operand type, which is not necessarily the `ResultType`.
      // For example, in the COUNT aggregate we calculate a sum of boolean
      // values, but the result is not boolean.
      using ResultType = std::decay_t<decltype(aggregateOperation._function(
          std::move(valueGetter(*it, context)), valueGetter(*it, context)))>;
      ResultType result = valueGetter(*it, context);
      ad_utility::HashSetWithMemoryLimit<
          typename decltype(operands)::value_type>
          uniqueHashSet({*it}, inputSize, context->_allocator);
      for (++it; it != operands.end(); ++it) {
        if (uniqueHashSet.insert(*it).second) {
          result = aggregateOperation._function(
              std::move(result), valueGetter(std::move(*it), context));
        }
      }
      result = finalOperation(std::move(result), uniqueHashSet.size());
      return result;
    }
  };

 protected:
  bool _distinct;
  Ptr _child;
  AggregateOperation _aggregateOp;
};

// The Aggregate expressions.

template <typename... Ts>
using AGG_OP = Operation<2, FunctionAndValueGetters<Ts...>>;

template <typename... Ts>
using AGG_EXP =
    AggregateExpression<Operation<2, FunctionAndValueGetters<Ts...>>>;

// COUNT
/// For the count expression, we have to manually overwrite one member function
/// for the pattern trick.
inline auto count = [](const auto& a, const auto& b) -> int64_t {
  return a + b;
};
using CountExpressionBase = AGG_EXP<decltype(count), IsValidValueGetter>;
class CountExpression : public CountExpressionBase {
  using CountExpressionBase::CountExpressionBase;
  [[nodiscard]] std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCount() const override {
    auto optionalVariable = _child->getVariableOrNullopt();
    if (optionalVariable.has_value()) {
      return SparqlExpressionPimpl::VariableAndDistinctness{
          std::move(optionalVariable.value()), _distinct};
    } else {
      return std::nullopt;
    }
  }
};

// SUM
inline auto addForSum = [](const auto& a, const auto& b) { return a + b; };
using SumExpression = AGG_EXP<decltype(addForSum), NumericValueGetter>;

// AVG
inline auto averageFinalOp = [](const auto& aggregation, size_t numElements) {
  return numElements ? static_cast<double>(aggregation) /
                           static_cast<double>(numElements)
                     : std::numeric_limits<double>::quiet_NaN();
};
using AvgExpression =
    detail::AggregateExpression<AGG_OP<decltype(addForSum), NumericValueGetter>,
                                decltype(averageFinalOp)>;

// Note: the std::common_type_t is required in case we compare different numeric
// types like an int and a bool. Then we need to manually specify the
// return type.

// MIN
inline auto minLambda = []<typename T, typename U>(const T& a, const U& b) {
  using C = std::common_type_t<T, U>;
  return a < b ? static_cast<C>(a) : static_cast<C>(b);
};

inline auto minLambdaForAllTypes = []<SingleExpressionResult T>(const T& a,
                                                                const T& b) {
  if constexpr (std::is_arithmetic_v<T> || ad_utility::isSimilar<T, Bool> ||
                ad_utility::isSimilar<T, std::string>) {
    return std::min(a, b);
  } else if constexpr (ad_utility::isSimilar<T, Id>) {
    return valueIdComparators::compareIds(a, b,
                                          valueIdComparators::Comparison::LT)
               ? a
               : b;
  } else {
    return ad_utility::alwaysFalse<T>;
  }
};
using MinExpression =
    AGG_EXP<decltype(minLambdaForAllTypes), ActualValueGetter>;

// MAX
inline auto maxLambda = []<typename T, typename U>(const T& a, const U& b) {
  using C = std::common_type_t<T, U>;
  return a > b ? static_cast<C>(a) : static_cast<C>(b);
};

inline auto maxLambdaForAllTypes = []<SingleExpressionResult T>(const T& a,
                                                                const T& b) {
  if constexpr (std::is_arithmetic_v<T> || ad_utility::isSimilar<T, Bool> ||
                ad_utility::isSimilar<T, std::string>) {
    return std::max(a, b);
  } else if constexpr (ad_utility::isSimilar<T, Id>) {
    return valueIdComparators::compareIds(a, b,
                                          valueIdComparators::Comparison::GT)
               ? a
               : b;
  } else {
    return ad_utility::alwaysFalse<T>;
  }
};
using MaxExpression =
    AGG_EXP<decltype(maxLambdaForAllTypes), ActualValueGetter>;

}  // namespace detail

using detail::AvgExpression;
using detail::CountExpression;
using detail::MaxExpression;
using detail::MinExpression;
using detail::SumExpression;
}  // namespace sparqlExpression

#endif  // QLEVER_AGGREGATEEXPRESSION_H
