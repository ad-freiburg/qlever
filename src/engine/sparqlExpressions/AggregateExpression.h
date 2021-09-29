//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 28.09.21.
//

#ifndef QLEVER_AGGREGATEEXPRESSION_H
#define QLEVER_AGGREGATEEXPRESSION_H

#include "SparqlExpression.h"
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
                      AggregateOperation aggregateOp = AggregateOperation{})
      : _distinct(distinct),
        _child{std::move(child)},
        _aggregateOp{std::move(aggregateOp)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    auto childResult = _child->evaluate(context);

    return ad_utility::visitWithVariantsAndParameters(
        evaluateOnChildOperand, _aggregateOp, FinalOperation{}, context,
        _distinct, std::move(childResult));
  }

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override { return {&_child, 1}; }

  // _________________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregate, so it never leaves any unaggregated variables.
    return {};
  }

  // __________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return std::string(typeid(*this).name()) + std::to_string(_distinct) + "(" +
           _child->getCacheKey(varColMap) + ")";
  }

  // __________________________________________________________________________
  [[nodiscard]] std::optional<string> getVariableForNonDistinctCountOrNullopt()
      const override {
    // This behavior is not correct for the `COUNT` aggreate. The count is
    // therefore implemented in a separate `CountExpression` class, which
    // overrides this function.
    return std::nullopt;
  }

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
    if constexpr (detail::isCalculationWithSetOfIntervalsAllowed<
                      AggregateOperation, Operand>) {
      return aggregateOperation._functionForSetOfIntervals(
          std::forward<Operand>(operand));
    }

    // The number of inputs we aggregate over.
    auto inputSize = getResultSize(*context, operand);

    // Aggregates are unary expressions, therefore we have only one value getter
    // for the single operand.
    static_assert(
        std::tuple_size_v<typename AggregateOperation::ValueGetters> == 1);
    const auto& valueGetter = std::get<0>(aggregateOperation._valueGetters);

    if (!distinct) {
      auto values = valueGetterGenerator(
          valueGetter, std::forward<Operand>(operand), inputSize, context);
      auto it = values.begin();
      auto result = *it;
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
      auto result = valueGetter(*it, context);
      ad_utility::HashSetWithMemoryLimit<typename decltype(
          operands)::value_type>
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
  [[nodiscard]] std::optional<string> getVariableForNonDistinctCountOrNullopt()
      const override {
    if (this->_distinct) {
      return std::nullopt;
    }
    return _child->getVariableOrNullopt();
  }
};

// SUM
inline auto add = [](const auto& a, const auto& b) { return a + b; };
using SumExpression = AGG_EXP<decltype(add), NumericValueGetter>;

// AVG
inline auto averageFinalOp = [](const auto& aggregation, size_t numElements) {
  return numElements ? static_cast<double>(aggregation) /
                           static_cast<double>(numElements)
                     : std::numeric_limits<double>::quiet_NaN();
};
using AvgExpression =
    detail::AggregateExpression<AGG_OP<decltype(add), NumericValueGetter>>;

// MIN
inline auto minLambda = [](const auto& a, const auto& b) {
  return a < b ? a : b;
};
using MinExpression = AGG_EXP<decltype(minLambda), NumericValueGetter>;

// MAX
inline auto maxLambda = [](const auto& a, const auto& b) {
  return a > b ? a : b;
};
using MaxExpression = AGG_EXP<decltype(maxLambda), NumericValueGetter>;

}  // namespace detail
}  // namespace sparqlExpression

#endif  // QLEVER_AGGREGATEEXPRESSION_H
