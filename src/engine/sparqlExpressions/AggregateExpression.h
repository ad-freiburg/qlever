// Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_AGGREGATEEXPRESSION_H
#define QLEVER_AGGREGATEEXPRESSION_H

#include "engine/sparqlExpressions/RelationalExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
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

// For DISTINCT we must put the operands into the hash set before
// applying the `valueGetter`. For example, COUNT(?x), where ?x matches
// three different strings, the value getter always returns `1`, but
// we still have three distinct inputs.
inline auto getUniqueElements = []<typename OperandGenerator>(
                                    const EvaluationContext* context,
                                    size_t inputSize,
                                    OperandGenerator operandGenerator)
    -> cppcoro::generator<std::ranges::range_value_t<OperandGenerator>> {
  ad_utility::HashSetWithMemoryLimit<
      std::ranges::range_value_t<OperandGenerator>>
      uniqueHashSet(inputSize, context->_allocator);
  for (auto& operand : operandGenerator) {
    if (uniqueHashSet.insert(operand).second) {
      auto elForYielding = std::move(operand);
      co_yield elForYielding;
    }
  }
};
template <typename AggregateOperation, typename FinalOperation = decltype(noop)>
class AggregateExpression : public SparqlExpression {
 public:
  // __________________________________________________________________________
  AggregateExpression(bool distinct, Ptr&& child,
                      AggregateOperation aggregateOp = AggregateOperation{});

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  vector<Variable> getUnaggregatedVariables() override;

  // _________________________________________________________________________
  bool isAggregate() const override { return true; }

  // __________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // __________________________________________________________________________
  bool isDistinct() const override { return _distinct; }

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
      AD_CONTRACT_CHECK(optionalResult);
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
    auto callFunction = [&aggregateOperation, context](
                            auto&& x, auto&& y) -> decltype(auto) {
      if constexpr (requires {
                      aggregateOperation._function(AD_FWD(x), AD_FWD(y));
                    }) {
        return aggregateOperation._function(AD_FWD(x), AD_FWD(y));
      } else {
        return aggregateOperation._function(AD_FWD(x), AD_FWD(y), context);
      }
    };
    // The operands *without* applying the `valueGetter`.
    auto operands =
        makeGenerator(std::forward<Operand>(operand), inputSize, context);
    auto checkCancellation =
        [context](ad_utility::source_location location =
                      ad_utility::source_location::current()) {
          context->cancellationHandle_->throwIfCancelled(location);
        };

    auto impl = [&valueGetter, context, &finalOperation, &callFunction,
                 &checkCancellation](auto&& inputs) {
      auto it = inputs.begin();
      AD_CORRECTNESS_CHECK(it != inputs.end());

      using ResultType = std::decay_t<decltype(callFunction(
          std::move(valueGetter(*it, context)), valueGetter(*it, context)))>;
      ResultType result = valueGetter(*it, context);
      checkCancellation();
      size_t numValues = 1;

      for (++it; it != inputs.end(); ++it) {
        result = callFunction(std::move(result),
                              valueGetter(std::move(*it), context));
        checkCancellation();
        ++numValues;
      }
      result = finalOperation(std::move(result), numValues);
      checkCancellation();
      return result;
    };
    auto result = [&]() {
      if (distinct) {
        auto uniqueValues =
            getUniqueElements(context, inputSize, std::move(operands));
        checkCancellation();
        return impl(std::move(uniqueValues));
      } else {
        return impl(std::move(operands));
      }
    }();

    // Currently the intermediate results can be `double` or `int` values
    // which then have to be converted to an ID again.
    // TODO<joka921> Check if this is really necessary, or if we can also use
    // IDs in the intermediate steps without loss of efficiency.
    if constexpr (requires { makeNumericId(result); }) {
      return makeNumericId(result);
    } else {
      return result;
    }
  };

 private:
  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> childrenImpl() override;

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

// Take a `NumericOperation` that takes numeric arguments (integral or floating
// points) and returns a numeric result. Return a function that performs the
// same operation, but takes and returns the `NumericValue` variant.
template <typename NumericOperation>
inline auto makeNumericExpressionForAggregate() {
  return [](const std::same_as<NumericValue> auto&... args) -> NumericValue {
    auto visitor = []<typename... Ts>(const Ts&... t) -> NumericValue {
      if constexpr ((... || std::is_same_v<NotNumeric, Ts>)) {
        return NotNumeric{};
      } else {
        return (NumericOperation{}(t...));
      }
    };
    return std::visit(visitor, args...);
  };
}

// SUM
inline auto addForSum = makeNumericExpressionForAggregate<std::plus<>>();
using SumExpression = AGG_EXP<decltype(addForSum), NumericValueGetter>;

// AVG
inline auto averageFinalOp = [](const NumericValue& aggregation,
                                size_t numElements) {
  return makeNumericExpressionForAggregate<std::divides<>>()(
      aggregation, NumericValue{static_cast<double>(numElements)});
};
using AvgExpression =
    detail::AggregateExpression<AGG_OP<decltype(addForSum), NumericValueGetter>,
                                decltype(averageFinalOp)>;

// TODO<joka921> Comment
template <valueIdComparators::Comparison Comp>
inline const auto compareIdsOrStrings =
    []<typename T, typename U>(
        const T& a, const U& b,
        const EvaluationContext* ctx) -> IdOrLiteralOrIri {
  // TODO<joka921> moveTheStrings.
  return toBoolNotUndef(
             sparqlExpression::compareIdsOrStrings<
                 Comp, valueIdComparators::ComparisonForIncompatibleTypes::
                           CompareByType>(a, b, ctx))
             ? IdOrLiteralOrIri{a}
             : IdOrLiteralOrIri{b};
};
// Min and Max.
template <valueIdComparators::Comparison comparison>
inline const auto minMaxLambdaForAllTypes =
    []<SingleExpressionResult T>(const T& a, const T& b,
                                 const EvaluationContext* ctx) {
      auto actualImpl = [ctx](const auto& x, const auto& y) {
        return compareIdsOrStrings<comparison>(x, y, ctx);
      };
      if constexpr (ad_utility::isSimilar<T, Id>) {
        return std::get<Id>(actualImpl(a, b));
      } else {
        // TODO<joka921> We should definitely move strings here.
        return std::visit(actualImpl, a, b);
      }
    };

constexpr inline auto minLambdaForAllTypes =
    minMaxLambdaForAllTypes<valueIdComparators::Comparison::LT>;
constexpr inline auto maxLambdaForAllTypes =
    minMaxLambdaForAllTypes<valueIdComparators::Comparison::GT>;
// MIN
using MinExpression =
    AGG_EXP<decltype(minLambdaForAllTypes), ActualValueGetter>;

// MAX
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
