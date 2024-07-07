// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RelationalExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/ValueIdComparators.h"

namespace sparqlExpression {

// This can be used as the `FinalOperation` parameter to an
// `AggregateExpression` if there is nothing to be done on the final result.
inline auto identity = []<typename T>(T&& result, size_t) {
  return std::forward<T>(result);
};

namespace detail {

// For a given `operandGenerator`, generate the sequence of distinct values.
// This is needed for aggregation together with the `DISTINCT` keyword. For
// example, `COUNT(DISTINCT ?x)` should count the number of distinct values for
// `?x`.
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

// Class for a SPARQL expression that aggregates a given set of values to a
// single value using `AggregateOperation`, and then applies `FinalOperation`.
//
// NOTE: The `FinalOperation` is typically the `identiy` from above. One
// exception is the `AvgExpression`, where the `FinalOperation` divides the
// aggregated value (sum) by the number of elements.
template <typename AggregateOperation,
          typename FinalOperation = decltype(identity)>
class AggregateExpression : public SparqlExpression {
 public:
  // Create an aggregate expression, where `child` is the expression to be
  // aggregated.
  //
  // For example, for `SUM(?x + 5)`, `child` is the expression for `?x + 5`,
  // `distinct` is `false`, and `aggregateOp` is the operation for computing
  // the sum.
  AggregateExpression(bool distinct, Ptr&& child,
                      AggregateOperation aggregateOp = AggregateOperation{});

  // Evaluate this aggregate expression.
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // Each aggregate expression has to say what its result for an empty group is
  // (needed only for implicit GROUP BYs).
  virtual ValueId resultForEmptyGroup() const = 0;

  // Get the unaggregated variables of the expression (can be empty).
  vector<Variable> getUnaggregatedVariables() override;

  // Yes, this is an aggregate expression.
  bool isAggregate() const override { return true; }

  // Get the cache key for this expression.
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // This is only used for aggregate expressions, and then it's exactly the
  // value of the `distinct` parameter in the constructor.
  bool isDistinct() const override { return _distinct; }

  // Needed for the pattern trick, see `SparqlExpression.h`.
  [[nodiscard]] std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCount() const override;

  // Evaluate a `SingleExpressionResult` (that is, one of the possible
  // `ExpressionResult` variants). Used in the `evaluate` function.
  //
  // TODO: Why is this a lambda and not a normal member function? It's rather
  // long and complex.
  inline static const auto evaluateOnChildOperand =
      []<SingleExpressionResult Operand>(
          const AggregateOperation& aggregateOperation,
          const ValueId resultForEmptyGroup,
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

    // The number of values we aggregate.
    auto inputSize = getResultSize(*context, operand);

    // If there are no values, return the neutral element. It is important to
    // handle this case separately, because the following code only words if
    // there is at least one value.
    if (inputSize == 0) {
      return resultForEmptyGroup;
    }

    // All aggregate operations are binary, with the same value getter for each
    // operand.
    {
      using V = typename AggregateOperation::ValueGetters;
      static_assert(std::tuple_size_v<V> == 2);
      static_assert(std::is_same_v<std::tuple_element_t<0, V>,
                                   std::tuple_element_t<1, V>>);
    }
    const auto& valueGetter = std::get<0>(aggregateOperation._valueGetters);

    // Helper lambda for aggregating two values.
    //
    // TODO: Why is the `context` not passed by reference?
    auto aggregateTwoValues = [&aggregateOperation, context](
                                  auto&& x, auto&& y) -> decltype(auto) {
      if constexpr (requires {
                      aggregateOperation._function(AD_FWD(x), AD_FWD(y));
                    }) {
        return aggregateOperation._function(AD_FWD(x), AD_FWD(y));
      } else {
        return aggregateOperation._function(AD_FWD(x), AD_FWD(y), context);
      }
    };

    // A generator for the operands (before the value getter is applied to get
    // the actual values).
    auto operands =
        makeGenerator(std::forward<Operand>(operand), inputSize, context);

    // Set up cancellation handling.
    auto checkCancellation =
        [context](ad_utility::source_location location =
                      ad_utility::source_location::current()) {
          context->cancellationHandle_->throwIfCancelled(location);
        };

    // Lambda to compute the aggregate of the given operands. This requires
    // that `inputs` is not empty.
    auto computeAggregate = [&valueGetter, context, &finalOperation,
                             &aggregateOperation, &aggregateTwoValues,
                             &checkCancellation](auto&& inputs) {
      auto it = inputs.begin();
      AD_CORRECTNESS_CHECK(it != inputs.end());

      using ResultType = std::decay_t<decltype(aggregateTwoValues(
          std::move(valueGetter(*it, context)), valueGetter(*it, context)))>;
      ResultType result = valueGetter(*it, context);
      checkCancellation();
      size_t numValues = 1;

      for (++it; it != inputs.end(); ++it) {
        result = aggregateTwoValues(std::move(result),
                                    valueGetter(std::move(*it), context));
        checkCancellation();
        ++numValues;
      }
      result = finalOperation(std::move(result), numValues);
      checkCancellation();
      return result;
    };

    // Compute the aggregate (over all values or, if this is a DISTINCT
    // aggregate, only over the distinct values).
    //
    // NOTE: If the GROUP BY is implicit and we have a single group, that
    // group can be empty. Then we cannot start with the first value and
    // successively aggregate the others. Instead, we have to return the
    // "neutral element" of that aggregation operation.
    auto result = [&]() {
      if (distinct) {
        auto uniqueValues =
            getUniqueElements(context, inputSize, std::move(operands));
        checkCancellation();
        return computeAggregate(std::move(uniqueValues));
      } else {
        return computeAggregate(std::move(operands));
      }
    }();

    // If the result is numeric, convert it to an `Id`.
    //
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

// Instantiations of `AggregateExpression` for COUNT, SUM, AVG, MIN, and MAX.

// Shortcut for a binary `AggregateExpression` (all of them are binary).
template <typename Function, typename ValueGetter>
using AGG_EXP = AggregateExpression<
    Operation<2, FunctionAndValueGetters<Function, ValueGetter>>>;

// Helper function that for a given `NumericOperation` with numeric arguments
// and result (integer or floating points), returns the corresponding function
// with arguments and result of type `NumericValue` (which is a `std::variant`).
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

// Aggregate expression for COUNT.
//
// NOTE: For this expression, we have to override `getVariableForCount` for the
// pattern trick.
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
  ValueId resultForEmptyGroup() const override { return Id::makeFromInt(0); }
};

// Aggregate expression for SUM.
inline auto addForSum = makeNumericExpressionForAggregate<std::plus<>>();
using SumExpressionBase = AGG_EXP<decltype(addForSum), NumericValueGetter>;
class SumExpression : public AGG_EXP<decltype(addForSum), NumericValueGetter> {
  using SumExpressionBase::SumExpressionBase;
  ValueId resultForEmptyGroup() const override { return Id::makeFromInt(0); }
};

// Aggregate expression for AVG.
inline auto avgFinalOperation = [](const NumericValue& aggregation,
                                   size_t numElements) {
  return makeNumericExpressionForAggregate<std::divides<>>()(
      aggregation, NumericValue{static_cast<double>(numElements)});
};
using AvgOperation =
    Operation<2,
              FunctionAndValueGetters<decltype(addForSum), NumericValueGetter>>;
using AvgExpressionBase =
    AggregateExpression<AvgOperation, decltype(avgFinalOperation)>;
class AvgExpression : public AvgExpressionBase {
  using AvgExpressionBase::AvgExpressionBase;
  ValueId resultForEmptyGroup() const override {
    return Id::makeFromDouble(0.0);
  }
};

// Compare two arbitrary values (each of which can be an ID, a literal, or an
// IRI). This always returns a `bool`, see `ValueIdComparators.h` for details.
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

// Aggregate expression for MIN and MAX.
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
using MinExpressionBase =
    AGG_EXP<decltype(minLambdaForAllTypes), ActualValueGetter>;
using MaxExpressionBase =
    AGG_EXP<decltype(maxLambdaForAllTypes), ActualValueGetter>;
class MinExpression : public MinExpressionBase {
  using MinExpressionBase::MinExpressionBase;
  ValueId resultForEmptyGroup() const override { return Id::makeUndefined(); }
};
class MaxExpression : public MaxExpressionBase {
  using MaxExpressionBase::MaxExpressionBase;
  ValueId resultForEmptyGroup() const override { return Id::makeUndefined(); }
};

}  // namespace detail

using detail::AvgExpression;
using detail::CountExpression;
using detail::MaxExpression;
using detail::MinExpression;
using detail::SumExpression;
}  // namespace sparqlExpression
