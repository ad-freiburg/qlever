// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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
inline auto identity = [](auto&& result, size_t) { return AD_FWD(result); };

namespace detail {

// For a given `operandGenerator`, generate the sequence of distinct values.
// This is needed for aggregation together with the `DISTINCT` keyword. For
// example, `COUNT(DISTINCT ?x)` should count the number of distinct values for
// `?x`.
inline auto getUniqueElements = []<typename OperandGenerator>(
                                    const EvaluationContext* context,
                                    size_t inputSize,
                                    OperandGenerator operandGenerator)
    -> cppcoro::generator<ql::ranges::range_value_t<OperandGenerator>> {
  ad_utility::HashSetWithMemoryLimit<
      ql::ranges::range_value_t<OperandGenerator>>
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
// NOTE: The `FinalOperation` is typically the `identity` from above. One
// exception is the `AvgExpression`, where the `FinalOperation` divides the
// aggregated value (sum) by the number of elements.
template <typename AggregateOperation,
          typename FinalOperation = decltype(identity)>
class AggregateExpression : public SparqlExpression {
 public:
  // Create an aggregate expression from the given arguments. For example, for
  // `SUM(?x + 5)`, `child` is the expression for `?x + 5`, `distinct` is
  // `false`, and `aggregateOp` is the operation for computing the sum.
  //
  // NOTE: For almost all aggregates, the `AggregateOperation` is stateless,
  // hence the default-constructed default argument. The only exception is the
  // `GROUP_CONCAT` expression, which stores its separator in the
  // `AggregateOperation`.
  AggregateExpression(bool distinct, Ptr&& child,
                      AggregateOperation aggregateOp = AggregateOperation{});

  // Evaluate this aggregate expression.
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // Each aggregate expression has to say what its result for an empty group is
  // (needed only for implicit GROUP BYs).
  virtual ValueId resultForEmptyGroup() const = 0;

  // Yes, this is an aggregate expression.
  AggregateStatus isAggregate() const override {
    return _distinct ? AggregateStatus::DistinctAggregate
                     : AggregateStatus::NonDistinctAggregate;
  }

  // Get the cache key for this expression.
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // Needed for the pattern trick, see `SparqlExpression.h`.
  [[nodiscard]] std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCount() const override;

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
  return []<typename... Args>(const Args&... args)
             -> CPP_ret(NumericValue)(
                 requires(concepts::same_as<Args, NumericValue>&&...)) {
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
  ValueId resultForEmptyGroup() const override { return Id::makeFromInt(0); }
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
inline const auto minMaxLambdaForAllTypes = CPP_template_lambda()(typename T)(
    const T& a, const T& b,
    const EvaluationContext* ctx)(requires SingleExpressionResult<T>) {
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
