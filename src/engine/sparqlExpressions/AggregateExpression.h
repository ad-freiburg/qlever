// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_AGGREGATEEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_AGGREGATEEXPRESSION_H

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
struct Identity {
  template <typename T>
  auto operator()(T&& result, size_t) const {
    return AD_FWD(result);
  }
};

namespace detail {

// For a given `operandGenerator`, generate the sequence of distinct values.
// This is needed for aggregation together with the `DISTINCT` keyword. For
// example, `COUNT(DISTINCT ?x)` should count the number of distinct values for
// `?x`.
inline auto getUniqueElements = [](const EvaluationContext* context,
                                   size_t inputSize, auto operandGenerator) {
  using OperandGenerator = decltype(operandGenerator);
  ad_utility::HashSetWithMemoryLimit<
      ql::ranges::range_value_t<OperandGenerator>>
      uniqueHashSet(inputSize, context->_allocator);
  // Note: The `filter` below is technically undefined behavior for
  // `std::views::filter`, because in a second iteration the values would
  // all be filtered out, because they are already contained in the hash map.
  // We mitigate this issue by using the `filter` from range-v3 +
  // `ForceInputView`, which disallows multiple calls to `begin`.
  return ad_utility::ForceInputView(
      ad_utility::OwningView(std::move(operandGenerator)) |
      ::ranges::views::filter(
          [set = std::move(uniqueHashSet)](auto& operand) mutable {
            return set.insert(operand).second;
          }));
};

// Class for a SPARQL expression that aggregates a given set of values to a
// single value using `AggregateOperation`, and then applies `FinalOperation`.
//
// NOTE: The `FinalOperation` is typically the `Identity` from above. One
// exception is the `AvgExpression`, where the `FinalOperation` divides the
// aggregated value (sum) by the number of elements.
template <typename AggregateOperation, typename FinalOperation = Identity>
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
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // Needed for the pattern trick, see `SparqlExpression.h`.
  [[nodiscard]] std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCount() const override;

 private:
  // _________________________________________________________________________
  ql::span<SparqlExpression::Ptr> childrenImpl() override;

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

// Helper struct that for a given `NumericOperation` with numeric arguments
// and result (integer or floating points), returns the corresponding function
// with arguments and result of type `NumericValue` (which is a `std::variant`).
template <typename NumericOperation>
struct NumericExpressionForAggregate {
  template <typename... Args>
  auto operator()(const Args&... args) const -> CPP_ret(NumericValue)(
      requires(ad_utility::SimilarTo<Args, NumericValue>&&...)) {
    auto visitor = [](const auto&... t) -> NumericValue {
      if constexpr ((... ||
                     std::is_same_v<NotNumeric, std::decay_t<decltype(t)>>)) {
        return NotNumeric{};
      } else {
        return (NumericOperation{}(t...));
      }
    };
    return std::visit(visitor, args...);
  }
};

template <typename NumericOperation>
inline auto makeNumericExpressionForAggregate() {
  return NumericExpressionForAggregate<NumericOperation>{};
}

// Aggregate expression for COUNT.
//
// NOTE: For this expression, we have to override `getVariableForCount` for the
// pattern trick.
struct Count {
  template <typename T1, typename T2>
  int64_t operator()(const T1& a, const T2& b) const {
    return a + b;
  }
};
using CountExpressionBase = AGG_EXP<Count, IsValidValueGetter>;
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
using AddForSum = NumericExpressionForAggregate<std::plus<>>;
using SumExpressionBase = AGG_EXP<AddForSum, NumericValueGetter>;
class SumExpression : public AGG_EXP<AddForSum, NumericValueGetter> {
  using SumExpressionBase::SumExpressionBase;
  ValueId resultForEmptyGroup() const override { return Id::makeFromInt(0); }
};

// Aggregate expression for AVG.
struct AvgFinalOperation {
  NumericValue operator()(const NumericValue& aggregation,
                          size_t numElements) const {
    return makeNumericExpressionForAggregate<std::divides<>>()(
        aggregation, NumericValue{static_cast<double>(numElements)});
  }
};
using AvgOperation =
    Operation<2, FunctionAndValueGetters<AddForSum, NumericValueGetter>>;
using AvgExpressionBase = AggregateExpression<AvgOperation, AvgFinalOperation>;
class AvgExpression : public AvgExpressionBase {
  using AvgExpressionBase::AvgExpressionBase;
  ValueId resultForEmptyGroup() const override { return Id::makeFromInt(0); }
};

// Compare two arbitrary values (each of which can be an ID, a literal, or an
// IRI). This always returns a `bool`, see `ValueIdComparators.h` for details.
template <valueIdComparators::Comparison Comp>
inline const auto compareIdsOrStrings =
    [](const auto& a, const auto& b,
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
struct MinMaxLambdaForAllTypes {
  template <typename T>
  auto operator()(const T& a, const T& b, const EvaluationContext* ctx) const
      -> CPP_ret(T)(requires SingleExpressionResult<T>) {
    auto actualImpl = [ctx](const auto& x, const auto& y) {
      return compareIdsOrStrings<comparison>(x, y, ctx);
    };
    if constexpr (ad_utility::isSimilar<T, Id>) {
      return std::get<Id>(actualImpl(a, b));
    } else {
      // TODO<joka921> We should definitely move strings here.
      return std::visit(actualImpl, a, b);
    }
  }
};
using MinLambdaForAllTypes =
    MinMaxLambdaForAllTypes<valueIdComparators::Comparison::LT>;
using MaxLambdaForAllTypes =
    MinMaxLambdaForAllTypes<valueIdComparators::Comparison::GT>;
using MinExpressionBase = AGG_EXP<MinLambdaForAllTypes, ActualValueGetter>;
using MaxExpressionBase = AGG_EXP<MaxLambdaForAllTypes, ActualValueGetter>;
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

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_AGGREGATEEXPRESSION_H
