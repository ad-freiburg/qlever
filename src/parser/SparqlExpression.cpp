//
// Created by johannes on 15.05.21.
//
#include "./SparqlExpression.h"

#include <cmath>

#include "../engine/CallFixedSize.h"
#include "../util/TupleHelpers.h"
#include "./SparqlExpressionHelpers.h"

namespace sparqlExpression::detail {
using ad_utility::SetOfIntervals;

// This is the visitor for the `evaluateAggregateExpression` function below.
// It works on a `SingleExpressionResult` rather than on the `ExpressionResult`
// variant.
template <typename RangeCalculation, typename ValueExtractor,
          typename AggregateOperation, typename FinalOperation,
          SingleExpressionResult Input>
ExpressionResult evaluateAggregateExpressionOnSingleExpressionResult(
    RangeCalculation rangeCalculation, ValueExtractor valueExtractor,
    AggregateOperation aggregateOperation, FinalOperation finalOperation,
    EvaluationContext* context, bool distinct, Input&& args) {
  // Perform the more efficient range calculation if it is possible.
  if constexpr (rangeCalculationIsAllowed<RangeCalculation, decltype(args)>) {
    return rangeCalculation(std::forward<decltype(args)>(args));
  }

  // The number of inputs we aggregate over
  auto inputSize = getAndVerifyResultSize(*context, args);
  // TODO: one line of comment;
  auto extractor = makeExtractorFromChildResult(std::move(args), inputSize,
                                                context, valueExtractor);

  using ResultType =
      std::decay_t<decltype(aggregateOperation(extractor(0), extractor(0)))>;

  // Compute the actual result.
  ResultType result{};
  if (!distinct) {
    for (size_t i = 0; i < inputSize; ++i) {
      result = aggregateOperation(std::move(result), extractor(i));
    }
    result = finalOperation(std::move(result), inputSize);
    return result;
  } else {
    using HashMapType = std::decay_t<decltype(extractor(0, std::true_type{}))>;
    ad_utility::HashSetWithMemoryLimit<HashMapType> uniqueHashSet(
        context->_allocator);
    for (size_t i = 0; i < inputSize; ++i) {
      const auto& el = extractor(i, std::true_type{});
      if (!uniqueHashSet.contains(el)) {
        uniqueHashSet.insert(extractor(i, std::true_type{}));
        result = aggregateOperation(std::move(result), extractor(i));
      }
    }
    result = finalOperation(std::move(result), uniqueHashSet.size());
    return result;
  }
}

/// Evaluate an aggregate operation on an `ExpressionResult`.
/// For the meaning of the template arguments, see the `SparqlExpression.h`
/// header
template <typename RangeCalculation, typename ValueExtractor,
          typename AggregateOperation, typename FinalOperation>
ExpressionResult evaluateAggregateExpression(
    RangeCalculation rangeCalculation, ValueExtractor valueExtractor,
    AggregateOperation aggregateOperation, FinalOperation finalOperation,
    EvaluationContext* context, bool distinct, ExpressionResult&& childResult) {
  auto visitor = [&](auto&& input) {
    return evaluateAggregateExpressionOnSingleExpressionResult(
        rangeCalculation, valueExtractor, aggregateOperation, finalOperation,
        context, distinct, input);
  };
  return std::visit(visitor, std::move(childResult));
}

// ____________________________________________________________________________
template <typename RangeCalculation, typename ValueExtractor,
          typename BinaryOperation, TagString Tag>
ExpressionResult
BinaryExpression<RangeCalculation, ValueExtractor, BinaryOperation,
                 Tag>::evaluate(EvaluationContext* context) const {
  AD_CHECK(!_children.empty())
  auto result = _children[0]->evaluate(context);

  for (size_t i = 1; i < _children.size(); ++i) {
    result = evaluateNaryOperation(
        RangeCalculation{}, ValueExtractor{}, BinaryOperation{}, context,
        std::move(result), _children[i]->evaluate(context));
  }
  return result;
}

// ___________________________________________________________________________
template <typename RangeCalculation, typename ValueExtractor,
          typename UnaryOperation, TagString Tag>
ExpressionResult
UnaryExpression<RangeCalculation, ValueExtractor, UnaryOperation,
                Tag>::evaluate(EvaluationContext* context) const {
  auto result = _child->evaluate(context);

  return evaluateNaryOperation(RangeCalculation{}, ValueExtractor{},
                               UnaryOperation{}, context, std::move(result));
};

/// Find out, which of the TaggedFunctions matches the `identifier`, and perform
/// it on the given input
template <typename... TagAndFunctions>
auto TaggedFunctionVisitor = []<typename... Args>(TagString identifier,
                                                  auto valueExtractor,
                                                  EvaluationContext* context,
                                                  Args&&... args)
                                 -> ExpressionResult {
  // Does the first tag match?
  std::optional<ExpressionResult> result;
  auto executeIfTagMatches = [&]<typename T>(const T&) {
    if (identifier == T::tag) {
      result = evaluateNaryOperation(
          NoRangeCalculation{}, std::move(valueExtractor),
          typename T::functionType{}, context, std::forward<Args>(args)...);
      return true;
    } else {
      return false;
    }
  };

  bool didAnyTagMatch = (... || executeIfTagMatches(TagAndFunctions{}));
  AD_CHECK(didAnyTagMatch);
  return std::move(*result);
};

// ___________________________________________________________________________
template <typename ValueExtractor, TaggedFunctionConcept... TagAndFunctions>
ExpressionResult
DispatchedBinaryExpression<ValueExtractor, TagAndFunctions...>::evaluate(
    EvaluationContext* context) const {
  auto result = _children[0]->evaluate(context);

  for (size_t i = 1; i < _children.size(); ++i) {
    result = TaggedFunctionVisitor<TagAndFunctions...>(
        _relations[i - 1], ValueExtractor{}, context, std::move(result),
        _children[i]->evaluate(context));
  }
  return result;
};

template <typename RangeCalculation, typename ValueGetter, typename AggregateOp,
          typename FinalOp, TagString Tag>
ExpressionResult
AggregateExpression<RangeCalculation, ValueGetter, AggregateOp, FinalOp,
                    Tag>::evaluate(EvaluationContext* context) const {
  auto childResult = _child->evaluate(context);
  return evaluateAggregateExpression(RangeCalculation{}, ValueGetter{},
                                     _aggregateOp, FinalOp{}, context,
                                     _distinct, std::move(childResult));
}

// Explicit instantiations.
template class UnaryExpression<NoRangeCalculation, EffectiveBooleanValueGetter,
                               decltype(unaryNegate), "!">;
template class UnaryExpression<NoRangeCalculation, NumericValueGetter,
                               decltype(unaryMinus), "unary-">;
template class BinaryExpression<ad_utility::Union, EffectiveBooleanValueGetter,
                                decltype(orLambda), "||">;

template class BinaryExpression<ad_utility::Intersection, EffectiveBooleanValueGetter,
                                decltype(andLambda), "&&">;

template class DispatchedBinaryExpression<
    NumericValueGetter, TaggedFunction<"+", decltype(add)>,
    TaggedFunction<"-", decltype(subtract)>>;

template class DispatchedBinaryExpression<
    NumericValueGetter, TaggedFunction<"*", decltype(multiply)>,
    TaggedFunction<"/", decltype(divide)>>;

template class AggregateExpression<NoRangeCalculation, EffectiveBooleanValueGetter,
                                   decltype(count), decltype(noop), "COUNT">;

template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(add), decltype(noop), "SUM">;

template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(add), decltype(averageFinalOp),
                                   "AVG">;

template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(minLambda), decltype(noop), "MIN">;
template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(maxLambda), decltype(noop), "MAX">;
template class AggregateExpression<NoRangeCalculation, StringValueGetter,
                                   PerformConcat, decltype(noop),
                                   "GROUP_CONCAT">;
}  // namespace sparqlExpression::detail
