//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 28.09.21.
//

#include "AggregateExpression.h"
#include "./SparqlExpressionHelpers.h"

namespace sparqlExpression {

namespace detail {

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
  if constexpr (detail::rangeCalculationIsAllowed<RangeCalculation, decltype(args)>) {
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
}

// ______________________________________________________________________________
// TODO<joka921> Commentd
inline auto makePerformConcat(std::string separator) {
  return [sep = std::move(separator)](string&& a, const string& b) -> string {
    if (a.empty()) [[unlikely]] {
      return b;
    } else [[likely]] {
      a.append(sep);
      a.append(std::move(b));
      return std::move(a);
    }
  };
}

// TODO<joka921> Comment
using PerformConcat = std::decay_t<decltype(makePerformConcat(std::string{}))>;

GroupConcatExpression::GroupConcatExpression(bool distinct, Ptr&& child,
                                             std::string separator)
    : _separator{std::move(separator)} {
  auto performConcat = makePerformConcat(_separator);

  using G = detail::AggregateExpression<detail::NoRangeCalculation, detail::StringValueGetter,
                                PerformConcat, decltype(detail::noop), "GROUP_CONCAT">;
  _actualExpression = std::make_unique<G>(distinct, std::move(child), performConcat);
}

// __________________________________________________________________________
ExpressionResult GroupConcatExpression::evaluate(EvaluationContext* context) const {
  // The child is already set up to perform all the work.
  return _actualExpression->evaluate(context);
}

namespace detail {
// explicit instantiations for the aggregates

template class AggregateExpression<NoRangeCalculation,
                                   EffectiveBooleanValueGetter, decltype(count),
                                   decltype(noop), "COUNT">;

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
}

} // namespace sparqlExpression