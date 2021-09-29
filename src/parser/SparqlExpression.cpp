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
template <typename CalculationWithSetOfIntervals, typename... TagAndFunctions>
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
          CalculationWithSetOfIntervals{}, std::move(valueExtractor),
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
template <typename CalculationWithSetOfIntervals, typename ValueExtractor,
          TaggedFunctionConcept... TagAndFunctions>
ExpressionResult DispatchedBinaryExpression<
    CalculationWithSetOfIntervals, ValueExtractor,
    TagAndFunctions...>::evaluate(EvaluationContext* context) const {
  auto result = _children[0]->evaluate(context);

  for (size_t i = 1; i < _children.size(); ++i) {
    result = TaggedFunctionVisitor<CalculationWithSetOfIntervals, TagAndFunctions...>(
        _relations[i - 1], ValueExtractor{}, context, std::move(result),
        _children[i]->evaluate(context));
  }
  return result;
};


// Explicit instantiations.
template class UnaryExpression<NoRangeCalculation, EffectiveBooleanValueGetter,
                               decltype(unaryNegate), "!">;
template class UnaryExpression<NoRangeCalculation, NumericValueGetter,
                               decltype(unaryMinus), "unary-">;

template class DispatchedBinaryExpression<
    ad_utility::Union, EffectiveBooleanValueGetter,
    TaggedFunction<"||", decltype(orLambda)>>;

template class DispatchedBinaryExpression<
    ad_utility::Intersection, EffectiveBooleanValueGetter,
    TaggedFunction<"&&", decltype(andLambda)>>;

template class DispatchedBinaryExpression<
    NoRangeCalculation, NumericValueGetter, TaggedFunction<"+", decltype(add)>,
    TaggedFunction<"-", decltype(subtract)>>;

template class DispatchedBinaryExpression<
    NoRangeCalculation, NumericValueGetter,
    TaggedFunction<"*", decltype(multiply)>,
    TaggedFunction<"/", decltype(divide)>>;

}  // namespace sparqlExpression::detail
