//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 15.09.21.
//

#include "./SampleExpression.h"

#include "./SparqlExpressionGenerators.h"

using namespace sparqlExpression;
using namespace sparqlExpression::detail;

// ____________________________________________________________________________
ExpressionResult SampleExpression::evaluate(EvaluationContext* context) const {
  // The child is already set up to perform all the work.
  auto childResultVariant = _child->evaluate(context);
  auto evaluator =
      [context]<typename T>(const T& childResult) -> ExpressionResult {
    if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
      // If any element is true, then we sample this element.
      return !childResult._intervals.empty();
    } else if constexpr (isVectorResult<T>) {
      AD_CHECK(!childResult.empty());
      return T({childResult[0]}, context->_allocator);
    } else if constexpr (std::is_same_v<T, Variable>) {
      AD_CHECK(context->_endIndex > context->_beginIndex);
      EvaluationContext newInput = *context;
      newInput._endIndex = newInput._beginIndex + 1;
      auto idOfFirstAsVector =
          detail::getIdsFromVariable(childResult, &newInput);
      return StrongIdWithResultType{
          idOfFirstAsVector[0],
          context->_variableToColumnAndResultTypeMap.at(childResult._variable)
              .second};
    } else {
      static_assert(isConstantResult<T>);
      return childResult;
    }
  };

  return std::visit(evaluator, std::move(childResultVariant));
}
