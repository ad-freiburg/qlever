//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 15.09.21.
//

#include "./SampleExpression.h"

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

using namespace sparqlExpression;
using namespace sparqlExpression::detail;

// ____________________________________________________________________________
ExpressionResult SampleExpression::evaluate(EvaluationContext* context) const {
  auto evaluator =
      [context]<typename T>(const T& childResult) -> ExpressionResult {
    if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
      // If there exists an element that is true, return true.
      return Bool{!childResult._intervals.empty()};
    } else if constexpr (isVectorResult<T>) {
      AD_CHECK(!childResult.empty());
      return childResult[0];
    } else if constexpr (std::is_same_v<T, ::Variable>) {
      // TODO<joka921> Can't this be a simpler function (getIdAt)
      AD_CHECK(context->_endIndex > context->_beginIndex);
      EvaluationContext contextForSingleValue = *context;
      contextForSingleValue._endIndex = contextForSingleValue._beginIndex + 1;
      auto idOfFirstAsVector =
          detail::getIdsFromVariable(childResult, &contextForSingleValue);
      return StrongIdWithResultType{
          idOfFirstAsVector[0],
          context->_variableToColumnAndResultTypeMap.at(childResult.name())
              .second};
    } else {
      static_assert(isConstantResult<T>);
      return childResult;
    }
  };

  return std::visit(evaluator, _child->evaluate(context));
}
