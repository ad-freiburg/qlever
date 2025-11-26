//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/sparqlExpressions/SampleExpression.h"

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

using namespace sparqlExpression;
using namespace sparqlExpression::detail;

// ____________________________________________________________________________
ExpressionResult SampleExpression::evaluate(EvaluationContext* context) const {
  auto evaluator = [context](const auto& childResult) -> ExpressionResult {
    using T = std::decay_t<decltype(childResult)>;
    if (getResultSize(*context, childResult) == 0) {
      return Id::makeUndefined();
    }
    if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
      // If there exists an element that is true, return true.
      return Id::makeFromBool(!childResult._intervals.empty());
    } else if constexpr (isVectorResult<T>) {
      AD_CORRECTNESS_CHECK(!childResult.empty());
      return childResult[0];
    } else if constexpr (std::is_same_v<T, ::Variable>) {
      // TODO<joka921> Can't this be a simpler function (getIdAt)
      AD_CORRECTNESS_CHECK(context->_endIndex > context->_beginIndex);
      ql::span<const ValueId> idOfFirstAsVector = detail::getIdsFromVariable(
          childResult, context, context->_beginIndex, context->_endIndex);
      return ExpressionResult{idOfFirstAsVector[0]};
    } else {
      static_assert(isConstantResult<T>);
      return childResult;
    }
  };

  return std::visit(evaluator, _child->evaluate(context));
}
