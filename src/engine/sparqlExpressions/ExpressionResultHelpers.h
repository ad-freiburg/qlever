// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EXPRESSIONRESULTHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EXPRESSIONRESULTHELPERS_H

#include <type_traits>

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/Forward.h"

namespace sparqlExpression::detail {

// Convert an expression result into either a vector-like or constant
// representation that can be accessed directly during expression evaluation.
// Variables and interval sets are materialized as `ValueId` vectors, while
// existing vectors and constants are forwarded unchanged.
template <typename T>
decltype(auto) convertToVectorOrConstant(T&& value,
                                         EvaluationContext* context) {
  using Type = std::decay_t<T>;

  if constexpr (ad_utility::isSimilar<Type, ad_utility::SetOfIntervals>) {
    AD_CORRECTNESS_CHECK(
        value.size() == context->size(),
        "The size of a `SetOfIntervals` does not match the size of the "
        "evaluation context.");
    return ad_utility::SetOfIntervals::toIdVector(value, context->_allocator);
  } else if constexpr (ad_utility::isSimilar<Type, ::Variable>) {
    return getIdsFromVariable(value, context);
  } else {
    static_assert(isVectorResult<Type> || isConstantResult<Type>);
    return AD_FWD(value);
  }
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EXPRESSIONRESULTHELPERS_H
