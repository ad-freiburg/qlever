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

// Return the raw value at `i` of a vector operand, or the constant itself.
// Works for every vector or constant result type (unlike `getIdAt` in
// `HomogeneousNumericExpressionHelpers.h`, which is restricted to `Id`s).
template <typename Operand>
decltype(auto) getValueAt(const Operand& operand, size_t i) {
  if constexpr (isVectorResult<std::decay_t<Operand>>) {
    return operand[i];
  } else {
    static_assert(isConstantResult<Operand>);
    return (operand);
  }
}

// Return a callable that provides the converted value at index `i`. For
// constant operands, the converted value is computed only once.
template <typename ValueGetter, typename Operand>
auto makeIndexedValueGetter(Operand&& operand, EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    AD_CORRECTNESS_CHECK(operand.size() == context->size());

    // TODO<Prashanth0703>: The indexed vector elements are passed to the value
    // getter as lvalues. This is irrelevant for the currently used value
    // getters, but should be revisited for move-sensitive value types.
    return [&operand, context](size_t i) {
      return ValueGetter{}(operand[i], context);
    };
  } else {
    // The converted constant is returned as a const rvalue reference, so that
    // functions taking their arguments by const reference or by value bind to
    // it without a copy, and functions that require rvalues (like `IfImpl`)
    // copy from it.
    using Value =
        std::decay_t<decltype(ValueGetter{}(AD_FWD(operand), context))>;
    return [value = ValueGetter{}(AD_FWD(operand), context)](
               size_t) -> const Value&& { return std::move(value); };
  }
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EXPRESSIONRESULTHELPERS_H
