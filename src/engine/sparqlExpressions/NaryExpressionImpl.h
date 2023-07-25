//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include "engine/sparqlExpressions/NaryExpression.h"

namespace sparqlExpression::detail {

// _____________________________________________________________________________
template <typename Op>
requires(isOperation<Op>)
NaryExpression<Op>::NaryExpression(Children&& children)
    : _children{std::move(children)} {}

// _____________________________________________________________________________

template <typename NaryOperation>
requires(isOperation<NaryOperation>)
ExpressionResult NaryExpression<NaryOperation>::evaluate(
    EvaluationContext* context) const {
  auto resultsOfChildren = ad_utility::applyFunctionToEachElementOfTuple(
      [context](const auto& child) { return child->evaluate(context); },
      _children);

  // A function that only takes several `ExpressionResult`s,
  // and evaluates the expression.
  auto evaluateOnChildrenResults =
      std::bind_front(ad_utility::visitWithVariantsAndParameters,
                      evaluateOnChildrenOperands, NaryOperation{}, context);

  return std::apply(evaluateOnChildrenResults, std::move(resultsOfChildren));
}

// _____________________________________________________________________________
template <typename Op>
requires(isOperation<Op>)
std::span<SparqlExpression::Ptr> NaryExpression<Op>::children() {
  return {_children.data(), _children.size()};
}

// __________________________________________________________________________
template <typename Op>
requires(isOperation<Op>) [[nodiscard]] string NaryExpression<Op>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  string key = typeid(*this).name();
  for (const auto& child : _children) {
    key += child->getCacheKey(varColMap);
  }
  return key;
}

#define NARY_EXPRESSION(Name, N, X, ...)                                     \
  class Name : public NaryExpression<detail::Operation<N, X, __VA_ARGS__>> { \
    using Base = NaryExpression<Operation<N, X, __VA_ARGS__>>;               \
    using Base::Base;                                                        \
  };

#define INSTANTIATE_NARY(N, X, ...) \
  template class NaryExpression<Operation<N, X, __VA_ARGS__>>
}
