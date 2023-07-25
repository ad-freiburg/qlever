//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"

namespace sparqlExpression {
namespace detail {

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

INSTANTIATE_NARY(2, FV<decltype(orLambda), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Union>);

INSTANTIATE_NARY(2, FV<decltype(andLambda), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Intersection>);

INSTANTIATE_NARY(1, FV<decltype(unaryNegate), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Complement>);

INSTANTIATE_NARY(1, FV<decltype(unaryMinus), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(multiply), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(divide), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(add), NumericValueGetter>);

INSTANTIATE_NARY(2, FV<decltype(subtract), NumericValueGetter>);

INSTANTIATE_NARY(1, FV<decltype(abs), NumericValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(round), NumericValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(ceil), NumericValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(floor), NumericValueGetter>);

INSTANTIATE_NARY(1,
                 FV<NumericIdWrapper<decltype(ad_utility::wktLongitude), true>,
                    StringValueGetter>);
INSTANTIATE_NARY(1,
                 FV<NumericIdWrapper<decltype(ad_utility::wktLatitude), true>,
                    StringValueGetter>);
INSTANTIATE_NARY(2, FV<NumericIdWrapper<decltype(ad_utility::wktDist), true>,
                       StringValueGetter>);

INSTANTIATE_NARY(1, FV<decltype(extractYear), DateValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(extractMonth), DateValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(extractDay), DateValueGetter>);
INSTANTIATE_NARY(1, FV<std::identity, StringValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(strlen), StringValueGetter>);

NARY_EXPRESSION(AbsExpressionInternal, 1,
                FV<decltype(abs), NumericValueGetter>);
}  // namespace detail

SparqlExpression::Ptr makeAddExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2) {
  return std::make_unique<AddExpression>(std::move(child1), std::move(child2));
}
SparqlExpression::Ptr makeAndExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2) {
  return std::make_unique<AndExpression>(std::move(child1), std::move(child2));
}

SparqlExpression::Ptr makeDivideExpression(SparqlExpression::Ptr child1,
                                           SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeMultiplyExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeOrExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeSubtractExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeUnaryMinusExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeUnaryNegateExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeRoundExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeAbsExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeCeilExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeFloorExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeDayExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeMonthExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeYearExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeStrExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeStrlenExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeRoundExpression(SparqlExpression::Ptr child) {
  return std::make_unique<RoundExpression>(std::move(child));
}
SparqlExpression::Ptr makeAbsExpression(SparqlExpression::Ptr child) {
  return std::make_unique<detail::AbsExpressionInternal>(std::move(child));
}
SparqlExpression::Ptr makeCeilExpression(SparqlExpression::Ptr child) {
  return std::make_unique<CeilExpression>(std::move(child));
}
SparqlExpression::Ptr makeFloorExpression(SparqlExpression::Ptr child) {
  return std::make_unique<FloorExpression>(std::move(child));
}
}  // namespace sparqlExpression
