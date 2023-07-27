//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail {
// Multiplication.
inline auto multiply = makeNumericExpression<std::multiplies<>>();
NARY_EXPRESSION(MultiplyExpression, 2,
                FV<decltype(multiply), NumericValueGetter>);

// Division.
//
// TODO<joka921> If `b == 0` this is technically undefined behavior and
// should lead to an expression error in SPARQL. Fix this as soon as we
// introduce the proper semantics for expression errors.
// Update: I checked it, and the standard differentiates between `xsd:decimal`
// (error) and `xsd:float/xsd:double` where we have `NaN` and `inf` results. We
// currently implement the latter behavior. Note: The result of a division in
// SPARQL is always a decimal number, so there is no integer division.
[[maybe_unused]] inline auto divideImpl = [](auto x, auto y) {
  return static_cast<double>(x) / static_cast<double>(y);
};
inline auto divide = makeNumericExpression<decltype(divideImpl)>();
NARY_EXPRESSION(DivideExpression, 2, FV<decltype(divide), NumericValueGetter>);

// Addition and subtraction, currently all results are converted to double.
inline auto add = makeNumericExpression<std::plus<>>();
NARY_EXPRESSION(AddExpression, 2, FV<decltype(add), NumericValueGetter>);

inline auto subtract = makeNumericExpression<std::minus<>>();
NARY_EXPRESSION(SubtractExpression, 2,
                FV<decltype(subtract), NumericValueGetter>);

// Or
inline auto orLambda = [](TernaryBool a, TernaryBool b) {
  using enum TernaryBool;
  if (a == True || b == True) {
    return Id::makeFromBool(true);
  }
  if (a == False && b == False) {
    return Id::makeFromBool(false);
  }
  return Id::makeUndefined();
};

NARY_EXPRESSION(OrExpression, 2,
                FV<decltype(orLambda), EffectiveBooleanValueGetter>,
                SET<SetOfIntervals::Union>);

// And
inline auto andLambda = [](TernaryBool a, TernaryBool b) {
  using enum TernaryBool;
  if (a == True && b == True) {
    return Id::makeFromBool(true);
  }
  if (a == False || b == False) {
    return Id::makeFromBool(false);
  }
  return Id::makeUndefined();
};
NARY_EXPRESSION(AndExpression, 2,
                FV<decltype(andLambda), EffectiveBooleanValueGetter>,
                SET<SetOfIntervals::Intersection>);

}  // namespace detail

using namespace detail;
SparqlExpression::Ptr makeAddExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2) {
  return std::make_unique<AddExpression>(std::move(child1), std::move(child2));
}

SparqlExpression::Ptr makeDivideExpression(SparqlExpression::Ptr child1,
                                           SparqlExpression::Ptr child2) {
  return std::make_unique<DivideExpression>(std::move(child1),
                                            std::move(child2));
}
SparqlExpression::Ptr makeMultiplyExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2) {
  return std::make_unique<MultiplyExpression>(std::move(child1),
                                              std::move(child2));
}
SparqlExpression::Ptr makeSubtractExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2) {
  return std::make_unique<SubtractExpression>(std::move(child1),
                                              std::move(child2));
}

SparqlExpression::Ptr makeAndExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2) {
  return std::make_unique<AndExpression>(std::move(child1), std::move(child2));
}
SparqlExpression::Ptr makeOrExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2) {
  return std::make_unique<OrExpression>(std::move(child1), std::move(child2));
}
}  // namespace sparqlExpression
