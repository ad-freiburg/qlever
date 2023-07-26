//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail {

// Unary negation.
inline auto unaryNegate = [](TernaryBool a) {
  using enum TernaryBool;
  switch (a) {
    case True:
      return Id::makeFromBool(false);
    case False:
      return Id::makeFromBool(true);
    case Undef:
      return Id::makeUndefined();
  }
  AD_FAIL();
};
NARY_EXPRESSION(UnaryNegateExpression, 1,
                FV<decltype(unaryNegate), EffectiveBooleanValueGetter>,
                SET<SetOfIntervals::Complement>);

// Unary Minus.
inline auto unaryMinus = makeNumericExpression<std::negate<>>();
NARY_EXPRESSION(UnaryMinusExpression, 1,
                FV<decltype(unaryMinus), NumericValueGetter>);
// Abs
inline const auto absImpl = []<typename T>(T num) { return std::abs(num); };
inline const auto abs = makeNumericExpression<decltype(absImpl)>();
NARY_EXPRESSION(AbsExpression, 1, FV<decltype(abs), NumericValueGetter>);

// Rounding.
inline const auto roundImpl = []<typename T>(T num) {
  if constexpr (std::is_floating_point_v<T>) {
    auto res = std::round(num);
    // In SPARQL, negative numbers are rounded towards zero if they lie exactly
    // between two integers.
    return (num < 0 && std::abs(res - num) == 0.5) ? res + 1 : res;
  } else {
    return num;
  }
};

inline const auto round = makeNumericExpression<decltype(roundImpl)>();
NARY_EXPRESSION(RoundExpression, 1, FV<decltype(round), NumericValueGetter>);

// Ceiling.
inline const auto ceilImpl = []<typename T>(T num) {
  if constexpr (std::is_floating_point_v<T>) {
    return std::ceil(num);
  } else {
    return num;
  }
};
inline const auto ceil = makeNumericExpression<decltype(ceilImpl)>();
NARY_EXPRESSION(CeilExpression, 1, FV<decltype(ceil), NumericValueGetter>);

// Flooring.
inline const auto floorImpl = []<typename T>(T num) {
  if constexpr (std::is_floating_point_v<T>) {
    return std::floor(num);
  } else {
    return num;
  }
};
inline const auto floor = makeNumericExpression<decltype(floorImpl)>();
using FloorExpression = NARY<1, FV<decltype(floor), NumericValueGetter>>;
}  // namespace detail

using namespace detail;
SparqlExpression::Ptr makeRoundExpression(SparqlExpression::Ptr child) {
  return std::make_unique<RoundExpression>(std::move(child));
}
SparqlExpression::Ptr makeAbsExpression(SparqlExpression::Ptr child) {
  return std::make_unique<AbsExpression>(std::move(child));
}
SparqlExpression::Ptr makeCeilExpression(SparqlExpression::Ptr child) {
  return std::make_unique<CeilExpression>(std::move(child));
}
SparqlExpression::Ptr makeFloorExpression(SparqlExpression::Ptr child) {
  return std::make_unique<FloorExpression>(std::move(child));
}

SparqlExpression::Ptr makeUnaryMinusExpression(SparqlExpression::Ptr child) {
  return std::make_unique<UnaryMinusExpression>(std::move(child));
}

SparqlExpression::Ptr makeUnaryNegateExpression(SparqlExpression::Ptr child) {
  return std::make_unique<UnaryNegateExpression>(std::move(child));
}

}  // namespace sparqlExpression
