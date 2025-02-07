//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <ranges>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail {

// _____________________________________________________________________________
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

CPP_template(typename NaryOperation)(
    requires isOperation<NaryOperation>) class UnaryNegateExpressionImpl
    : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      bool isNegated) const override {
    AD_CORRECTNESS_CHECK(this->N == 1);
    namespace p = prefilterExpressions;
    // The bool flag isNegated (by default false) acts as decision variable
    // to select the correct merging procedure while constructing the
    // PrefilterExpression(s) for a binary expression (AND or OR). For the
    // negation procedure, we apply (partially over multiple Variables)
    // De-Morgans law w.r.t. the affected (lower) expression parts, and
    // isNegated indicates if we should apply it (or not). For more detailed
    // information see NumericBinaryExpressions.cpp. For UnaryNegate we have to
    // toggle the value of isNegated to pass the respective negation information
    // down the expression tree.
    // Remark:
    // - Expression sub-tree has an even number of NOT expressions as
    // parents: the negation cancels out (isNegated = false).
    // - For an uneven number of NOT expressions as parent nodes: the
    // sub-tree is actually negated (isNegated = true).
    //
    // Example - Apply De-Morgans law in two steps (see (1) and (2)) on
    // expression !(?x >= IntId(10) || ?y >= IntId(10)) (SparqlExpression)
    // With De-Morgan's rule we retrieve: ?x < IntId(10) && ?y < IntId(10)
    //
    // (1) Merge {<(>= IntId(10)), ?x>} and {<(>= IntId(10)), ?y>}
    // with mergeChildrenForBinaryOpExpressionImpl (defined in
    // NumericBinaryExpressions.cpp), which we select based on isNegated = true
    // (first part of De-Morgans law).
    // Result (1): {<(>= IntId(10)), ?x>, <(>= IntId(10)), ?y>}
    //
    // (2) On each pair <PrefilterExpression, Variable> given the result from
    // (1), apply NotExpression (see the following implementation part).
    // Step by step for the given example:
    // {<(>= IntId(10)), ?x>, <(>= IntId(10)), ?y>} (apply NotExpression) =>
    // {<(!(>= IntId(10))), ?x>, <(!(>= IntId(10))), ?y>}
    // => Result (2): {<(< IntId(10)), ?x>, <(< IntId(10)), ?y>}
    auto child =
        this->getNthChild(0).value()->getPrefilterExpressionForMetadata(
            !isNegated);
    ql::ranges::for_each(
        child | ql::views::keys,
        [](std::unique_ptr<p::PrefilterExpression>& expression) {
          expression =
              std::make_unique<p::NotExpression>(std::move(expression));
        });
    p::detail::checkPropertiesForPrefilterConstruction(child);
    return child;
  }
};

using UnaryNegateExpression = UnaryNegateExpressionImpl<
    Operation<1, FV<decltype(unaryNegate), EffectiveBooleanValueGetter>,
              SET<SetOfIntervals::Complement>>>;

// _____________________________________________________________________________
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
  if constexpr (ad_utility::FloatingPoint<T>) {
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
  if constexpr (ad_utility::FloatingPoint<T>) {
    return std::ceil(num);
  } else {
    return num;
  }
};
inline const auto ceil = makeNumericExpression<decltype(ceilImpl)>();
NARY_EXPRESSION(CeilExpression, 1, FV<decltype(ceil), NumericValueGetter>);

// Flooring.
inline const auto floorImpl = []<typename T>(T num) {
  if constexpr (ad_utility::FloatingPoint<T>) {
    return std::floor(num);
  } else {
    return num;
  }
};
inline const auto floor = makeNumericExpression<decltype(floorImpl)>();
using FloorExpression = NARY<1, FV<decltype(floor), NumericValueGetter>>;

// Natural Logarithm.
inline const auto logImpl = []<typename T>(T num) { return std::log(num); };
inline const auto log = makeNumericExpression<decltype(logImpl)>();
using LogExpression = NARY<1, FV<decltype(log), NumericValueGetter>>;

// Exponentiation.
inline const auto expImpl = []<typename T>(T num) { return std::exp(num); };
inline const auto exp = makeNumericExpression<decltype(expImpl)>();
using ExpExpression = NARY<1, FV<decltype(exp), NumericValueGetter>>;

// Square root.
inline const auto sqrtImpl = []<typename T>(T num) { return std::sqrt(num); };
inline const auto sqrt = makeNumericExpression<decltype(sqrtImpl)>();
using SqrtExpression = NARY<1, FV<decltype(sqrt), NumericValueGetter>>;

// Sine.
inline const auto sinImpl = []<typename T>(T num) { return std::sin(num); };
inline const auto sin = makeNumericExpression<decltype(sinImpl)>();
using SinExpression = NARY<1, FV<decltype(sin), NumericValueGetter>>;

// Cosine.
inline const auto cosImpl = []<typename T>(T num) { return std::cos(num); };
inline const auto cos = makeNumericExpression<decltype(cosImpl)>();
using CosExpression = NARY<1, FV<decltype(cos), NumericValueGetter>>;

// Tangent.
inline const auto tanImpl = []<typename T>(T num) { return std::tan(num); };
inline const auto tan = makeNumericExpression<decltype(tanImpl)>();
using TanExpression = NARY<1, FV<decltype(tan), NumericValueGetter>>;

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
SparqlExpression::Ptr makeLogExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LogExpression>(std::move(child));
}
SparqlExpression::Ptr makeExpExpression(SparqlExpression::Ptr child) {
  return std::make_unique<ExpExpression>(std::move(child));
}
SparqlExpression::Ptr makeSqrtExpression(SparqlExpression::Ptr child) {
  return std::make_unique<SqrtExpression>(std::move(child));
}
SparqlExpression::Ptr makeSinExpression(SparqlExpression::Ptr child) {
  return std::make_unique<SinExpression>(std::move(child));
}
SparqlExpression::Ptr makeCosExpression(SparqlExpression::Ptr child) {
  return std::make_unique<CosExpression>(std::move(child));
}
SparqlExpression::Ptr makeTanExpression(SparqlExpression::Ptr child) {
  return std::make_unique<TanExpression>(std::move(child));
}

SparqlExpression::Ptr makeUnaryMinusExpression(SparqlExpression::Ptr child) {
  return std::make_unique<UnaryMinusExpression>(std::move(child));
}

SparqlExpression::Ptr makeUnaryNegateExpression(SparqlExpression::Ptr child) {
  return std::make_unique<UnaryNegateExpression>(std::move(child));
}

}  // namespace sparqlExpression
