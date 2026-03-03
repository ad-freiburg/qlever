//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail {

// _____________________________________________________________________________
// Unary negation.
struct UnaryNegate {
  Id operator()(TernaryBool a) const {
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
  }
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
    auto child = this->children()[0].get()->getPrefilterExpressionForMetadata(
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
    Operation<1, FV<UnaryNegate, EffectiveBooleanValueGetter>,
              SET<SetOfIntervals::Complement>>>;

// _____________________________________________________________________________
// Unary Minus.
using UnaryMinus = MakeNumericExpression<std::negate<>>;
NARY_EXPRESSION(UnaryMinusExpression, 1, FV<UnaryMinus, NumericValueGetter>);
// Abs
struct AbsImpl {
  template <typename T>
  auto operator()(T num) const {
    return std::abs(num);
  }
};
using Abs = MakeNumericExpression<AbsImpl>;
NARY_EXPRESSION(AbsExpression, 1, FV<Abs, NumericValueGetter>);

// Rounding.
struct RoundImpl {
  template <typename T>
  auto operator()(T num) const {
    if constexpr (ql::concepts::floating_point<T>) {
      auto res = std::round(num);
      // In SPARQL, negative numbers are rounded towards zero if they lie
      // exactly between two integers.
      return (num < 0 && std::abs(res - num) == 0.5) ? res + 1 : res;
    } else {
      return num;
    }
  }
};

using Round = MakeNumericExpression<RoundImpl>;
NARY_EXPRESSION(RoundExpression, 1, FV<Round, NumericValueGetter>);

// Ceiling.
struct CeilImpl {
  template <typename T>
  auto operator()(T num) const {
    if constexpr (ql::concepts::floating_point<T>) {
      return std::ceil(num);
    } else {
      return num;
    }
  }
};
using Ceil = MakeNumericExpression<CeilImpl>;
NARY_EXPRESSION(CeilExpression, 1, FV<Ceil, NumericValueGetter>);

// Flooring.
struct FloorImpl {
  template <typename T>
  auto operator()(T num) const {
    if constexpr (ql::concepts::floating_point<T>) {
      return std::floor(num);
    } else {
      return num;
    }
  }
};
using Floor = MakeNumericExpression<FloorImpl>;
using FloorExpression = NARY<1, FV<Floor, NumericValueGetter>>;

// Natural Logarithm.
struct LogImpl {
  template <typename T>
  auto operator()(T num) const {
    return std::log(num);
  }
};
using Log = MakeNumericExpression<LogImpl>;
using LogExpression = NARY<1, FV<Log, NumericValueGetter>>;

// Exponentiation.
struct ExpImpl {
  template <typename T>
  auto operator()(T num) const {
    return std::exp(num);
  }
};
using Exp = MakeNumericExpression<ExpImpl>;
using ExpExpression = NARY<1, FV<Exp, NumericValueGetter>>;

// Square root.
struct SqrtImpl {
  template <typename T>
  auto operator()(T num) const {
    return std::sqrt(num);
  }
};
using Sqrt = MakeNumericExpression<SqrtImpl>;
using SqrtExpression = NARY<1, FV<Sqrt, NumericValueGetter>>;

// Sine.
struct SinImpl {
  template <typename T>
  auto operator()(T num) const {
    return std::sin(num);
  }
};
using Sin = MakeNumericExpression<SinImpl>;
using SinExpression = NARY<1, FV<Sin, NumericValueGetter>>;

// Cosine.
struct CosImpl {
  template <typename T>
  auto operator()(T num) const {
    return std::cos(num);
  }
};
using Cos = MakeNumericExpression<CosImpl>;
using CosExpression = NARY<1, FV<Cos, NumericValueGetter>>;

// Tangent.
struct TanImpl {
  template <typename T>
  auto operator()(T num) const {
    return std::tan(num);
  }
};
using Tan = MakeNumericExpression<TanImpl>;
using TanExpression = NARY<1, FV<Tan, NumericValueGetter>>;

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
