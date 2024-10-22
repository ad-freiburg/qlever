//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

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

// _____________________________________________________________________________
// Power.
inline auto powImpl = [](double base, double exp) {
  return std::pow(base, exp);
};
inline auto pow = makeNumericExpression<decltype(powImpl)>();
NARY_EXPRESSION(PowExpression, 2, FV<decltype(pow), NumericValueGetter>);

// OR and AND
// _____________________________________________________________________________
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

// _____________________________________________________________________________
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

namespace constructPrefilterExpr {

// _____________________________________________________________________________
template <typename BinaryPrefilterExpr>
std::optional<std::vector<PrefilterExprVariablePair>> mergeChildrenAndLogicImpl(
    std::optional<std::vector<PrefilterExprVariablePair>>&& leftChildExprs,
    std::optional<std::vector<PrefilterExprVariablePair>>&& rightChildExprs) {
  if (!leftChildExprs.has_value() || !rightChildExprs.has_value()) {
    // If we have two children that yield no PrefilterExpressions, then there
    // is also no PrefilterExpression over the AND (&&) conjunction available.
    if (!leftChildExprs.has_value() && !rightChildExprs.has_value()) {
      return std::nullopt;
    }
    // Given that only one of the children returned a PrefilterExpression
    // vector and the pre-filter construction logic of AND (&&), we can just
    // return the corresponding (non-empty) vector.
    //
    // EXAMPLE why this works:
    // Assume that left is - {(>=5, ?x), (<=6, ?y)}
    // and right is - std::nullopt ({undefinable prefilter})
    // Remark: left represents the expression ?x >= 5 && ?y <= 6
    //
    // For this AND conjunction {(>=5, ?x), (<=6, ?y)} && {undef.
    // prefilter}, we can at least (partly) pre-define the plausible true
    // evaluation range with the left child. This is because the respective
    // expressions contained in left child must always yield true
    // for making this AND conjunction expression true.
    return leftChildExprs.has_value() ? std::move(leftChildExprs.value())
                                      : std::move(rightChildExprs.value());
  }
  // Merge the PrefilterExpression vectors from the left and right child.
  // Remark: The vectors contain std::pairs, sorted by their respective
  // variable to the PrefilterExpression.
  auto& expressionsLeft = leftChildExprs.value();
  auto& expressionsRight = rightChildExprs.value();
  auto itLeft = expressionsLeft.begin();
  auto itRight = expressionsRight.begin();
  std::vector<PrefilterExprVariablePair> resPairs;
  while (itLeft != expressionsLeft.end() && itRight != expressionsRight.end()) {
    auto& [exprLeft, varLeft] = *itLeft;
    auto& [exprRight, varRight] = *itRight;
    // For our pre-filtering logic over index scans, we need exactly one
    // PrefilterExpression for each of the variables. Thus if the left and right
    // child contain a PrefilterExpression w.r.t. the same variable, combine
    // them here over a (new) prefilter AndExpression.
    if (varLeft == varRight) {
      resPairs.emplace_back(std::make_unique<BinaryPrefilterExpr>(
                                std::move(exprLeft), std::move(exprRight)),
                            varLeft);
      ++itLeft;
      ++itRight;
    } else if (varLeft < varRight) {
      resPairs.emplace_back(std::move(*itLeft));
      ++itLeft;
    } else {
      resPairs.emplace_back(std::move(*itRight));
      ++itRight;
    }
  }
  std::ranges::move(itLeft, expressionsLeft.end(),
                    std::back_inserter(resPairs));
  std::ranges::move(itRight, expressionsRight.end(),
                    std::back_inserter(resPairs));
  return resPairs;
}

//______________________________________________________________________________
template <typename BinaryPrefilterExpr>
std::optional<std::vector<PrefilterExprVariablePair>> mergeChildrenOrLogicImpl(
    std::optional<std::vector<PrefilterExprVariablePair>>&& leftChildExprs,
    std::optional<std::vector<PrefilterExprVariablePair>>&& rightChildExprs) {
  if (!leftChildExprs.has_value() || !rightChildExprs.has_value()) {
    // If one of the children yields no PrefilterExpressions, we simply can't
    // further define a PrefilterExpression with this OR (||) conjunction.
    //
    // EXAMPLE / REASON:
    // Assume that left is - {(=10, ?x), (=10, ?y)}
    // and right is - std::nullopt ({undefinable prefilter})
    // Remark: left represents the expression ?x = 10 && ?y = 10.
    // We can't define a PrefilterExpression here because left must not always
    // yield true when this OR expression evaluates to true (because right can
    // plausibly yield true).
    // And because we can't definitely say that one child (and which of the two
    // children) must be true, pre-filtering w.r.t. underlying compressed blocks
    // is not possible here.
    // In short: OR has multiple configurations that yield true w.r.t. two child
    // expressions => we can't define a distinct PrefilterExpression.
    return std::nullopt;
  }
  auto& expressionsLeft = leftChildExprs.value();
  auto& expressionsRight = rightChildExprs.value();
  auto itLeft = expressionsLeft.begin();
  auto itRight = expressionsRight.begin();
  std::vector<PrefilterExprVariablePair> resPairs;
  while (itLeft != expressionsLeft.end() && itRight != expressionsRight.end()) {
    auto& [exprLeft, varLeft] = *itLeft;
    auto& [exprRight, varRight] = *itRight;
    // The logic that is implemented with this loop.
    //
    // EXAMPLE 1
    // left child: {(>=5, ?y)}
    // right child: {(<=3, ?x)}
    // We want to pre-filter the blocks for the expression: ?y >= 5 || ?x <= 3
    // No PrefilterExpression will be returned.
    //
    // EXAMPLE 2
    // left child: {(>=5, ?x)}
    // right child: {(=0, ?x)}
    // We want to pre-filter the blocks to the expression: ?x >= 5 || ?x = 0
    // The resulting PrefilterExpression is {((>=5 OR =0), ?x)}
    //
    // EXAMPLE 3
    // left child: {(>=10, ?x), (!=0, ?y)}
    // right child: {(<= 0, ?x)}
    // We have to construct a PrefilterExpression for (?x >= 10 && ?y != 0) ||
    // ?x <= 0. If this OR expression yields true, at least ?x >= 10 || ?x <= 0
    // must be staisifed; for this objective we can construct a
    // PrefiterExpression. We can't make a distinct prediction w.r.t. ?y != 0
    // => not relevant for the PrefilterExpression. Thus, we return the
    // PrefilterExpresion {((>= 10 OR <= 0), ?x)}.
    if (varLeft == varRight) {
      resPairs.emplace_back(std::make_unique<BinaryPrefilterExpr>(
                                std::move(exprLeft), std::move(exprRight)),
                            varLeft);
      ++itLeft;
      ++itRight;
    } else if (varLeft < varRight) {
      ++itLeft;
    } else {
      ++itRight;
    }
  }
  if (resPairs.empty()) {
    return std::nullopt;
  }
  return resPairs;
}

template <typename BinaryPrefilterExpr>
constexpr auto getCombineLogic(bool isNegated) {
  if constexpr (std::is_same_v<BinaryPrefilterExpr,
                               prefilterExpressions::AndExpression>) {
    // This follows the principles of De Morgan's law.
    // If this is a child of a NotExpression, we have to swap the combination
    // procedure (swap AND(&&) and OR(||) respectively). This equals a partial
    // application of De Morgan's law. On the resulting PrefilterExpressions,
    // NOT is applied in UnaryNegateExpression(Impl). For more details take a
    // look at the implementation in NumericUnaryExpressions.cpp
    return !isNegated ? mergeChildrenAndLogicImpl<BinaryPrefilterExpr>
                      : mergeChildrenOrLogicImpl<BinaryPrefilterExpr>;
  } else {
    static_assert(std::is_same_v<BinaryPrefilterExpr,
                                 prefilterExpressions::OrExpression>);
    return !isNegated ? mergeChildrenOrLogicImpl<BinaryPrefilterExpr>
                      : mergeChildrenAndLogicImpl<BinaryPrefilterExpr>;
  }
}
}  //  namespace constructPrefilterExpr

//______________________________________________________________________________
template <typename BinaryPrefilterExpr, typename NaryOperation>
requires(isOperation<NaryOperation> &&
         prefilterExpressions::check_is_logical_v<BinaryPrefilterExpr>)
class LogicalBinaryExpressionImpl : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;

  std::optional<std::vector<PrefilterExprVariablePair>>
  getPrefilterExpressionForMetadata(bool isNegated) const override {
    AD_CORRECTNESS_CHECK(this->N == 2);
    auto leftChild =
        this->getNthChild(0).value()->getPrefilterExpressionForMetadata(
            isNegated);
    auto rightChild =
        this->getNthChild(1).value()->getPrefilterExpressionForMetadata(
            isNegated);
    return constructPrefilterExpr::getCombineLogic<BinaryPrefilterExpr>(
        isNegated)(std::move(leftChild), std::move(rightChild));
  }
};

//______________________________________________________________________________
using AndExpression = LogicalBinaryExpressionImpl<
    prefilterExpressions::AndExpression,
    Operation<2, FV<decltype(andLambda), EffectiveBooleanValueGetter>,
              SET<SetOfIntervals::Intersection>>>;

using OrExpression = LogicalBinaryExpressionImpl<
    prefilterExpressions::OrExpression,
    Operation<2, FV<decltype(orLambda), EffectiveBooleanValueGetter>,
              SET<SetOfIntervals::Union>>>;

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

SparqlExpression::Ptr makePowExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2) {
  return std::make_unique<PowExpression>(std::move(child1), std::move(child2));
}

}  // namespace sparqlExpression
