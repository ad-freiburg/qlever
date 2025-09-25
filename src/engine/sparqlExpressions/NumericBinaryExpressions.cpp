//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/RuntimeParameters.h"

namespace sparqlExpression {
namespace detail {
// Multiplication.
inline auto multiply = makeNumericExpression<std::multiplies<>>();
NARY_EXPRESSION(MultiplyExpression, 2,
                FV<decltype(multiply), NumericValueGetter>);

// Division.
//
// TODO<joka921> If `b == 0` the the behavior of the division depends on whether
// the inputs are `xsd:decimal` or `xsd:double` (`double`s have special values
// like `NaN` or `infinity`, decimals don't). As we currently make no difference
// between those two types, we have to choose one of the behaviors. We make the
// result `UNDEF` in this case to pass the sparql conformance tests that rely on
// this behavior. The old behavior can be reinstated by a RuntimeParameter.
// Note: The result of a division in SPARQL is always a decimal number, so there
// is no integer division.
[[maybe_unused]] inline auto divideImpl = [](auto x, auto y) {
  return static_cast<double>(x) / static_cast<double>(y);
};

inline auto divide1 = makeNumericExpression<decltype(divideImpl), true>();
NARY_EXPRESSION(DivideExpressionByZeroIsUndef, 2,
                FV<decltype(divide1), NumericValueGetter>);

inline auto divide2 = makeNumericExpression<decltype(divideImpl), false>();
NARY_EXPRESSION(DivideExpressionByZeroIsNan, 2,
                FV<decltype(divide2), NumericValueGetter>);

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
namespace {

// `BinaryOperator` defines the operations `AND` and `OR`.
using BinaryOperator = prefilterExpressions::LogicalOperator;

// MERGE <BinaryOperator::AND, AndExpression>
// _____________________________________________________________________________
// For our pre-filtering logic over index scans, we need exactly one
// corresponding PrefilterExpression for each relevant Variable. Thus, if the
// left and right child contain a PrefilterExpression w.r.t. the same Variable,
// combine them here for an AND conjunction. In the following, three examples
// are given on how the following function merges the content of the left and
// right child.
//
// EXAMPLE 1
// left child: {<(>= 10), ?x>, <(!= 5), ?y>}; (?x >= 10 && ?y != 5)
// right child: {}; (std::nullopt)
// The AND conjunction can only evaluate to true, if both child expressions
// evaluate to true. For the given example, we have no PrefilterExpression
// for the right child, but we can certainly assume that the left child
// expression must evaluate to true.
// Thus, return {<(>= 10), ?x>, <(!= 5), ?y>}.
//
// EXAMPLE 2
// left child: {<(= 5), ?x>}; (?x = 5)
// right child: {<(= VocabId(10)), ?y>}; (?y = VocabId(10))
// Returns {<(= 5), ?x>, <(= VocabId(10)), ?y>}.
//
// EXAMPLE 3
// left child: {<(>= 10 AND <= 20), ?x>}; (?x >= 10 && ?x <= 20)
// right child: {<(!= 15), ?x>, <(= 10), ?y>}; (?x != 15 && ?y = 10)
// Returns {<((>= 10 AND <= 20) AND != 15), ?x>, <(= 10), ?y>}
//
//
// MERGE <BinaryOperator::AND, OrExpression>
// (Partial De-Morgan applied w.r.t. Or-conjunction)
//______________________________________________________________________________
// Merge the pair values of leftChild and rightChild with the combine logic for
// AND. Given we have a respective pair <PrefilterExpression,
// Variable> from leftChild and rightChild for which the Variable value is
// equal, conjunct the two PrefilterExpressions over an OrExpression. For the
// resulting pairs, NOT (NotExpression) is applied later on. (As a result we
// have fully applied De-Morgan on the available PrefilterExpression)
//
// EXAMPLE 1
// !...(?x = 5 OR ?y = VocabId(10))
// partial De-Morgan: ...(?x = 5 AND ?y = VocabId(10))
// (complete De-Morgan later on with NotExpression:
// ...(?x != 5 AND ?y != VocabId(10))) (in NumericUnaryExpression.cpp)
// The merge function implements the following:
// left child: {<(= 5), ?x>}
// right child: {<(= VocabId(10)), ?y>}
// merged: {<(= 5), ?x>, <(= VocabId(10)), ?y>}
//
// EXAMPLE 2
// left child: {<(>= 10 OR <= 20), ?x>}
// right child: {<(!= 15), ?x>, <(= 10), ?y>}
// merged: {<((>= 10 OR <= 20) OR ?x != 15), ?x>, <(= 10), ?y>}
//
// MERGE <BinaryOperator::OR, OrExpression>
//______________________________________________________________________________
// Four examples on how this function (OR) merges the content of two children.
//
// EXAMPLE 1
// left child: {} (std::nullopt)
// right child: {<(<= 10), ?y>}
// Given that it is not possible to make a fixed value assumption (true or
// false) for the left (empty) child, it is also unclear if the right child
// must evaluate to true for this OR expression to become true.
// Hence, no PrefilterExpression (std::nullopt) will be returned.
//
// EXAMPLE 2
// left child: {<(= 5), ?y>}
// right child: {<(<= 3), ?x>}
// We want to pre-filter the blocks for the expression: ?y >= 5 || ?x <= 3
// No PrefilterExpression will be returned.
//
// EXAMPLE 3
// left child: {<(>= 5), ?x>}
// right child: {<(= 0), ?x>}
// We want to pre-filter the blocks to the expression: ?x >= 5 || ?x = 0
// The resulting PrefilterExpression is {<(>= 5 OR = 0), ?x>}
//
// EXAMPLE 4
// left child: {<(= 10), ?x), <(!= 0), ?y>}
// right child: {<(<= 0), ?x>}
// We have to construct a PrefilterExpression for (?x >= 10 && ?y != 0) ||
// ?x <= 0. If this OR expression yields true, at least ?x >= 10 || ?x <= 0
// must be staisifed; for this objective we can construct a
// PrefiterExpression. We can't make a distinct prediction w.r.t. ?y != 0
// => not relevant for the PrefilterExpression. Thus, we return the
// PrefilterExpresion {<(>= 10 OR <= 0), ?x>}.
//
//
// MERGE <BinaryOperator::OR, AndExpression>
// (Partial De-Morgan applied w.r.t. And-conjunction)
//______________________________________________________________________________
// Merge the pair values of leftChild and rightChild with the combine logic for
// OR. The difference, given we have a respective pair <PrefilterExpression,
// Variable> from leftChild and rightChild for which the Variable value is
// equal, conjunct the two PrefilterExpressions over an AndExpression. For the
// resulting pairs, NOT (NotExpression) is applied later on. As a result we
// have fully applied De-Morgan on the available PrefilterExpression.
//
// EXAMPLE 1
// !...(?y = 5 AND ?x <= 3); apply partial De-Morgan given NOT(!): Thus OR
// instead of AND merge (here).
// left child: {<(= 5), ?y>}
// right child: {<(<= 3), ?x>}
// merged result: {}
//
// EXAMPLE 2
// !...((?x = 10 OR ?y != 0) AND ?x <= 0); apply partial De-Morgan given NOT(!):
// Thus OR instead of AND merge (here).
// left child: {<(= 10), ?x), <(!= 0), ?y>};
// right child: {<(<= 0), ?x>}
// merged result: {<((= 10) AND (<= 0)), ?x>}
// (with NOT applied later on: {<((!= 10) OR (> 0)), ?x>})
//______________________________________________________________________________
template <BinaryOperator binOp, typename BinaryPrefilterExpr>
std::vector<PrefilterExprVariablePair> mergeChildrenForBinaryOpExpressionImpl(
    std::vector<PrefilterExprVariablePair>&& leftChild,
    std::vector<PrefilterExprVariablePair>&& rightChild) {
  using enum BinaryOperator;
  namespace pd = prefilterExpressions::detail;
  pd::checkPropertiesForPrefilterConstruction(leftChild);
  pd::checkPropertiesForPrefilterConstruction(rightChild);
  // Merge the PrefilterExpression vectors from the left and right child.
  // Remark: The vectors contain std::pairs, sorted by their respective
  // Variable to the PrefilterExpression.
  auto itLeft = leftChild.begin();
  auto itRight = rightChild.begin();
  std::vector<PrefilterExprVariablePair> resPairs;
  while (itLeft != leftChild.end() && itRight != rightChild.end()) {
    auto& [exprLeft, varLeft] = *itLeft;
    auto& [exprRight, varRight] = *itRight;
    if (varLeft == varRight) {
      resPairs.emplace_back(std::make_unique<BinaryPrefilterExpr>(
                                std::move(exprLeft), std::move(exprRight)),
                            varLeft);
      ++itLeft;
      ++itRight;
    } else if (varLeft < varRight) {
      if constexpr (binOp == AND) {
        resPairs.emplace_back(std::move(*itLeft));
      }
      ++itLeft;
    } else {
      if constexpr (binOp == AND) {
        resPairs.emplace_back(std::move(*itRight));
      }
      ++itRight;
    }
  }
  if constexpr (binOp == AND) {
    ql::ranges::move(itLeft, leftChild.end(), std::back_inserter(resPairs));
    ql::ranges::move(itRight, rightChild.end(), std::back_inserter(resPairs));
  }
  pd::checkPropertiesForPrefilterConstruction(resPairs);
  return resPairs;
}

// TEMPLATED STANDARD MERGE
//______________________________________________________________________________
constexpr auto makeAndMergeWithAndConjunction =
    mergeChildrenForBinaryOpExpressionImpl<BinaryOperator::AND,
                                           prefilterExpressions::AndExpression>;
constexpr auto makeOrMergeWithOrConjunction =
    mergeChildrenForBinaryOpExpressionImpl<BinaryOperator::OR,
                                           prefilterExpressions::OrExpression>;
// TEMPLATED (PARTIAL) DE-MORGAN MERGE
//______________________________________________________________________________
constexpr auto makeAndMergeWithOrConjunction =
    mergeChildrenForBinaryOpExpressionImpl<BinaryOperator::AND,
                                           prefilterExpressions::OrExpression>;
constexpr auto makeOrMergeWithAndConjunction =
    mergeChildrenForBinaryOpExpressionImpl<BinaryOperator::OR,
                                           prefilterExpressions::AndExpression>;

// GET TEMPLATED MERGE FUNCTION (CONJUNCTION AND / OR)
//______________________________________________________________________________
// getMergeFunction (get the respective merging function) follows the
// principles of De Morgan's law. If this is a child of a NotExpression
// (NOT(!)), we have to swap the combination procedure (swap AND(&&) and
// OR(||) respectively). This equals a partial application of De Morgan's
// law. On the resulting PrefilterExpressions, NOT is applied in
// UnaryNegateExpression(Impl). For more details take a look at the
// implementation in NumericUnaryExpressions.cpp (see
// UnaryNegateExpressionImpl).
//
// EXAMPLE
// - How De-Morgan's law is applied in steps
// !((?y != 10 || ?x = 0) || (?x = 5 || ?x >= 10)) is the complete (Sparql
// expression). With De-Morgan's law applied:
// (?y = 10 && ?x != 0) && (?x != 5 && ?x < 10)
//
// {<expr1, var1>, ....<exprN, varN>} represents the PrefilterExpressions
// with their corresponding Variable.
//
// At the current step we have: isNegated = true; because of !(...) (NOT)
// left child: {<(!= 10), ?y>, <(!= 0), ?x>}; (?y != 10 || ?x != 0)
//
// Remark: The (child) expressions of the left and right child have been
// combined with the merging procedure AND (makeAndMergeWithOrConjunction),
// because with isNegated = true, it is implied that De-Morgan's law is applied.
// Thus, the OR is logically an AND conjunction with its application.
//
// right child: {<((= 5) OR (>= 10)), ?x>} (?x = 5 || ?x >= 10)
//
// isNegated is true, hence we know that we have to partially apply
// De-Morgan's law. Given that, OR switches to an AND. Therefore we use
// mergeChildrenForBinaryOpExpressionImpl with OrExpression
// (PrefilterExpression) as a template value (BinaryPrefilterExpr). Call
// makeAndMergeWithOrConjunction with left and right child as arguments, we get
// their merged combination:
// {<(!= 10), ?y>, <((!= 0) OR ((= 5) OR (>= 10))), ?x>}
//
// Next their merged value is returned to UnaryNegateExpressionImpl
// (defined in NumericUnaryExpressions.cpp), where for each expression of
// the <PrefilterExpression, Variable> pairs, NOT (NotExpression) is
// applied.
// {<(!= 10), ?y>, <((!= 0) OR ((= 5) OR (>= 10))), ?x>}
// apply NotExpr.: {<!(!= 10), ?y>, <!((!= 0) OR ((= 5) OR (>= 10))), ?x>}
// This procedure yields: {<(= 10), ?y>, <((= 0) AND ((!= 5) AND (< 10))), ?x>}
//______________________________________________________________________________
template <typename BinaryPrefilterExpr>
constexpr auto getMergeFunction(bool isNegated) {
  if constexpr (std::is_same_v<BinaryPrefilterExpr,
                               prefilterExpressions::AndExpression>) {
    return !isNegated ? makeAndMergeWithAndConjunction
                      // negated, partially apply De-Morgan:
                      // change AND to OR
                      : makeOrMergeWithAndConjunction;
  } else {
    static_assert(std::is_same_v<BinaryPrefilterExpr,
                                 prefilterExpressions::OrExpression>);
    return !isNegated ? makeOrMergeWithOrConjunction
                      // negated, partially apply De-Morgan:
                      // change OR to AND
                      : makeAndMergeWithOrConjunction;
  }
}

}  // namespace

//______________________________________________________________________________
CPP_template(typename BinaryPrefilterExpr, typename NaryOperation)(
    requires isOperation<NaryOperation>) class LogicalBinaryExpressionImpl
    : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      bool isNegated) const override {
    const auto& children = this->children();
    AD_CORRECTNESS_CHECK(children.size() == 2);
    auto leftChild =
        children[0].get()->getPrefilterExpressionForMetadata(isNegated);
    auto rightChild =
        children[1].get()->getPrefilterExpressionForMetadata(isNegated);
    return constructPrefilterExpr::getMergeFunction<BinaryPrefilterExpr>(
        isNegated)(std::move(leftChild), std::move(rightChild));
  }
};

}  //  namespace constructPrefilterExpr

//______________________________________________________________________________
using AndExpression = constructPrefilterExpr::LogicalBinaryExpressionImpl<
    prefilterExpressions::AndExpression,
    Operation<2, FV<decltype(andLambda), EffectiveBooleanValueGetter>,
              SET<SetOfIntervals::Intersection>>>;

using OrExpression = constructPrefilterExpr::LogicalBinaryExpressionImpl<
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
  if (getRuntimeParameter<&RuntimeParameters::divisionByZeroIsUndef_>()) {
    return std::make_unique<DivideExpressionByZeroIsUndef>(std::move(child1),
                                                           std::move(child2));
  } else {
    return std::make_unique<DivideExpressionByZeroIsNan>(std::move(child1),
                                                         std::move(child2));
  }
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
