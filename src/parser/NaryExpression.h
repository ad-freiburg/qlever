//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 29.09.21.
//

// Unary and Binary Expressions

#ifndef QLEVER_NARYEXPRESSION_H
#define QLEVER_NARYEXPRESSION_H

#include "./SparqlExpression.h"

namespace sparqlExpression {
namespace detail {
/**
 * A Unary Expression.
 * @tparam RangeCalculation  see documentation of `BinaryOperation`
 * @tparam ValueExtractor    see documentation of `BinaryOperation`
 * @tparam UnaryOperation  A unary operation that takes one input (the result of
 * the ValueGetter) and calculates the result.
 */
template <typename RangeCalculation, typename ValueExtractor,
          typename UnaryOperation, TagString Tag>
class UnaryExpression : public SparqlExpression {
 public:
  UnaryExpression(SparqlExpression::Ptr&& child) : _child{std::move(child)} {};
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override { return {&_child, 1}; }

  // _________________________________________________________________________
  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return std::string{Tag} + "("s + _child->getCacheKey(varColMap) + ")";
  }

 private:
  Ptr _child;
};

// TODO<joka921> Unify comments.
/**
 * @brief An associative binary expression, for example (?a or ?b), (?a and ?b
?and ?c).
 * Note that expressions involving the four basic arithmetic operations (+, - ,
* , /) are
 * implemented using the DispatchedBinaryExpression template.
 * @tparam CalculationWithSetOfIntervals A callable type that performs the
operation
 * efficiently, when both inputs are of type SetOfIntervals. If no such
 * operation exists, the type NoCalculationWithSetOfIntervals must be chosen.
 * @tparam ValueGetter A callable type that takes a single
 * double/int64_t/bool and extracts the actual input to the operation. Can be
 * used to perform conversions.
 * @tparam BinaryOperation The actual binary operation, it must be callable with
 * the result types of the `ValueGetter` .
 */
/**
 * This class template allows to perform different left-associative binary
 * operations in a single class, e.g. "3 * 5 / 7 * ?x"
 *
 * @tparam ValueExtractor see documention of `BinaryExpression`
 * @tparam TagsAndFunctions One or more BinaryOperations combined with Tags to
 * identify them (e.g. `TaggedFunction<"*", LambdaThatMultipliesTwoNumbers>`)
 */
template <typename RangeCalculation, typename ValueExtractor,
          TaggedFunctionConcept... TagsAndFunctions>
requires(std::is_same_v<RangeCalculation, NoCalculationWithSetOfIntervals> ||
         sizeof...(TagsAndFunctions) == 1) class DispatchedBinaryExpression
    : public SparqlExpression {
 public:
  /// If Children is {`<exprA>, <exprB>, <exprC>'} and relations is {"*", "/"},
  /// then this expression stands for `<exprA> * <exprB> / <exprC>`. Checks if
  /// the sizes match (number of children is number of relations + 1) and if the
  /// tags actually represent one of the TaggedFunctions.
  /// If there is only one child, then this BinaryExpression does not add any
  /// semantics, and a pointer to the single child is returned directly.
  static SparqlExpression::Ptr create(
      std::vector<SparqlExpression::Ptr>&& children,
      std::vector<TagString>&& relations) {
    if (children.size() == 1) {
      AD_CHECK(relations.empty());
      return std::move(children[0]);
    }
    // Unfortunately, std::make_unique cannot call
    return SparqlExpression::Ptr{new DispatchedBinaryExpression{
        std::move(children), std::move(relations)}};
  }

  static SparqlExpression::Ptr
  create(std::vector<SparqlExpression::Ptr>&& children) requires(
      sizeof...(TagsAndFunctions) == 1) {
    auto onlyTag =
        std::tuple_element_t<0, std::tuple<TagsAndFunctions...>>::tag;
    AD_CHECK(children.size() > 0);
    std::vector<TagString> tags(children.size() - 1, onlyTag);
    return create(std::move(children), std::move(tags));
  }

 private:
  /// The actual constructor. It is private, only construct using the
  /// static `create` function.
  DispatchedBinaryExpression(std::vector<SparqlExpression::Ptr>&& children,
                             std::vector<TagString>&& relations)
      : _children{std::move(children)}, _relations{std::move(relations)} {
    AD_CHECK(_relations.size() + 1 == _children.size());
    auto tags = allowedTags();
    for (const auto& rel : _relations) {
      AD_CHECK(tags.contains(rel));
    }
    static_assert(sizeof...(TagsAndFunctions) > 0);
  }

  // Obtain the set of Tags that represent a valid operation.
  ad_utility::HashSet<TagString> allowedTags() const {
    return {TagsAndFunctions{}.tag...};
  }

 public:
  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override {
    return {_children.begin(), _children.end()};
  }

  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    std::vector<string> childKeys;
    for (const auto& ptr : _children) {
      childKeys.push_back(ptr->getCacheKey(varColMap));
    }

    string key = "(" + std::move(childKeys[0]) + ")";
    for (size_t i = 1; i < childKeys.size(); ++i) {
      key += " " + _relations[i - 1] + " (" + std::move(childKeys[i]) + ")";
    }
    return key;
  }

 private:
  std::vector<Ptr> _children;
  std::vector<TagString> _relations;
};

}  // namespace detail

/// The Actual aliases for the expressions.

/// Boolean Or
inline auto orLambda = [](bool a, bool b) { return a || b; };
using ConditionalOrExpression = detail::DispatchedBinaryExpression<
    ad_utility::Union, detail::EffectiveBooleanValueGetter,
    TaggedFunction<"||", decltype(orLambda)>>;

/// Boolean And
inline auto andLambda = [](bool a, bool b) { return a && b; };
using ConditionalAndExpression = detail::DispatchedBinaryExpression<
    ad_utility::Intersection, detail::EffectiveBooleanValueGetter,
    TaggedFunction<"&&", decltype(andLambda)>>;

/// Unary Negation
inline auto unaryNegate = [](bool a) -> bool { return !a; };
using UnaryNegateExpression =
    detail::UnaryExpression<detail::NoCalculationWithSetOfIntervals,
                            detail::EffectiveBooleanValueGetter,
                            decltype(unaryNegate), "!">;

/// Unary Minus, currently all results are converted to double
inline auto unaryMinus = [](auto a) -> double { return -a; };
using UnaryMinusExpression =
    detail::UnaryExpression<detail::NoCalculationWithSetOfIntervals,
                            detail::NumericValueGetter, decltype(unaryMinus),
                            "unary-">;

/// Multiplication and Division, currently all results are converted to double.
inline auto multiply = [](const auto& a, const auto& b) -> double {
  return a * b;
};
inline auto divide = [](const auto& a, const auto& b) -> double {
  return static_cast<double>(a) / b;
};
using MultiplicativeExpression =
    detail::DispatchedBinaryExpression<detail::NoCalculationWithSetOfIntervals,
                                       detail::NumericValueGetter,
                                       TaggedFunction<"*", decltype(multiply)>,
                                       TaggedFunction<"/", decltype(divide)>>;

/// Addition and subtraction, currently all results are converted to double.
inline auto add = [](const auto& a, const auto& b) -> double { return a + b; };
inline auto subtract = [](const auto& a, const auto& b) -> double {
  return a - b;
};
using AdditiveExpression =
    detail::DispatchedBinaryExpression<detail::NoCalculationWithSetOfIntervals,
                                       detail::NumericValueGetter,
                                       TaggedFunction<"+", decltype(add)>,
                                       TaggedFunction<"-", decltype(subtract)>>;

}  // namespace sparqlExpression

#endif  // QLEVER_NARYEXPRESSION_H
