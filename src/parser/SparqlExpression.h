//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <memory>
#include <span>
#include <variant>
#include <vector>

#include "../engine/CallFixedSize.h"
#include "../engine/QueryExecutionContext.h"
#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "../util/ConstexprSmallString.h"
#include "./SetOfIntervals.h"
#include "./SparqlExpressionTypes.h"
#include "./SparqlExpressionValueGetters.h"

namespace sparqlExpression {

/// Virtual base class for an arbitrary Sparql Expression which holds the
/// structure of the expression as well as the logic to evaluate this expression
/// on a given intermediate result
class SparqlExpression {
 private:
  std::string _descriptor;

 public:
  /// ________________________________________________________________________
  using Ptr = std::unique_ptr<SparqlExpression>;

  /// Evaluate a Sparql expression.
  virtual ExpressionResult evaluate(EvaluationContext*) const = 0;

  /// Return all variables and IRIs, needed for certain parser methods.
  virtual vector<string*> strings() final {
    vector<string*> result;
    // Recursively aggregate the strings from all children.
    for (auto& child : children()) {
      auto childStrings = child->strings();
      result.insert(result.end(), childStrings.begin(), childStrings.end());
    }

    // Add the strings from this expression.
    auto locallyAdded = getStringLiteralsAndVariablesNonRecursive();
    for (auto& el : locallyAdded) {
      result.push_back(&el);
    }
    return result;
  }

  /// Return all the variables that occur in the expression, but are not
  /// aggregated.
  virtual vector<std::string> getUnaggregatedVariables() {
    // Default implementation: This expression adds no variables, but all
    // unaggregated variables from the children remain unaggregated.
    std::vector<string> result;
    for (const auto& child : children()) {
      auto childResult = child->getUnaggregatedVariables();
      result.insert(result.end(), std::make_move_iterator(childResult.begin()),
                    std::make_move_iterator(childResult.end()));
    }
    return result;
  }

  /// Get a unique identifier for this expression, used as cache key.
  virtual string getCacheKey(const VariableToColumnMap& varColMap) const = 0;

  /// Get a short, human readable identifier for this expression.
  virtual const string& descriptor() const final { return _descriptor; }
  virtual string& descriptor() final { return _descriptor; }

  /// For the pattern trick we need to know, whether this expression
  /// is a non-distinct count of a single variable. In this case we return
  /// the variable. Otherwise we return std::nullopt.
  virtual std::optional<string> getVariableForNonDistinctCountOrNullopt()
      const {
    return std::nullopt;
  }

  /// Helper function for getVariableForNonDistinctCountOrNullopt() : If this
  /// expression is a single variable, return the name of this variable.
  /// Otherwise, return std::nullopt.
  virtual std::optional<string> getVariableOrNullopt() const {
    return std::nullopt;
  }

  virtual ~SparqlExpression() = default;

 private:
  virtual std::span<SparqlExpression::Ptr> children() = 0;
  virtual std::span<string> getStringLiteralsAndVariablesNonRecursive() {
    // Default implementation: This expression adds no strings or variables.
    return {};
  }
};

// ____________________________________________________________________________
namespace detail {
/// An expression with a single value, for example a numeric (42.0) or boolean
/// (false) constant or a variable (?x) or a string or iri (<Human>). These are
/// the "leaves" in the expression tree.
template <typename T>
class LiteralExpression : public SparqlExpression {
 public:
  // _________________________________________________________________________
  LiteralExpression(T _value) : _value{std::move(_value)} {}

  // Evaluating just returns the constant/literal value.
  ExpressionResult evaluate(
      [[maybe_unused]] EvaluationContext* context) const override {
    if constexpr (std::is_same_v<string, T>) {
      Id id;
      bool idWasFound = context->_qec.getIndex().getVocab().getId(_value, &id);
      if (!idWasFound) {
        // no vocabulary entry found, just use it as a string constant.
        // TODO<joka921>:: emit a warning.
        return _value;
      }
      return StrongIdWithResultType{id, ResultTable::ResultType::KB};
    } else {
      return _value;
    }
  }

  // Literal expressions don't have children
  std::span<SparqlExpression::Ptr> children() override { return {}; }

  // Variables and string constants add their values.
  virtual std::span<string> getStringLiteralsAndVariablesNonRecursive() {
    if constexpr (std::is_same_v<T, Variable>) {
      return {&_value._variable, 1};
    } else if constexpr (std::is_same_v<T, string>) {
      return {&_value, 1};
    } else {
      return {};
    }
  }

  // _________________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    if constexpr (std::is_same_v<T, Variable>) {
      return {_value._variable};
    } else {
      return {};
    }
  }

  // ______________________________________________________________________
  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    if constexpr (std::is_same_v<T, Variable>) {
      return {"#column_" + std::to_string(varColMap.at(_value._variable)) +
              "#"};
    } else if constexpr (std::is_same_v<T, string>) {
      return _value;
    } else {
      return {std::to_string(_value)};
    }
  }

 protected:
  // _________________________________________________________________________
  std::optional<std::string> getVariableOrNullopt() const override {
    if constexpr (std::is_same_v<T, Variable>) {
      return _value._variable;
    }
    return std::nullopt;
  }

 private:
  T _value;
};

/**
 * A Unary operation.
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

/// A Special type to be used as the CalculationWithSetOfIntervals template
/// Argument, when a range calculation is not allowed
struct NoRangeCalculation {};

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
 * operation exists, the type NoRangeCalculation must be chosen.
 * @tparam ValueGetter A callable type that takes a single
 * double/int64_t/bool and extracts the actual input to the operation. Can be
 * used to perform conversions.
 * @tparam BinaryOperation The actual binary operation, it must be callable with
 * the result types of the `ValueGetter` .
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
requires(std::is_same_v<RangeCalculation, NoRangeCalculation> ||
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

template <typename RangeCalculation, typename ValueGetter, typename AggregateOp,
          typename FinalOp, TagString Tag>
class AggregateExpression : public SparqlExpression {
 public:
  AggregateExpression(bool distinct, Ptr&& child,
                      AggregateOp aggregateOp = AggregateOp{})
      : _distinct(distinct),
        _child{std::move(child)},
        _aggregateOp{std::move(aggregateOp)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override { return {&_child, 1}; }

  // _________________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  // __________________________________________________________________________
  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return std::string{Tag} + "(" + _child->getCacheKey(varColMap) + ")";
  }

  // __________________________________________________________________________
  std::optional<string> getVariableForNonDistinctCountOrNullopt()
      const override {
    if (Tag == "COUNT" && !_distinct) {
      return _child->getVariableOrNullopt();
    }
    return std::nullopt;
  }

 private:
  bool _distinct;
  Ptr _child;
  AggregateOp _aggregateOp;
};

// This can be used as the FinalOp parameter to an Aggregate if there is nothing
// to be done on the final result
inline auto noop = []<typename T>(T&& result, size_t) {
  return std::forward<T>(result);
};

// ______________________________________________________________________________
inline auto makePerformConcat(std::string separator) {
  return [sep = std::move(separator)](string&& a, const string& b) -> string {
    if (a.empty()) [[unlikely]] {
      return b;
    } else [[likely]] {
      a.append(sep);
      a.append(std::move(b));
      return std::move(a);
    }
  };
}

using PerformConcat = std::decay_t<decltype(makePerformConcat(std::string{}))>;

/// The GROUP_CONCAT Expression
class GroupConcatExpression : public SparqlExpression {
 public:
  GroupConcatExpression(bool distinct, Ptr&& child, std::string separator)
      : _separator{std::move(separator)} {
    auto performConcat = makePerformConcat(_separator);

    using G =
        AggregateExpression<NoRangeCalculation, StringValueGetter,
                            PerformConcat, decltype(noop), "GROUP_CONCAT">;
    _child = std::make_unique<G>(distinct, std::move(child), performConcat);
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    // The child is already set up to perform all the work.
    return _child->evaluate(context);
  }

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override { return {&_child, 1}; }

  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return "["s + _separator + "]" + _child->getCacheKey(varColMap);
  }

 private:
  Ptr _child;
  std::string _separator;
};

}  // namespace detail

/// The Actual aliases for the expressions

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
    detail::UnaryExpression<detail::NoRangeCalculation,
                            detail::EffectiveBooleanValueGetter,
                            decltype(unaryNegate), "!">;

/// Unary Minus, currently all results are converted to double
inline auto unaryMinus = [](auto a) -> double { return -a; };
using UnaryMinusExpression =
    detail::UnaryExpression<detail::NoRangeCalculation,
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
    detail::DispatchedBinaryExpression<detail::NoRangeCalculation,
                                       detail::NumericValueGetter,
                                       TaggedFunction<"*", decltype(multiply)>,
                                       TaggedFunction<"/", decltype(divide)>>;

/// Addition and subtraction, currently all results are converted to double.
inline auto add = [](const auto& a, const auto& b) -> double { return a + b; };
inline auto subtract = [](const auto& a, const auto& b) -> double {
  return a - b;
};
using AdditiveExpression =
    detail::DispatchedBinaryExpression<detail::NoRangeCalculation,
                                       detail::NumericValueGetter,
                                       TaggedFunction<"+", decltype(add)>,
                                       TaggedFunction<"-", decltype(subtract)>>;

inline auto count = [](const auto& a, const auto& b) -> int64_t {
  return a + b;
};
using CountExpression =
    detail::AggregateExpression<detail::NoRangeCalculation,
                                detail::IsValidValueGetter, decltype(count),
                                decltype(detail::noop), "COUNT">;
using SumExpression =
    detail::AggregateExpression<detail::NoRangeCalculation,
                                detail::NumericValueGetter, decltype(add),
                                decltype(detail::noop), "SUM">;

inline auto averageFinalOp = [](const auto& aggregation, size_t numElements) {
  return numElements ? static_cast<double>(aggregation) /
                           static_cast<double>(numElements)
                     : std::numeric_limits<double>::quiet_NaN();
};
using AvgExpression =
    detail::AggregateExpression<detail::NoRangeCalculation,
                                detail::NumericValueGetter, decltype(add),
                                decltype(averageFinalOp), "AVG">;

inline auto minLambda = [](const auto& a, const auto& b) -> double {
  return a < b ? a : b;
};
inline auto maxLambda = [](const auto& a, const auto& b) -> double {
  return a > b ? a : b;
};

using MinExpression =
    detail::AggregateExpression<detail::NoRangeCalculation,
                                detail::NumericValueGetter, decltype(minLambda),
                                decltype(detail::noop), "MIN">;
using MaxExpression =
    detail::AggregateExpression<detail::NoRangeCalculation,
                                detail::NumericValueGetter, decltype(maxLambda),
                                decltype(detail::noop), "MAX">;

/// The base cases: Constants and variables
using BooleanLiteralExpression = detail::LiteralExpression<bool>;
using IntLiteralExpression = detail::LiteralExpression<int64_t>;
using DoubleLiteralExpression = detail::LiteralExpression<double>;
using VariableExpression = detail::LiteralExpression<Variable>;
using StringOrIriExpression = detail::LiteralExpression<string>;

}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSION_H
