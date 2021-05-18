//
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <memory>
#include <ranges>
#include <variant>
#include <vector>

#include "../engine/QueryExecutionContext.h"
#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "../util/ConstexprSmallString.h"
#include "./BooleanRangeExpressions.h"

namespace sparqlExpression {
/// Virtual base class for an arbitrary Sparql Expression which holds the
/// structure of the expression as well as the logic to evaluate this expression
/// on a given intermediate result
class SparqlExpression {
 public:
  /// A Sparql Variable, e.g. "?x"
  struct Variable {
    std::string _variable;
  };

  /// Typedef for a map from variables names to (input column, input column
  /// type) which is needed to evaluate expressions that contain variables.
  using VariableColumnMap =
      ad_utility::HashMap<std::string,
                          std::pair<size_t, ResultTable::ResultType>>;

  /// All the additional information which is needed to evaluate a Sparql
  /// Expression
  struct EvaluationInput {
    /// Constructor for evaluating an expression on the complete input
    EvaluationInput(const QueryExecutionContext& qec, VariableColumnMap map,
                    const IdTable& inputTable) noexcept
        : _qec{qec},
          _variableColumnMap{std::move(map)},
          _inputTable{inputTable} {}
    /// Constructor for evaluating an expression on a part of the input
    EvaluationInput(const QueryExecutionContext& qec, VariableColumnMap map,
                    const IdTable& inputTable, size_t beginIndex,
                    size_t endIndex) noexcept
        : _qec{qec},
          _variableColumnMap{std::move(map)},
          _inputTable{inputTable},
          _beginIndex{beginIndex},
          _endIndex{endIndex} {}
    /// Needed to map Ids to their value from the vocabulary

    const QueryExecutionContext& _qec;
    VariableColumnMap _variableColumnMap;

    /// The input of the expression
    const IdTable& _inputTable;
    /// The indices of the actual range in the _inputTable on which the
    /// expression is evaluated. For BIND expressions this is always [0,
    /// _inputTable.size()) but for GROUP BY evaluation we also need only parts
    /// of the input.
    size_t _beginIndex = 0;
    size_t _endIndex = _inputTable.size();
  };

  /// The result of an epxression variable can either be a constant of type
  /// bool/double/int (same value for all result rows), a vector of one of those
  /// types (one value for each result row), a variable (e.g. in BIND (?x as
  /// ?y)) or a "Set" of indices, which identifies the indices in the result at
  /// which the result columns value is "true".
  using EvaluateResult =
      std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>,
                   setOfIntervals::Set, double, int64_t, bool, Variable>;

  /// ________________________________________________________________________
  using Ptr = std::unique_ptr<SparqlExpression>;

  /// Evaluate a Sparql expression.
  virtual EvaluateResult evaluate(EvaluationInput*) const = 0;

  /// Returns all variables and IRIs, needed for certain parser methods.
  virtual vector<std::string*> strings() = 0;
  virtual ~SparqlExpression() = default;
};

/// We will use the string representation of various functions (e.g. '+' '*')
/// directly as template parameters. Currently 7 characters are enough for this,
/// but if we need longer names in the future, we can still change this at the
/// cost of a recompilation.
using TagString = ad_utility::ConstexprSmallString<8>;

// ____________________________________________________________________________
namespace detail {

using EvaluationInput = SparqlExpression::EvaluationInput;
using Variable = SparqlExpression::Variable;

/// Annotate an arbitrary Type (we will use this for Callables) with a
/// TagString. The TagString is part of the type.
template <TagString Tag, typename Function>
struct TaggedFunction {
  static constexpr TagString tag = Tag;
  using functionType = Function;
};

template <typename T>
constexpr bool IsTaggedFunction = false;

template <TagString Tag, typename Function>
constexpr bool IsTaggedFunction<TaggedFunction<Tag, Function>> = true;

template <typename T>
concept TaggedFunctionConcept = IsTaggedFunction<T>;

/// An expression with a single value, e.g. a numeric or boolean constant, or a
/// Variable
template <typename T>
class LiteralExpression : public SparqlExpression {
 public:
  // _________________________________________________________________________
  LiteralExpression(T _value) : _value{std::move(_value)} {}

  // Evaluating just returns the constant/literal value
  EvaluateResult evaluate(EvaluationInput*) const override {
    return EvaluateResult{_value};
  }

  // _____________________________________________________________________
  vector<std::string*> strings() override {
    if constexpr (std::is_same_v<T, Variable>) {
      return {&_value._variable};
    } else {
      return {};
    }
  }

 private:
  T _value;
};

// TODO<joka921>: Make the RangeCalculationParameter the last one and default
// it.
/**
 * @brief An associative binary expression
 * @tparam RangeCalculation A callable type that performs the operation
 * efficiently, when both inputs are of type setOfIntervals::Set. If no such
 * operation exists, the type NoRangeCalculation must be chosen.
 * @tparam ValueExtractor A callable type that takes a single
 * double/int64_t/bool and extracts the actual input to the operation. Can be
 * used to perform conversions.
 * @tparam BinaryOperation The actual binary operation, it must be callable with
 * the result types of the value extractor
 */
template <typename RangeCalculation, typename ValueExtractor,
          typename BinaryOperation>
class BinaryExpression : public SparqlExpression {
 public:
  /// Construct from a set of child operations. The operation is performed as a
  /// left fold on all the children using the BinaryOperation
  BinaryExpression(std::vector<Ptr>&& children)
      : _children{std::move(children)} {};
  // _________________________________________________________________________
  EvaluateResult evaluate(EvaluationInput* input) const override;

  // _____________________________________________________________________
  vector<std::string*> strings() override {
    std::vector<string*> result;
    for (const auto& ptr : _children) {
      auto childResult = ptr->strings();
      result.insert(result.end(), childResult.begin(), childResult.end());
    }
    return result;
  }

 private:
  std::vector<Ptr> _children;
};

/**
 * A Unary operation.
 * @tparam RangeCalculation  see documentation of `BinaryOperation`
 * @tparam ValueExtractor    see documentation of `BinaryOperation`
 * @tparam UnaryOperation  A unary operation that takes one input (the result of
 * the ValueExtractor) and calculates the result.
 */
template <typename RangeCalculation, typename ValueExtractor,
          typename UnaryOperation>
class UnaryExpression : public SparqlExpression {
 public:
  UnaryExpression(Ptr&& child) : _child{std::move(child)} {};
  EvaluateResult evaluate(EvaluationInput* input) const override;
  // _____________________________________________________________________
  vector<std::string*> strings() override { return _child->strings(); }

 private:
  Ptr _child;
};

/**
 * This class template allows to perform different left-associative binary
 * operations in a single class, e.g. "3 * 5 / 7 * ?x"
 *
 * @tparam ValueExtractor see documention of `BinaryExpression`
 * @tparam TagsAndFunctions One or more BinaryOperations combined with Tags to
 * identify them (e.g. `TaggedFunction<"*", LambdaThatMultipliesTwoNumbers>`)
 */
template <typename ValueExtractor, TaggedFunctionConcept... TagsAndFunctions>
class DispatchedBinaryExpression : public SparqlExpression {
 public:
  /// If Children is {`<exprA>, <exprB>, <exprC>'} and relations is {"*", "/"},
  /// then this expression stands for `<exprA> * <exprB> / <exprC>`. Checks if
  /// the sizes match (number of children is number of relations + 1) and if the
  /// tags actually represent one of the TaggedFunctions.
  DispatchedBinaryExpression(std::vector<Ptr>&& children,
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

  // __________________________________________________________________________
  EvaluateResult evaluate(EvaluationInput* input) const override;

  // _____________________________________________________________________
  vector<std::string*> strings() override {
    std::vector<string*> result;
    for (const auto& ptr : _children) {
      auto childResult = ptr->strings();
      result.insert(result.end(), childResult.begin(), childResult.end());
    }
    return result;
  }

 private:
  std::vector<Ptr> _children;
  std::vector<TagString> _relations;
};

/// A strong type for "Ids
struct StrongId {
  Id _value;
};

/// This class can be used as the `ValueExtractor` argument of Expression
/// templates. It preserves the numeric datatype.
struct NumericValueGetter {
  // Simply preserve the input from numeric values
  double operator()(double v, EvaluationInput*) const { return v; }
  int64_t operator()(int64_t v, EvaluationInput*) const { return v; }
  bool operator()(bool v, EvaluationInput*) const { return v; }
  // Convert an id from a result table to a double value
  double operator()(StrongId id, ResultTable::ResultType type,
                    EvaluationInput*) const;
};

/// This class can be used as the `ValueExtractor` argument of Expression
/// templates. It produces a boolean value.
struct BooleanValueGetter {
  bool operator()(double v, EvaluationInput*) const { return v; }
  bool operator()(int64_t v, EvaluationInput*) const { return v; }
  bool operator()(bool v, EvaluationInput*) const { return v; }
  // Convert an id from a result table to a boolean value
  bool operator()(StrongId id, ResultTable::ResultType type,
                  EvaluationInput*) const;
};

/// A Special type to be used as the RangeCalculation template Argument, when a
/// range calculation is not allowed
struct NoRangeCalculation {};

}  // namespace detail

/// The Actual aliases for the expressions

/// Boolean Or
inline auto orLambda = [](bool a, bool b) { return a || b; };
using ConditionalOrExpression =
    detail::BinaryExpression<setOfIntervals::Union, detail::BooleanValueGetter,
                             decltype(orLambda)>;

/// Boolean And
inline auto andLambda = [](bool a, bool b) { return a && b; };
using ConditionalAndExpression =
    detail::BinaryExpression<setOfIntervals::Intersection,
                             detail::BooleanValueGetter, decltype(andLambda)>;

/// Unary Negation
inline auto unaryNegate = [](bool a) -> bool { return !a; };
using UnaryNegateExpression =
    detail::UnaryExpression<detail::NoRangeCalculation,
                            detail::BooleanValueGetter, decltype(unaryNegate)>;

/// Unary Minus, currently all results are converted to double
inline auto unaryMinus = [](auto a) -> double { return -a; };
using UnaryMinusExpression =
    detail::UnaryExpression<detail::NoRangeCalculation,
                            detail::NumericValueGetter, decltype(unaryMinus)>;

/// Multiplication and Division, currently all results are converted to double.
inline auto multiply = [](const auto& a, const auto& b) -> double {
  return a * b;
};
inline auto divide = [](const auto& a, const auto& b) -> double {
  return static_cast<double>(a) / b;
};
using MultiplicativeExpression = detail::DispatchedBinaryExpression<
    detail::NumericValueGetter, detail::TaggedFunction<"*", decltype(multiply)>,
    detail::TaggedFunction<"/", decltype(divide)>>;

/// Addition and subtraction, currently all results are converted to double.
inline auto add = [](const auto& a, const auto& b) -> double { return a + b; };
inline auto subtract = [](const auto& a, const auto& b) -> double {
  return a - b;
};
using AdditiveExpression = detail::DispatchedBinaryExpression<
    detail::NumericValueGetter, detail::TaggedFunction<"+", decltype(add)>,
    detail::TaggedFunction<"-", decltype(subtract)>>;

/// The base cases: Constants and variables
using BooleanLiteralExpression = detail::LiteralExpression<bool>;
using IntLiteralExpression = detail::LiteralExpression<int64_t>;
using DoubleLiteralExpression = detail::LiteralExpression<double>;
using VariableExpression = detail::LiteralExpression<detail::Variable>;

}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSION_H
