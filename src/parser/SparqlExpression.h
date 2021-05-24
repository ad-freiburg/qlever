
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <memory>
#include <variant>
#include <vector>

#include "../engine/CallFixedSize.h"
#include "../engine/QueryExecutionContext.h"
#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "../util/ConstexprSmallString.h"
#include "./BooleanRangeExpressions.h"

namespace sparqlExpression {
/// Virtual base class for an arbitrary Sparql Expression which holds the
/// structure of the expression as well as the logic to evaluate this expression
/// on a given intermediate result

template <typename T>
class LimitedVector : public std::vector<T, ad_utility::AllocatorWithLimit<T>> {
 public:
  using Base = std::vector<T, ad_utility::AllocatorWithLimit<T>>;
  using Base::Base;
  // Copying is expensive, we never want to do this in the expression
  // evaluation.
  LimitedVector(const LimitedVector&) = delete;
  LimitedVector(LimitedVector&&) = default;
  LimitedVector& operator=(const LimitedVector&) = delete;
  LimitedVector& operator=(LimitedVector&&) = default;
};

/// A strong type for "Ids
struct StrongId {
  Id _value;

  bool operator==(const StrongId&) const = default;
};

/// A StrongId and its type. The type is needed to get the actual value
struct StrongIdAndDatatype {
  StrongId _id;
  ResultTable::ResultType _type;
  bool operator==(const StrongIdAndDatatype&) const = default;
};

/// Typedef for a map from variables names to (input column, input column
/// type) which is needed to evaluate expressions that contain variables.
using VariableColumnMapWithResultTypes =
    ad_utility::HashMap<std::string,
                        std::pair<size_t, ResultTable::ResultType>>;

using VariableColumnMap = ad_utility::HashMap<std::string, size_t>;

/// All the additional information which is needed to evaluate a Sparql
/// Expression
struct EvaluationInput {
  /// Constructor for evaluating an expression on the complete input
  EvaluationInput(const QueryExecutionContext& qec,
                  VariableColumnMapWithResultTypes map,
                  const IdTable& inputTable,
                  const ad_utility::AllocatorWithLimit<Id>& allocator)
      : _qec{qec},
        _variableColumnMap{std::move(map)},
        _inputTable{inputTable},
        _allocator{allocator} {}
  /// Constructor for evaluating an expression on a part of the input
  EvaluationInput(const QueryExecutionContext& qec,
                  VariableColumnMapWithResultTypes map,
                  const IdTable& inputTable, size_t beginIndex, size_t endIndex,
                  const ad_utility::AllocatorWithLimit<Id>& allocator)
      : _qec{qec},
        _variableColumnMap{std::move(map)},
        _inputTable{inputTable},
        _beginIndex{beginIndex},
        _endIndex{endIndex},
        _allocator{allocator} {}
  /// Needed to map Ids to their value from the vocabulary

  const QueryExecutionContext& _qec;
  VariableColumnMapWithResultTypes _variableColumnMap;

  /// The input of the expression
  const IdTable& _inputTable;
  /// The indices of the actual range in the _inputTable on which the
  /// expression is evaluated. For BIND expressions this is always [0,
  /// _inputTable.size()) but for GROUP BY evaluation we also need only parts
  /// of the input.
  size_t _beginIndex = 0;
  size_t _endIndex = _inputTable.size();

  /// The input is sorted on these columns. This information can be used to
  /// perform efficient filter operations like = < >
  std::vector<size_t> _resultSortedOn;

  /// Let the expression evaluation also respect the memory limit.
  ad_utility::AllocatorWithLimit<Id> _allocator;
};

class SparqlExpression {
 public:
  /// A Sparql Variable, e.g. "?x"
  struct Variable {
    std::string _variable;
  };

  using EvaluationInput = sparqlExpression::EvaluationInput;
  using VariableColumnMap = sparqlExpression::VariableColumnMap;
  using VariableColumnMapWithResultTypes = sparqlExpression::VariableColumnMapWithResultTypes;

  /// The result of an epxression variable can either be a constant of type
  /// bool/double/int (same value for all result rows), a vector of one of those
  /// types (one value for each result row), a variable (e.g. in BIND (?x as
  /// ?y)) or a "Set" of indices, which identifies the indices in the result at
  /// which the result columns value is "true".
  using EvaluateResult =
      std::variant<LimitedVector<double>, LimitedVector<int64_t>,
                   LimitedVector<bool>, setOfIntervals::Set, double, int64_t,
                   bool, string, StrongIdAndDatatype, Variable>;

  /// ________________________________________________________________________
  using Ptr = std::unique_ptr<SparqlExpression>;

  /// Evaluate a Sparql expression.
  virtual EvaluateResult evaluate(EvaluationInput*) const = 0;

  /// Returns all variables and IRIs, needed for certain parser methods.
  virtual vector<std::string*> strings() = 0;

  /// Return all the variables, that occur in the expression, but are not
  /// aggregated.
  virtual vector<std::string> getUnaggregatedVariables() = 0;

  virtual string getCacheKey(const VariableColumnMap& varColMap) const = 0;

  virtual ~SparqlExpression() = default;
};

/// We will use the string representation of various functions (e.g. '+' '*')
/// directly as template parameters. Currently 7 characters are enough for this,
/// but if we need longer names in the future, we can still change this at the
/// cost of a recompilation.
using TagString = ad_utility::ConstexprSmallString<16>;

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

  vector<std::string> getUnaggregatedVariables() override {
    if constexpr (std::is_same_v<T, Variable>) {
      return {_value._variable};
    } else {
      return {};
    }
  }

  // ______________________________________________________________________
  string getCacheKey(const VariableColumnMap& varColMap) const override {
    if constexpr (std::is_same_v<T, Variable>) {
      return {"#column_" + std::to_string(varColMap.at(_value._variable)) +
              "#"};
    } else {
      return {std::to_string(_value)};
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
          typename BinaryOperation, TagString Tag>
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

  // _________________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    std::vector<string> result;
    for (const auto& ptr : _children) {
      auto childResult = ptr->getUnaggregatedVariables();
      result.insert(result.end(), std::make_move_iterator(childResult.begin()),
                    std::make_move_iterator(childResult.end()));
    }
    return result;
  }

  string getCacheKey(const VariableColumnMap& varColMap) const override {
    std::vector<string> childKeys;
    for (const auto& ptr : _children) {
      childKeys.push_back("(" + ptr->getCacheKey(varColMap) + ")");
    }
    return ad_utility::join(childKeys, " "s + Tag + " ");
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
          typename UnaryOperation, TagString Tag>
class UnaryExpression : public SparqlExpression {
 public:
  UnaryExpression(Ptr&& child) : _child{std::move(child)} {};
  EvaluateResult evaluate(EvaluationInput* input) const override;
  // _____________________________________________________________________
  vector<std::string*> strings() override { return _child->strings(); }

  // _____________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    return _child->getUnaggregatedVariables();
  }

  // _________________________________________________________________________
  string getCacheKey(const VariableColumnMap& varColMap) const override {
    return std::string{Tag} + "("s + _child->getCacheKey(varColMap) + ")";
  }

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
  vector<std::string> getUnaggregatedVariables() override {
    std::vector<string> result;
    for (const auto& ptr : _children) {
      auto childResult = ptr->getUnaggregatedVariables();
      result.insert(result.end(), std::make_move_iterator(childResult.begin()),
                    std::make_move_iterator(childResult.end()));
    }
    return result;
  }

  string getCacheKey(const VariableColumnMap& varColMap) const override {
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

class EqualsExpression : public SparqlExpression {
 public:
  EqualsExpression(Ptr&& childLeft, Ptr&& childRight)
      : _childLeft{std::move(childLeft)}, _childRight{std::move(childRight)} {}

  // __________________________________________________________________________
  EvaluateResult evaluate(EvaluationInput* input) const override;

  // _____________________________________________________________________
  vector<std::string*> strings() override {
    auto result = _childLeft->strings();
    auto resultRight = _childRight->strings();
    result.insert(result.end(), resultRight.begin(), resultRight.end());
    return result;
  }

  // _______________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    auto result = _childLeft->getUnaggregatedVariables();
    auto resultRight = _childRight->getUnaggregatedVariables();
    result.insert(result.end(), std::make_move_iterator(resultRight.begin()),
                  std::make_move_iterator(resultRight.end()));
    return result;
  }

  string getCacheKey(const VariableColumnMap& varColMap) const override {
    return "(" + _childLeft->getCacheKey(varColMap) + ") = (" +
           _childRight->getCacheKey(varColMap) + ")";
  }

 private:
  Ptr _childLeft;
  Ptr _childRight;
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
  EvaluateResult evaluate(EvaluationInput* input) const override;

  // _____________________________________________________________________
  vector<std::string*> strings() override { return _child->strings(); }
  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  string getCacheKey(const VariableColumnMap& varColMap) const override {
    return std::string{Tag} + "(" + _child->getCacheKey(varColMap) + ")";
  }

 private:
  bool _distinct;
  Ptr _child;
  AggregateOp _aggregateOp;
};

// This can be used as the FinalOp parameter to an Aggregate if there is nothing
// to be done on the final result
inline auto noop = []<typename T>(T && result, size_t) {
  return std::forward<T>(result);
};

/// A Special type to be used as the RangeCalculation template Argument, when a
/// range calculation is not allowed
struct NoRangeCalculation {};

/// This class can be used as the `ValueExtractor` argument of Expression
/// templates. It produces a string value.
struct StringValueGetter {
  template <typename T>
  requires(std::is_arithmetic_v<T>) string operator()(T v,
                                                      EvaluationInput*) const {
    return std::to_string(v);
  }
  string operator()(StrongId id, ResultTable::ResultType type,
                    EvaluationInput*) const;

  string operator()(string s, EvaluationInput*) { return s; }
};

// ______________________________________________________________________________
inline auto makePerformConcat(std::string separator) {
  return [sep = std::move(separator)](string&& a, const string& b) -> string {
    if (a.empty()) [[unlikely]] {
        return b;
      }
    else
      [[likely]] {
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
  EvaluateResult evaluate(EvaluationInput* input) const override {
    // The child is already set up to perform all the work.
    return _child->evaluate(input);
  }

  // _____________________________________________________________________
  vector<std::string*> strings() override { return _child->strings(); }
  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  string getCacheKey(const VariableColumnMap& varColMap) const override {
    return "["s + _separator + "]" + _child->getCacheKey(varColMap);
  }

 private:
  Ptr _child;
  std::string _separator;
};

/// The SAMPLE Expression
class SampleExpression : public SparqlExpression {
 public:
  SampleExpression([[maybe_unused]] bool distinct, Ptr&& child)
      : _child{std::move(child)} {}

  // __________________________________________________________________________
  EvaluateResult evaluate(EvaluationInput* input) const override;

  // _____________________________________________________________________
  vector<std::string*> strings() override { return _child->strings(); }
  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  string getCacheKey(const VariableColumnMap& varColMap) const override {
    return "SAMPLE("s + _child->getCacheKey(varColMap) + ")";
  }

 private:
  Ptr _child;
};

/// This class can be used as the `ValueExtractor` argument of Expression
/// templates. It preserves the numeric datatype.
struct NumericValueGetter {
  // Simply preserve the input from numeric values
  double operator()(double v, EvaluationInput*) const { return v; }
  int64_t operator()(int64_t v, EvaluationInput*) const { return v; }
  bool operator()(bool v, EvaluationInput*) const { return v; }

  // This is the current error-signalling mechanism
  bool operator()(const string&, EvaluationInput*) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  // Convert an id from a result table to a double value
  double operator()(StrongId id, ResultTable::ResultType type,
                    EvaluationInput*) const;
};

/// This class is needed for the distinct calculation in the aggregates.
struct ActualValueGetter {
  // Simply preserve the input from numeric values
  template <typename T>
  T operator()(T v, EvaluationInput*) const {
    return v;
  }

  // _________________________________________________________________________
  StrongId operator()(StrongId id, ResultTable::ResultType,
                      EvaluationInput*) const {
    return id;
  }
};

/// This class can be used as the `ValueExtractor` argument of Expression
/// templates. It produces a boolean value.
struct BooleanValueGetter {
  bool operator()(double v, EvaluationInput*) const {
    return v && !std::isnan(v);
  }
  bool operator()(int64_t v, EvaluationInput*) const { return v; }
  bool operator()(bool v, EvaluationInput*) const { return v; }
  // Convert an id from a result table to a boolean value
  bool operator()(StrongId id, ResultTable::ResultType type,
                  EvaluationInput*) const;
  // TODO<joka921> check if an empty string is indeed "false"
  bool operator()(string s, EvaluationInput*) { return !s.empty(); }
};

// Implementation of the getValuesFromVariable method (see below). Optimized for
// the different IdTable specializations.
// TODO<joka921> Comment is out of date
template <size_t WIDTH>
void getIdsFromVariableImpl(std::vector<StrongId>& result,
                            const SparqlExpression::Variable& variable,
                            EvaluationInput* input) {
  AD_CHECK(result.empty());
  auto staticInput = input->_inputTable.asStaticView<WIDTH>();

  const size_t beginIndex = input->_beginIndex;
  size_t resultSize = input->_endIndex - input->_beginIndex;

  if (!input->_variableColumnMap.contains(variable._variable)) {
    throw std::runtime_error(
        "Variable " + variable._variable +
        " could not be mapped to input column of expression evaluation");
  }

  size_t columnIndex = input->_variableColumnMap[variable._variable].first;

  result.reserve(resultSize);
  for (size_t i = 0; i < resultSize; ++i) {
    result.push_back(StrongId{staticInput(beginIndex + i, columnIndex)});
  }
}

// Convert a variable to a std::vector of all the values it takes in the input
// range specified by `input`. The `valueExtractor` is used convert QLever IDs
// to the appropriate value type.
inline auto getIdsFromVariable(const SparqlExpression::Variable& variable,
                               EvaluationInput* input) {
  auto cols = input->_inputTable.cols();
  std::vector<StrongId> result;
  CALL_FIXED_SIZE_1(cols, getIdsFromVariableImpl, result, variable, input);
  return result;
}

/// TODO<joka921> Comment
template <typename T>
auto possiblyExpand(T&& childResult, [[maybe_unused]] size_t targetSize,
                    EvaluationInput* input) {
  if constexpr (std::is_same_v<setOfIntervals::Set, std::decay_t<T>>) {
    return std::pair{
        setOfIntervals::expandSet(std::move(childResult), targetSize),
        ResultTable::ResultType{}};
  } else if constexpr (std::is_same_v<Variable, std::decay_t<T>>) {
    return std::pair{getIdsFromVariable(std::forward<T>(childResult), input),
                     input->_variableColumnMap[childResult._variable].second};
  } else {
    return std::pair{std::forward<T>(childResult), ResultTable::ResultType{}};
  }
};

/// TODO<joka921> Comment
template <typename T>
auto makeExtractor(T&& expandedResult) {
  return [expanded = std::move(expandedResult)](size_t index) {
    if constexpr (ad_utility::isVector<T>) {
      return expanded[index];
    } else {
      static_assert(!std::is_same_v<Variable, T>);
      return expanded;
    }
  };
}

}  // namespace detail

/// The Actual aliases for the expressions

/// Boolean Or
inline auto orLambda = [](bool a, bool b) { return a || b; };
using ConditionalOrExpression =
    detail::BinaryExpression<setOfIntervals::Union, detail::BooleanValueGetter,
                             decltype(orLambda), "||">;

/// Boolean And
inline auto andLambda = [](bool a, bool b) { return a && b; };
using ConditionalAndExpression =
    detail::BinaryExpression<setOfIntervals::Intersection,
                             detail::BooleanValueGetter, decltype(andLambda),
                             "&&">;

/// Unary Negation
inline auto unaryNegate = [](bool a) -> bool { return !a; };
using UnaryNegateExpression =
    detail::UnaryExpression<detail::NoRangeCalculation,
                            detail::BooleanValueGetter, decltype(unaryNegate),
                            "!">;

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

inline auto count = [](const auto& a, const auto& b) -> int64_t {
  return a + b;
};
using CountExpression =
    detail::AggregateExpression<detail::NoRangeCalculation,
                                detail::BooleanValueGetter, decltype(count),
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
using VariableExpression = detail::LiteralExpression<detail::Variable>;

}  // namespace sparqlExpression

/// Specialize the isVector type trait for our limited type
namespace ad_utility {
template <typename T>
constexpr static bool isVector<sparqlExpression::LimitedVector<T>> = true;
}  // namespace ad_utility

namespace std {
template <>
struct hash<sparqlExpression::StrongId> {
  size_t operator()(const sparqlExpression::StrongId& x) const {
    return std::hash<Id>{}(x._value);
  }
};

template <>
struct hash<sparqlExpression::StrongIdAndDatatype> {
  size_t operator()(const sparqlExpression::StrongIdAndDatatype& x) const {
    return std::hash<sparqlExpression::StrongId>{}(x._id) ^
           std::hash<size_t>{}(static_cast<size_t>(x._type));
  }
};
}  // namespace std

#endif  // QLEVER_SPARQLEXPRESSION_H
