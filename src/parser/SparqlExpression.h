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

class SparqlExpression {
 public:

  struct Variable {
    std::string _variable;
    size_t _columnIndex = size_t(-1);
    ResultTable::ResultType _type;
  };

  using VariableColumnMap =
  ad_utility::HashMap<std::string,
      std::pair<size_t, ResultTable::ResultType>>;

  struct EvaluationInput {
    const QueryExecutionContext* _qec;
    IdTable::const_iterator _begin;
    IdTable::const_iterator _end;
  };
  using EvaluateResult =
  std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>,
      setOfIntervals::Set, double, int64_t, bool, Variable>;

  using Ptr = std::unique_ptr<SparqlExpression>;
  virtual EvaluateResult evaluate(EvaluationInput*) const = 0;
  virtual void initializeVariables(
      const VariableColumnMap& variableColumnMap) = 0;

  virtual vector<std::string*> strings() = 0;
  virtual ~SparqlExpression() = default;
};

// We will use the string representation of various functions (e.g. '+' '*') directly as template parameters. Currently 7 characters are enough for this, but if we need longer names in the future, we can still change this at the cost of a recompilation.
using conststr = ad_utility::ConstexprSmallString<8>;

namespace detail {

using EvaluationInput = SparqlExpression::EvaluationInput;
using Variable = SparqlExpression::Variable;


    template <conststr Tag, typename Function>
    struct TaggedFunction {
  static constexpr conststr tag = Tag;
  using functionType = Function;
};


// TODO<joka921> : CommentThis
double getNumericValueFromVariable(const SparqlExpression::Variable& variable, size_t i,
                                          EvaluationInput* input);



template <typename T>
class LiteralExpression : public SparqlExpression {
 public:
  LiteralExpression(T _value) : _value{std::move(_value)} {}
  EvaluateResult evaluate(EvaluationInput*) const override {
    return EvaluateResult{_value};
  }
  void initializeVariables(
      [[maybe_unused]] const VariableColumnMap& variableColumnMap) override {
    if constexpr (std::is_same_v<T, Variable>) {
      if (!variableColumnMap.contains(_value._variable)) {
        throw std::runtime_error(
            "Variable " + _value._variable +
            "could not be mapped to a column of an expression input");
      }
      _value._columnIndex = variableColumnMap.at(_value._variable).first;
      _value._type = variableColumnMap.at(_value._variable).second;
    }
  }

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

template <bool RangeCalculationAllowed, typename RangeCalculation,
          typename ValueExtractor, typename BinaryOperation>
class BinaryExpression : public SparqlExpression {
 public:
  BinaryExpression(std::vector<Ptr>&& children)
      : _children{std::move(children)} {};
  EvaluateResult evaluate(EvaluationInput* input) const override;
  ;
  void initializeVariables(
      const VariableColumnMap& variableColumnMap) override {
    for (const auto& ptr : _children) {
      ptr->initializeVariables(variableColumnMap);
    }
  }

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

template <bool RangeCalculationAllowed, typename RangeCalculation,
          typename ValueExtractor, typename UnaryOperation>
class UnaryExpression : public SparqlExpression {
 public:
  UnaryExpression(Ptr&& child) : _child{std::move(child)} {};
  EvaluateResult evaluate(EvaluationInput* input) const override;
  void initializeVariables(
      const VariableColumnMap& variableColumnMap) override {
    _child->initializeVariables(variableColumnMap);
  }
  // _____________________________________________________________________
  vector<std::string*> strings() override { return _child->strings(); }

 private:
  Ptr _child;
};

template <typename ValueExtractor, typename... TagAndFunctions>
class DispatchedBinaryExpression2 : public SparqlExpression {
 public:
  DispatchedBinaryExpression2(std::vector<Ptr>&& children,
                              std::vector<conststr>&& relations)
      : _children{std::move(children)}, _relations{std::move(relations)} {};
  EvaluateResult evaluate(EvaluationInput* input) const override;
  void initializeVariables(
      const VariableColumnMap& variableColumnMap) override {
    for (const auto& ptr : _children) {
      ptr->initializeVariables(variableColumnMap);
    }
  }

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
  std::vector<conststr> _relations;
};

struct NumericValueGetter {
  double operator()(double v, EvaluationInput*) const { return v; }
  int64_t operator()(int64_t v, EvaluationInput*) const { return v; }
  bool operator()(bool v, EvaluationInput*) const { return v; }
};

struct BooleanValueGetter {
  bool operator()(double v, EvaluationInput*) const { return v; }
  bool operator()(int64_t v, EvaluationInput*) const { return v; }
  bool operator()(bool v, EvaluationInput*) const { return v; }
};

}

/// The Actual aliases for the expressions

inline auto orLambda = [](bool a, bool b) { return a || b; };
using ConditionalOrExpression =
    detail::BinaryExpression<true, setOfIntervals::Union, detail::BooleanValueGetter,
                     decltype(orLambda)>;

inline auto andLambda = [](bool a, bool b) { return a && b; };
using ConditionalAndExpression =
    detail::BinaryExpression<true, setOfIntervals::Intersection, detail::BooleanValueGetter,
                     decltype(andLambda)>;

struct EmptyType{};
inline auto unaryNegate = [](bool a) -> bool { return !a; };
using UnaryNegateExpression =
    detail::UnaryExpression<false, EmptyType, detail::BooleanValueGetter,
                    decltype(unaryNegate)>;
inline auto unaryMinus = [](auto a) -> double { return -a; };
using UnaryMinusExpression =
    detail::UnaryExpression<false, EmptyType, detail::NumericValueGetter,
                    decltype(unaryMinus)>;

inline auto multiply = [](const auto& a, const auto& b) -> double { return a * b; };
inline auto divide = [](const auto& a, const auto& b) -> double { return static_cast<double>(a) / b; };
using MultiplicativeExpression =
    detail::DispatchedBinaryExpression2<detail::NumericValueGetter, detail::TaggedFunction<"*", decltype(multiply)>, detail::TaggedFunction<"/", decltype(divide)>>;

inline auto add = [](const auto& a, const auto& b) -> double { return a + b; };
inline auto subtract = [](const auto& a, const auto& b) -> double { return a - b; };
using AdditiveExpression =
    detail::DispatchedBinaryExpression2<detail::NumericValueGetter, detail::TaggedFunction<"+", decltype(add)>, detail::TaggedFunction<"-", decltype(subtract)>>;

using BooleanLiteralExpression = detail::LiteralExpression<bool>;
using IntLiteralExpression = detail::LiteralExpression<int64_t>;
using DoubleLiteralExpression = detail::LiteralExpression<double>;
using VariableExpression = detail::LiteralExpression<detail::Variable>;

}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSION_H
