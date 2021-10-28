//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 29.09.21.
//

#ifndef QLEVER_LITERALEXPRESSION_H
#define QLEVER_LITERALEXPRESSION_H

#include "./SparqlExpression.h"

namespace sparqlExpression {
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
      return StrongIdWithResultType{{id}, ResultTable::ResultType::KB};
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
}  // namespace detail

///  The actual instantiations and aliases of LiteralExpressions.
using BoolExpression = detail::LiteralExpression<bool>;
using IntExpression = detail::LiteralExpression<int64_t>;
using DoubleExpression = detail::LiteralExpression<double>;
using VariableExpression = detail::LiteralExpression<Variable>;
using StringOrIriExpression = detail::LiteralExpression<string>;
}  // namespace sparqlExpression

#endif  // QLEVER_LITERALEXPRESSION_H
