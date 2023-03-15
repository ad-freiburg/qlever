//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
namespace detail {

/// An expression with a single value, for example a numeric (42.0) or boolean
/// (false) constant or a variable (?x) or a string or iri (<Human>). These are
/// the "leaves" in the expression tree.
template <typename T>
class LiteralExpression : public SparqlExpression {
 public:
  // _________________________________________________________________________
  explicit LiteralExpression(T _value) : _value{std::move(_value)} {}

  // A simple getter for the stored value.
  const T& value() const { return _value; }

  // Evaluating just returns the constant/literal value.
  ExpressionResult evaluate(
      [[maybe_unused]] EvaluationContext* context) const override {
    if constexpr (std::is_same_v<string, T>) {
      Id id;
      bool idWasFound = context->_qec.getIndex().getId(_value, &id);
      if (!idWasFound) {
        return _value;
      }
      return id;
    } else if constexpr (std::is_same_v<Variable, T>) {
      return evaluateIfVariable(context, _value);
    } else {
      return _value;
    }
  }

  // Literal expressions don't have children
  std::span<SparqlExpression::Ptr> children() override { return {}; }

  // Variables and string constants add their values.
  std::span<const Variable> getContainedVariablesNonRecursive() const override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      return {&_value, 1};
    } else {
      return {};
    }
  }

  // _________________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      return {_value.name()};
    } else {
      return {};
    }
  }

  // ______________________________________________________________________
  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      if (!varColMap.contains(_value)) {
        AD_THROW(absl::StrCat("Variable ", _value.name(), " not found"));
      }
      return {"#column_" + std::to_string(varColMap.at(_value)) + "#"};
    } else if constexpr (std::is_same_v<T, string>) {
      return _value;
    } else if constexpr (std::is_same_v<T, ValueId>) {
      return absl::StrCat("#valueId ", _value.getBits(), "#");
    } else {
      return {std::to_string(_value)};
    }
  }

  // ______________________________________________________________________
  bool isConstantExpression() const override {
    return !std::is_same_v<T, ::Variable>;
  }

 protected:
  // _________________________________________________________________________
  std::optional<::Variable> getVariableOrNullopt() const override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      return _value;
    }
    return std::nullopt;
  }

 private:
  T _value;

  // Evaluate the expression if it is a variable expression with the given
  // `variable`. The variable is passed in explicitly because this function
  // might be called recursively.
  ExpressionResult evaluateIfVariable(EvaluationContext* context,
                                      const Variable& variable) const {
    // If this is a variable that is not visible in the input but was bound by a
    // previous alias in the same SELECT clause, then read the constant value of
    // the variable from the data structures dedicated to this case.
    auto optionalResultFromSameRow =
        context->getResultFromPreviousAggregate(variable);
    if (optionalResultFromSameRow.has_value() &&
        !context->_groupedVariables.contains(variable)) {
      // If the expression is a simple renaming of a variable `(?x AS ?y)` we
      // have to recurse to track a possible chain of such renamings in the
      // SELECT clause.
      if (const Variable* var =
              std::get_if<Variable>(&optionalResultFromSameRow.value());
          var != nullptr) {
        return evaluateIfVariable(context, *var);
      } else {
        return std::move(optionalResultFromSameRow.value());
      }
    }
    // If a variable is grouped, then we know that it always has the same
    // value and can treat it as a constant. This is not possible however when
    // we are inside an aggregate, because for example `SUM(?variable)` must
    // still compute the sum over the whole group.
    if (context->_groupedVariables.contains(variable) && !isInsideAggregate()) {
      auto column = context->getColumnIndexForVariable(variable);
      const auto& table = context->_inputTable;
      auto constantValue = table.at(context->_beginIndex, column);
      assert((std::ranges::all_of(
          table.begin() + context->_beginIndex,
          table.begin() + context->_endIndex,
          [&](const auto& row) { return row[column] == constantValue; })));
      return constantValue;
    } else {
      return variable;
    }
  }
};
}  // namespace detail

///  The actual instantiations and aliases of LiteralExpressions.
using BoolExpression = detail::LiteralExpression<bool>;
using IntExpression = detail::LiteralExpression<int64_t>;
using DoubleExpression = detail::LiteralExpression<double>;
using VariableExpression = detail::LiteralExpression<::Variable>;
using StringOrIriExpression = detail::LiteralExpression<string>;
using IdExpression = detail::LiteralExpression<ValueId>;
}  // namespace sparqlExpression
