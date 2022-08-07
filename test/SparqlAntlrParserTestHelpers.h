// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <gmock/gmock.h>

#include "../src/parser/Alias.h"
#include "../src/parser/data/VarOrTerm.h"
#include "../src/util/TypeTraits.h"

// Not relevant for the actual test logic, but provides
// human-readable output if a test fails.
std::ostream& operator<<(std::ostream& out, const GraphTerm& graphTerm) {
  std::visit(
      [&]<typename T>(const T& object) {
        if constexpr (ad_utility::isSimilar<T, Literal>) {
          out << "Literal " << object.literal();
        } else if constexpr (ad_utility::isSimilar<T, BlankNode>) {
          out << "BlankNode generated: " << object.isGenerated()
              << ", label: " << object.label();
        } else if constexpr (ad_utility::isSimilar<T, Iri>) {
          out << "Iri " << object.iri();
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "unknown type");
        }
      },
      static_cast<const GraphTermBase&>(graphTerm));
  return out;
}

// _____________________________________________________________________________

std::ostream& operator<<(std::ostream& out, const VarOrTerm& varOrTerm) {
  std::visit(
      [&]<typename T>(const T& object) {
        if constexpr (ad_utility::isSimilar<T, GraphTerm>) {
          out << object;
        } else if constexpr (ad_utility::isSimilar<T, Variable>) {
          out << "Variable " << object.name();
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "unknown type");
        }
      },
      static_cast<const VarOrTermBase&>(varOrTerm));
  return out;
}

// _____________________________________________________________________________

std::ostream& operator<<(std::ostream& out,
                         const GraphPatternOperation::Bind& bind) {
  out << "Bind " << bind._expression.getDescriptor() << " as " << bind._target;
  return out;
}

// _____________________________________________________________________________

std::ostream& operator<<(std::ostream& out, const VariableOrderKey& orderkey) {
  out << "Order " << (orderkey.isDescending_ ? "DESC" : "ASC") << " by "
      << orderkey.variable_;
  return out;
}

std::ostream& operator<<(std::ostream& out,
                         const ExpressionOrderKey& expressionOrderKey) {
  out << "Order " << (expressionOrderKey.isDescending_ ? "DESC" : "ASC")
      << " by " << expressionOrderKey.expression_.getDescriptor();
  return out;
}

std::ostream& operator<<(std::ostream& out,
                         const GraphPatternOperation::Values& values) {
  out << "Values: variables "
      << ::testing::PrintToString(values._inlineValues._variables) << " values "
      << ::testing::PrintToString(values._inlineValues._values);
  return out;
}

// _____________________________________________________________________________

namespace sparqlExpression {

std::ostream& operator<<(
    std::ostream& out,
    const sparqlExpression::SparqlExpressionPimpl& groupKey) {
  out << "Group by " << groupKey.getDescriptor();
  return out;
}
}  // namespace sparqlExpression

// _____________________________________________________________________________

// Recursively unwrap a std::variant object, or return a pointer
// to the argument directly if it is already unwrapped.

template <typename Current, typename... Others>
constexpr const ad_utility::Last<Current, Others...>* unwrapVariant(
    const auto& arg) {
  if constexpr (sizeof...(Others) > 0) {
    if constexpr (ad_utility::isSimilar<decltype(arg), Current>) {
      if (const auto ptr = std::get_if<ad_utility::First<Others...>>(&arg)) {
        return unwrapVariant<Others...>(*ptr);
      }
      return nullptr;
    } else {
      return unwrapVariant<Others...>(arg);
    }
  } else {
    return &arg;
  }
}
// _____________________________________________________________________________
/**
 * Ensures that the matcher matches on the result of the parsing and that the
 * text has been fully consumed by the parser.
 *
 * @param resultOfParseAndText Parsing result
 * @param matcher Matcher that must be fulfilled
 */
void expectCompleteParse(const auto& resultOfParseAndText, auto&& matcher) {
  EXPECT_THAT(resultOfParseAndText.resultOfParse_, matcher);
  EXPECT_TRUE(resultOfParseAndText.remainingText_.empty());
}

// _____________________________________________________________________________
/**
 * Ensures that resultOfParseAndText.resultOfParse_ is an array-like type (e.g.
 * std::vector) the size of which is the number of specified matchers and the
 * the i-th matcher matches resultOfParseAndText.resultOfParser_[i] and that the
 * text has been fully consumed by the parser.
 *
 * @param resultOfParseAndText Parsing result
 * @param matchers Matcher... that must be fulfilled
 */
void expectCompleteArrayParse(const auto& resultOfParseAndText,
                              auto&&... matchers) {
  auto expect_single_element = [](auto&& result, auto matcher) {
    EXPECT_THAT(result, matcher);
  };
  ASSERT_EQ(resultOfParseAndText.resultOfParse_.size(), sizeof...(matchers));
  auto sequence = std::make_index_sequence<sizeof...(matchers)>();

  auto lambda = [&]<size_t... i>(std::index_sequence<i...>) {
    (...,
     expect_single_element(resultOfParseAndText.resultOfParse_[i], matchers));
  };

  lambda(sequence);
  EXPECT_TRUE(resultOfParseAndText.remainingText_.empty());
}
// _____________________________________________________________________________

MATCHER_P(IsIri, value, "") {
  if (const auto iri = unwrapVariant<VarOrTerm, GraphTerm, Iri>(arg)) {
    return iri->iri() == value;
  }
  return false;
}

// _____________________________________________________________________________

MATCHER_P2(IsBlankNode, generated, label, "") {
  if (const auto blankNode =
          unwrapVariant<VarOrTerm, GraphTerm, BlankNode>(arg)) {
    return blankNode->isGenerated() == generated && blankNode->label() == label;
  }
  return false;
}

// _____________________________________________________________________________

MATCHER_P(IsVariable, value, "") {
  if (const auto variable = unwrapVariant<VarOrTerm, Variable>(arg)) {
    return variable->name() == value;
  }
  return false;
}

// _____________________________________________________________________________

MATCHER_P(IsLiteral, value, "") {
  if (const auto literal = unwrapVariant<VarOrTerm, GraphTerm, Literal>(arg)) {
    return literal->literal() == value;
  }
  return false;
}

// _____________________________________________________________________________

MATCHER_P2(IsBind, variable, expression, "") {
  return (arg._target == variable) &&
         (arg._expression.getDescriptor() == expression);
}

MATCHER_P(IsBindExpression, expression, "") {
  return (arg._expression.getDescriptor() == expression);
}

MATCHER_P3(IsLimitOffset, limit, textLimit, offset, "") {
  return (arg._limit == limit) && (arg._textLimit == textLimit) &&
         (arg._offset == offset);
}

MATCHER_P2(IsVariableOrderKey, key, desc, "") {
  if (const auto variableOrderKey =
          unwrapVariant<OrderKey, VariableOrderKey>(arg)) {
    return (variableOrderKey->variable_ == key) &&
           (variableOrderKey->isDescending_ == desc);
  }
  return false;
}

MATCHER_P2(IsExpressionOrderKey, expr, desc, "") {
  if (const auto bindOrderKey =
          unwrapVariant<OrderKey, ExpressionOrderKey>(arg)) {
    return (bindOrderKey->expression_.getDescriptor() == expr) &&
           (bindOrderKey->isDescending_ == desc);
  }
  return false;
}

MATCHER_P(IsVariableGroupKey, key, "") {
  if (const auto variable = unwrapVariant<GroupKey, Variable>(arg)) {
    return (variable->name() == key);
  }
  return false;
}

MATCHER_P(IsExpressionGroupKey, expr, "") {
  if (const auto expression =
          unwrapVariant<GroupKey, sparqlExpression::SparqlExpressionPimpl>(
              arg)) {
    return (expression->getDescriptor() == expr);
  }
  return false;
}

MATCHER_P2(IsAliasGroupKey, expr, variable, "") {
  if (const auto alias = unwrapVariant<GroupKey, Alias>(arg)) {
    return (alias->_expression.getDescriptor() == expr) &&
           (alias->_outVarName == variable);
  }
  return false;
}

MATCHER_P(GroupByVariablesMatch, vars, "") {
  vector<Variable> groupVariables = arg._groupByVariables;
  if (groupVariables.size() != vars.size()) return false;
  return std::equal(groupVariables.begin(), groupVariables.end(), vars.begin(),
                    [](auto& var, auto& var1) { return var.name() == var1; });
}

MATCHER_P2(IsValues, vars, values, "") {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  return (arg._inlineValues._variables == vars) &&
         (arg._inlineValues._values == values);
}

MATCHER_P2(IsAsteriskSelect, distinct, reduced, "") {
  return arg._distinct == distinct && arg._reduced == reduced &&
         arg.isAsterisk() && arg.getAliases().empty();
}

MATCHER_P3(IsVariablesSelect, distinct, reduced, variables, "") {
  return arg._distinct == distinct && arg._reduced == reduced &&
         arg.getSelectedVariablesAsStrings() == variables &&
         arg.getAliases().empty();
}

MATCHER_P3(IsSelect, distinct, reduced, selection, "") {
  const auto& selectedVariables = arg.getSelectedVariables();
  if (selection.size() != selectedVariables.size()) return false;
  size_t alias_counter = 0;
  for (size_t i = 0; i < selection.size(); i++) {
    if (holds_alternative<Variable>(selection[i])) {
      if (get<Variable>(selection[i]) != selectedVariables[i]) return false;
    } else {
      auto pair = get<std::pair<string, string>>(selection[i]);
      if (pair.first !=
              arg.getAliases()[alias_counter]._expression.getDescriptor() ||
          pair.second != arg.getAliases()[alias_counter++]._outVarName ||
          pair.second != selectedVariables[i].name())
        return false;
    }
  }
  return arg._distinct == distinct && arg._reduced == reduced &&
         arg.getAliases().size() == alias_counter;
}
