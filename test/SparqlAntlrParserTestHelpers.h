// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2022 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#pragma once

#include <gmock/gmock.h>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/Alias.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlParserHelpers.h"
#include "parser/data/OrderKey.h"
#include "parser/data/VarOrTerm.h"
#include "util/TypeTraits.h"

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

std::ostream& operator<<(std::ostream& out, const ExceptionMetadata& metadata) {
  out << "ExceptionMetadata(\"" << metadata.query_ << "\", "
      << metadata.startIndex_ << ", " << metadata.stopIndex_ << ", "
      << metadata.line_ << ", " << metadata.charPositionInLine_ << ")";
  return out;
}

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

// Cannot be broken down into a combination of existing googletest matchers
// because unwrapVariant is not available as a matcher.
MATCHER_P(IsIri, value, "") {
  if (const auto iri = unwrapVariant<VarOrTerm, GraphTerm, Iri>(arg)) {
    return iri->iri() == value;
  }
  return false;
}

// _____________________________________________________________________________

// Cannot be broken down into a combination of existing googletest matchers
// because unwrapVariant is not available as a matcher.
MATCHER_P2(IsBlankNode, generated, label, "") {
  if (const auto blankNode =
          unwrapVariant<VarOrTerm, GraphTerm, BlankNode>(arg)) {
    return blankNode->isGenerated() == generated && blankNode->label() == label;
  }
  return false;
}

// _____________________________________________________________________________

auto IsVariable = [](const std::string& value) {
  return testing::Property("name()", &Variable::name, testing::StrEq(value));
};

auto IsVariableVariant = [](const std::string& value) {
  return testing::VariantWith<Variable>(IsVariable(value));
};

// _____________________________________________________________________________

// Cannot be broken down into a combination of existing googletest matchers
// because unwrapVariant is not available as a matcher.
MATCHER_P(IsLiteral, value, "") {
  if (const auto literal = unwrapVariant<VarOrTerm, GraphTerm, Literal>(arg)) {
    return literal->literal() == value;
  }
  return false;
}

// _____________________________________________________________________________

auto IsExpression = [](const std::string& descriptor) {
  return testing::Property(
      "getDescriptor()",
      &sparqlExpression::SparqlExpressionPimpl::getDescriptor,
      testing::StrEq(descriptor));
};

template <typename T>
auto IsGraphPatternOperation = [](auto subMatcher) {
  return testing::Field("variant_", &GraphPatternOperation::variant_,
                        testing::VariantWith<T>(subMatcher));
};

// Return a matcher that checks that a given Bind has the given expression
// descriptor.
auto IsBindExpression = [](const string& expression) {
  return testing::Field("_expression",
                        &GraphPatternOperation::Bind::_expression,
                        IsExpression(expression));
};

// Return a matcher that checks that a given GraphPatternOperation is a Bind
// with the given variable and expression descriptor.
auto IsBind = [](const string& variable, const string& expression) {
  return IsGraphPatternOperation<GraphPatternOperation::Bind>(testing::AllOf(
      IsBindExpression(expression),
      testing::Field("_target", &GraphPatternOperation::Bind::_target,
                     testing::StrEq(variable))));
};

auto IsLimitOffset = [](uint64_t limit, uint64_t textLimit, uint64_t offset) {
  return testing::AllOf(
      testing::Field("_limit", &LimitOffsetClause::_limit, testing::Eq(limit)),
      testing::Field("_textLimit", &LimitOffsetClause::_textLimit,
                     testing::Eq(textLimit)),
      testing::Field("_offset", &LimitOffsetClause::_offset,
                     testing::Eq(offset)));
};

auto IsVariableOrderKey = [](const string& key, bool desc) {
  return testing::AllOf(
      testing::Field("variable_", &VariableOrderKey::variable_,
                     testing::Eq(key)),
      testing::Field("isDescending_", &VariableOrderKey::isDescending_,
                     testing::Eq(desc)));
};

auto IsVariableOrderKeyVariant = [](const string& key, bool desc) {
  return testing::VariantWith<VariableOrderKey>(IsVariableOrderKey(key, desc));
};

auto IsExpressionOrderKey = [](const string& expr, bool desc) {
  return testing::VariantWith<ExpressionOrderKey>(testing::AllOf(
      testing::Field("expression_", &ExpressionOrderKey::expression_,
                     IsExpression(expr)),
      testing::Field("isDescending_", &ExpressionOrderKey::isDescending_,
                     testing::Eq(desc))));
};

using ExpressionOrderKeyTest = std::pair<std::string, bool>;
auto IsOrderKeys =
    [](const std::vector<
        std::variant<VariableOrderKey, ExpressionOrderKeyTest>>& orderKeys) {
      vector<testing::Matcher<OrderKey>> keyMatchers;
      auto variableOrderKey =
          [](const VariableOrderKey& key) -> testing::Matcher<OrderKey> {
        // TODO: enhance
        return IsVariableOrderKeyVariant(key.variable_, key.isDescending_);
      };
      auto expressionOrderKey = [](const std::pair<std::string, bool>& key)
          -> testing::Matcher<OrderKey> {
        return IsExpressionOrderKey(key.first, key.second);
      };
      for (auto& key : orderKeys) {
        keyMatchers.push_back(
            std::visit(ad_utility::OverloadCallOperator{variableOrderKey,
                                                        expressionOrderKey},
                       key));
      }
      return testing::ElementsAreArray(keyMatchers);
    };

auto IsVariableGroupKey = [](const string& key) {
  return testing::VariantWith<Variable>(
      testing::Property("name()", &Variable::name, testing::StrEq(key)));
};

auto IsExpressionGroupKey = [](const string& expr) {
  return testing::VariantWith<sparqlExpression::SparqlExpressionPimpl>(
      IsExpression(expr));
};

auto IsAliasGroupKey = [](const string& expr, const string& variable) {
  return testing::VariantWith<Alias>(testing::AllOf(
      testing::Field("_outVarName", &Alias::_outVarName,
                     testing::StrEq(variable)),
      testing::Field("_expression", &Alias::_expression, IsExpression(expr))));
};

auto IsGroupKeys =
    [](const std::vector<std::variant<
           std::string, std::pair<std::string, std::string>, Variable>>&
           groupKeys) {
      vector<testing::Matcher<GroupKey>> keyMatchers;
      auto variableGroupKey =
          [](const Variable& key) -> testing::Matcher<GroupKey> {
        return IsVariableGroupKey(key.name());
      };
      auto expressionGroupKey =
          [](const std::string& key) -> testing::Matcher<GroupKey> {
        return IsExpressionGroupKey(key);
      };
      auto aliasGroupKey = [](const std::pair<std::string, std::string>& alias)
          -> testing::Matcher<GroupKey> {
        return IsAliasGroupKey(alias.first, alias.second);
      };
      for (auto& key : groupKeys) {
        keyMatchers.push_back(std::visit(
            ad_utility::OverloadCallOperator{variableGroupKey,
                                             expressionGroupKey, aliasGroupKey},
            key));
      }
      return testing::ElementsAreArray(keyMatchers);
    };

auto GroupByVariablesMatch = [](const vector<Variable>& vars) {
  return testing::Field("_groupByVariables", &ParsedQuery::_groupByVariables,
                        testing::UnorderedElementsAreArray(vars));
};

auto IsValues = [](const vector<string>& vars,
                   const vector<vector<string>>& values) {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  return testing::AllOf(testing::Field(
      "_inlineValues", &GraphPatternOperation::Values::_inlineValues,
      testing::AllOf(testing::Field("_variables", &SparqlValues::_variables,
                                    testing::Eq(vars)),
                     testing::Field("_values", &SparqlValues::_values,
                                    testing::Eq(values)))));
};

auto IsInlineData = [](const vector<string>& vars,
                       const vector<vector<string>>& values) {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  return IsGraphPatternOperation<GraphPatternOperation::Values>(
      IsValues(vars, values));
};

auto IsSelectBase = [](bool distinct, bool reduced) {
  return testing::AllOf(
      testing::Field("_distinct", &ParsedQuery::SelectClause::_distinct,
                     testing::Eq(distinct)),
      testing::Field("_reduced", &ParsedQuery::SelectClause::_reduced,
                     testing::Eq(reduced)),
      testing::Property("getAliases()", &ParsedQuery::SelectClause::getAliases,
                        testing::IsEmpty()));
};

auto IsAsteriskSelect = [](bool distinct = false, bool reduced = false) {
  return testing::AllOf(
      IsSelectBase(distinct, reduced),
      testing::Property("isAsterisk()", &ParsedQuery::SelectClause::isAsterisk,
                        testing::IsTrue()));
};

auto IsVariablesSelect = [](const vector<string>& variables,
                            bool distinct = false, bool reduced = false) {
  return testing::AllOf(
      IsSelectBase(distinct, reduced),
      testing::Property(
          "getSelectedVariablesAsStrings()",
          &ParsedQuery::SelectClause::getSelectedVariablesAsStrings,
          testing::Eq(variables)));
};

// This matcher cannot be trivially broken down into a combination of existing
// googletest matchers because of the way how the aliases are stored in the
// select clause.
MATCHER_P3(IsSelect, distinct, reduced, selection, "") {
  const auto& selectedVariables = arg.getSelectedVariables();
  if (selection.size() != selectedVariables.size()) return false;
  size_t alias_counter = 0;
  for (size_t i = 0; i < selection.size(); i++) {
    if (holds_alternative<Variable>(selection[i])) {
      if (get<Variable>(selection[i]) != selectedVariables[i]) {
        *result_listener << "where Variable#" << i << " = "
                         << testing::PrintToString(selectedVariables[i]);
        return false;
      }
    } else {
      auto pair = get<std::pair<string, string>>(selection[i]);
      if (alias_counter >= arg.getAliases().size()) {
        *result_listener << "where selected Variables contain less Aliases ("
                         << testing::PrintToString(alias_counter)
                         << ") than provided to matcher";
        return false;
      }
      if (pair.first !=
              arg.getAliases()[alias_counter]._expression.getDescriptor() ||
          pair.second != arg.getAliases()[alias_counter++]._outVarName ||
          pair.second != selectedVariables[i].name()) {
        *result_listener << "where Alias#" << i << " = "
                         << testing::PrintToString(
                                arg.getAliases()[alias_counter - 1]);
        return false;
      }
    }
  }
  return testing::ExplainMatchResult(
      testing::AllOf(
          testing::Field("_distinct", &ParsedQuery::SelectClause::_distinct,
                         testing::Eq(distinct)),
          testing::Property("getAliases()",
                            &ParsedQuery::SelectClause::getAliases,
                            testing::SizeIs(testing::Eq(alias_counter))),
          testing::Field("_reduced", &ParsedQuery::SelectClause::_reduced,
                         testing::Eq(reduced))),
      arg, result_listener);
}

auto IsSolutionModifier =
    [](const std::vector<std::variant<
           std::string, std::pair<std::string, std::string>, Variable>>&
           groupKeys = {},
       const vector<SparqlFilter>& havingClauses = {},
       const std::vector<std::variant<VariableOrderKey,
                                      ExpressionOrderKeyTest>>& orderKeys = {},
       const LimitOffsetClause& limitOffset = {}) {
      return testing::AllOf(
          testing::Field("groupByVariables_",
                         &SolutionModifiers::groupByVariables_,
                         IsGroupKeys(groupKeys)),
          testing::Field("havingClauses_", &SolutionModifiers::havingClauses_,
                         testing::Eq(havingClauses)),
          testing::Field("orderBy_", &SolutionModifiers::orderBy_,
                         IsOrderKeys(orderKeys)),
          testing::Field("limitOffset_", &SolutionModifiers::limitOffset_,
                         testing::Eq(limitOffset)));
    };

auto IsTriples = [](const vector<SparqlTriple>& triples) {
  return IsGraphPatternOperation<GraphPatternOperation::BasicGraphPattern>(
      testing::Field("_triples",
                     &GraphPatternOperation::BasicGraphPattern::_triples,
                     testing::UnorderedElementsAreArray(triples)));
};

auto IsOptional = [](auto&& subMatcher) {
  return IsGraphPatternOperation<GraphPatternOperation::Optional>(
      testing::Field("_child", &GraphPatternOperation::Optional::_child,
                     subMatcher));
};

auto IsGroup = [](auto&& subMatcher) {
  return IsGraphPatternOperation<GraphPatternOperation::GroupGraphPattern>(
      testing::Field("_child",
                     &GraphPatternOperation::GroupGraphPattern::_child,
                     subMatcher));
};

auto IsUnion = [](auto&& subMatcher1, auto&& subMatcher2) {
  return IsGraphPatternOperation<GraphPatternOperation::Union>(testing::AllOf(
      testing::Field("_child1", &GraphPatternOperation::Union::_child1,
                     subMatcher1),
      testing::Field("_child2", &GraphPatternOperation::Union::_child2,
                     subMatcher2)));
};

auto IsMinus = [](auto&& subMatcher) {
  return IsGraphPatternOperation<GraphPatternOperation::Minus>(testing::Field(
      "_child", &GraphPatternOperation::Minus::_child, subMatcher));
};

auto IsGraphPattern = [](bool optional, const vector<SparqlFilter>& filters,
                         auto&&... childMatchers) {
  return testing::AllOf(
      testing::Field("_optional", &ParsedQuery::GraphPattern::_optional,
                     testing::Eq(optional)),
      testing::Field("_filters", &ParsedQuery::GraphPattern::_filters,
                     testing::UnorderedElementsAreArray(filters)),
      testing::Field("_graphPatterns",
                     &ParsedQuery::GraphPattern::_graphPatterns,
                     testing::ElementsAre(childMatchers...)));
};

auto IsGraphPatternSimple = [](auto&&... childMatchers) {
  return IsGraphPattern(false, {}, childMatchers...);
};

auto OptionalGraphPattern = [](vector<SparqlFilter>&& filters,
                               const auto&... childMatchers) {
  return IsOptional(IsGraphPattern(true, filters, childMatchers...));
};

auto GroupGraphPattern = [](vector<SparqlFilter>&& filters,
                            const auto&... childMatchers) {
  return IsGroup(IsGraphPattern(false, filters, childMatchers...));
};

auto MinusGraphPattern = [](vector<SparqlFilter>&& filters,
                            const auto&... childMatchers) {
  return IsMinus(IsGraphPattern(false, filters, childMatchers...));
};

auto IsSubSelect = [](auto&& selectMatcher, auto&& whereMatcher) {
  return IsGraphPatternOperation<GraphPatternOperation::Subquery>(
      testing::Field(
          "_subquery", &GraphPatternOperation::Subquery::_subquery,
          testing::AllOf(
              testing::Property("hasSelectClause()",
                                &ParsedQuery::hasSelectClause,
                                testing::IsTrue()),
              testing::Property("selectClause()", &ParsedQuery::selectClause,
                                selectMatcher),
              testing::Field("_rootGraphPattern",
                             &ParsedQuery::_rootGraphPattern, whereMatcher))));
};
