// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

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
      << metadata.startIndex_ << ", " << metadata.stopIndex_ << ")";
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
  return testing::ExplainMatchResult(
      testing::Property("name()", &Variable::name, testing::StrEq(value)), arg,
      result_listener);
}

MATCHER_P(IsVariableVariant, value, "") {
  return testing::ExplainMatchResult(
      testing::VariantWith<Variable>(IsVariable(value)), arg, result_listener);
}

// _____________________________________________________________________________

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

// Would require ugly `IsGraphPatternOperation.template
// operator()<GraphPatternOperation::Bind>(...)` syntax
auto IsGraphPatternOperation = []<typename T>(auto subMatcher) {
  return testing::Field("_variant", &GraphPatternOperation::variant_,
                        testing::VariantWith<T>(subMatcher));
};

auto IsBind = [](const string& variable, const string& expression) {
  return testing::Field(
      "_variant", &GraphPatternOperation::variant_,
      testing::VariantWith<GraphPatternOperation::Bind>((testing::AllOf(
          testing::Field("_expression",
                         &GraphPatternOperation::Bind::_expression,
                         IsExpression(expression)),
          testing::Field("_target", &GraphPatternOperation::Bind::_target,
                         testing::StrEq(variable))))));
};

auto IsBindExpression = [](const string& expression) {
  return testing::Field("_expression",
                        &GraphPatternOperation::Bind::_expression,
                        IsExpression(expression));
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

MATCHER_P(GroupByVariablesMatch, vars, "") {
  vector<Variable> groupVariables = arg._groupByVariables;
  if (groupVariables.size() != vars.size()) return false;
  return std::equal(groupVariables.begin(), groupVariables.end(), vars.begin(),
                    [](auto& var, auto& var1) { return var.name() == var1; });
}

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
  return testing::Field("variant_", &GraphPatternOperation::variant_,
                        testing::VariantWith<GraphPatternOperation::Values>(
                            IsValues(vars, values)));
};

MATCHER_P2(IsAsteriskSelect, distinct, reduced, "") {
  return arg._distinct == distinct && arg._reduced == reduced &&
         arg.isAsterisk() && arg.getAliases().empty();
}

auto IsVariablesSelect = [](const vector<string>& variables,
                            bool distinct = false, bool reduced = false) {
  return testing::AllOf(
      testing::Field("_distinct", &ParsedQuery::SelectClause::_distinct,
                     testing::Eq(distinct)),
      testing::Field("_reduced", &ParsedQuery::SelectClause::_reduced,
                     testing::Eq(reduced)),
      testing::Property(
          "getSelectedVariablesAsStrings()",
          &ParsedQuery::SelectClause::getSelectedVariablesAsStrings,
          testing::Eq(variables)),
      testing::Property("getAliases()", &ParsedQuery::SelectClause::getAliases,
                        testing::IsEmpty()));
};

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
          testing::Field("_distinct", &ParsedQuery::SelectClause::_distinct,
                         testing::Eq(distinct))),
      arg, result_listener);
}

MATCHER_P4(IsSolutionModifier, groupByVariables, havingClauses, orderBy,
           limitOffset, "") {
  auto equalComp = []<typename T>(const T& a, const T& b) { return a == b; };
  auto falseComp = [](auto, auto) { return false; };
  auto exprOrderKeyComp = [](const ExpressionOrderKey& a,
                             const std::pair<std::string, bool>& b) {
    return a.isDescending_ == b.second &&
           a.expression_.getDescriptor() == b.first;
  };
  auto pimplComp = [](const sparqlExpression::SparqlExpressionPimpl& a,
                      const std::string& b) { return a.getDescriptor() == b; };
  auto orderKeyComp = [&exprOrderKeyComp, &equalComp, &falseComp](
                          const OrderKey& a,
                          const std::variant<std::pair<std::string, bool>,
                                             VariableOrderKey>& b) {
    return std::visit(ad_utility::OverloadCallOperator{exprOrderKeyComp,
                                                       equalComp, falseComp},
                      a, b);
  };
  auto groupKeyComp = [&pimplComp, &equalComp, &falseComp](
                          const GroupKey& a,
                          const std::variant<std::string, Alias, Variable>& b) {
    return std::visit(
        ad_utility::OverloadCallOperator{pimplComp, equalComp, falseComp}, a,
        b);
  };
  return std::equal(arg.groupByVariables_.begin(), arg.groupByVariables_.end(),
                    groupByVariables.begin(), groupKeyComp) &&
         arg.havingClauses_ == havingClauses &&
         std::equal(arg.orderBy_.begin(), arg.orderBy_.end(), orderBy.begin(),
                    orderKeyComp) &&
         arg.limitOffset_ == limitOffset;
}

auto IsTriples = [](const vector<SparqlTriple>& triples) {
  return testing::VariantWith<GraphPatternOperation::BasicGraphPattern>(
      testing::Field("_triples",
                     &GraphPatternOperation::BasicGraphPattern::_triples,
                     testing::UnorderedElementsAreArray(triples)));
};

auto IsOptional = [](auto&& subMatcher) {
  return testing::Field(
      "variant_", &GraphPatternOperation::variant_,
      testing::VariantWith<GraphPatternOperation::Optional>(testing::Field(
          "_child", &GraphPatternOperation::Optional::_child, subMatcher)));
};

auto IsGroup = [](auto&& subMatcher) {
  return testing::Field(
      "variant_", &GraphPatternOperation::variant_,
      testing::VariantWith<GraphPatternOperation::GroupGraphPattern>(
          testing::Field("_child",
                         &GraphPatternOperation::GroupGraphPattern::_child,
                         subMatcher)));
};

auto IsUnion = [](auto&& subMatcher1, auto&& subMatcher2) {
  return testing::Field(
      "variant_", &GraphPatternOperation::variant_,
      testing::VariantWith<GraphPatternOperation::Union>(testing::AllOf(
          testing::Field("_child1", &GraphPatternOperation::Union::_child1,
                         subMatcher1),
          testing::Field("_child2", &GraphPatternOperation::Union::_child2,
                         subMatcher2))));
};

auto IsMinus = [](auto&& subMatcher) {
  return testing::Field(
      "variant_", &GraphPatternOperation::variant_,
      testing::VariantWith<GraphPatternOperation::Union>(
          testing::VariantWith<GraphPatternOperation::Minus>(testing::Field(
              "_child", &GraphPatternOperation::Minus::_child, subMatcher))));
};

MATCHER_P3(IsGraphPattern, optional, filters, childMatchers, "") {
  if (arg._graphPatterns.size() != std::tuple_size_v<decltype(childMatchers)>) {
    return false;
  }

  // TODO<joka921, qup42> I think there is a `tupleForEach` function somewhere
  //  in `util/xxx.h` that could be used to make this eve more idiomatic.
  auto lambda = [&](auto&&... matchers) {
    size_t i = 0;
    return (... && testing::Value(arg._graphPatterns[i++], matchers));
  };
  bool childrenMatch = std::apply(lambda, childMatchers);

  return arg._optional == optional &&
         testing::Value(arg._filters,
                        testing::UnorderedElementsAreArray(filters)) &&
         childrenMatch;
}

auto OptionalGraphPattern = [](vector<SparqlFilter>&& filters,
                               const auto&... childMatchers) {
  return IsOptional(
      IsGraphPattern(true, filters, std::tuple{childMatchers...}));
};

auto MinusGraphPattern = [](vector<SparqlFilter>&& filters,
                            const auto&... childMatchers) {
  return IsMinus(IsGraphPattern(false, filters, std::tuple{childMatchers...}));
};

auto IsSubSelect = [](auto&& selectMatcher, auto&& whereMatcher) {
  return testing::Field(
      "variant_", &GraphPatternOperation::variant_,
      testing::VariantWith<GraphPatternOperation::Subquery>(testing::Field(
          "_subquery", &GraphPatternOperation::Subquery::_subquery,
          testing::AllOf(
              testing::Property("hasSelectClause()",
                                &ParsedQuery::hasSelectClause,
                                testing::IsTrue()),
              testing::Property("selectClause()", &ParsedQuery::selectClause,
                                selectMatcher),
              testing::Field("_rootGraphPattern",
                             &ParsedQuery::_rootGraphPattern, whereMatcher)))));
};
