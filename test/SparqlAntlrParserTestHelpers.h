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
#include "util/SourceLocation.h"
#include "util/TypeTraits.h"

#ifdef AD_PROPERTY
#error "AD_PROPERTY must not already be defined. Consider renaming it."
#else
#define AD_PROPERTY(Class, Member, Matcher) \
  testing::Property(#Member "()", &Class::Member, Matcher)
#endif

#ifdef AD_FIELD
#error "AD_FIELD must not already be defined. Consider renaming it."
#else
#define AD_FIELD(Class, Member, Matcher) \
  testing::Field(#Member, &Class::Member, Matcher)
#endif

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
// Adds the position of the actual caller in the gtest error messages.
[[nodiscard]] testing::ScopedTrace generateLocationTrace(
    ad_utility::source_location l) {
  return {l.file_name(), static_cast<int>(l.line()),
          "Actual location of the test failure"};
}
// _____________________________________________________________________________
/**
 * Ensures that the matcher matches on the result of the parsing and that the
 * text has been fully consumed by the parser.
 *
 * @param resultOfParseAndText Parsing result
 * @param matcher Matcher that must be fulfilled
 */
void expectCompleteParse(
    const auto& resultOfParseAndText, auto&& matcher,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l);
  EXPECT_THAT(resultOfParseAndText.resultOfParse_, matcher);
  EXPECT_TRUE(resultOfParseAndText.remainingText_.empty());
}

// _____________________________________________________________________________
/**
 * Ensures that the matcher matches on the result of the parsing and that the
 * text has not been fully consumed by the parser. rest is expected to be the
 * unconsumed input of the parser.
 *
 * @param resultOfParseAndText Parsing result
 * @param matcher Matcher that must be fulfilled
 * @param rest Input that is not consumed
 */
void expectIncompleteParse(
    const auto& resultOfParseAndText, const string& rest, auto&& matcher,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l);
  EXPECT_THAT(resultOfParseAndText.resultOfParse_, matcher);
  EXPECT_THAT(resultOfParseAndText.remainingText_, testing::Eq(rest));
}
// _____________________________________________________________________________
auto NumericLiteralFP =
    [](double value) -> testing::Matcher<std::variant<int64_t, double>> {
  return testing::VariantWith<double>(testing::DoubleEq(value));
};

auto NumericLiteralWhole =
    [](int64_t value) -> testing::Matcher<std::variant<int64_t, double>> {
  return testing::VariantWith<int64_t>(testing::Eq(value));
};
// _____________________________________________________________________________
namespace variant_matcher {
// Implements a matcher that checks the value of arbitrarily deeply nested
// variants that contain a value with the last Type of the Ts. The variant may
// also be an suffix of the Ts so to speak. E.g.
// `MultiVariantMatcher<VarOrTerm, GraphTerm, Iri>` can match on a `Iri`,
// `GraphTerm=std::variant<Literal, BlankNode, Iri>` and
// `VarOrTerm=std::variant<Variable, GraphTerm>`.
template <typename... Ts>
class MultiVariantMatcher {
 public:
  explicit MultiVariantMatcher(
      ::testing::Matcher<const ad_utility::Last<Ts...>&> matcher)
      : matcher_(std::move(matcher)) {}

  template <typename Variant>
  bool MatchAndExplain(const Variant& value,
                       ::testing::MatchResultListener* listener) const {
    auto elem = unwrapVariant<Ts...>(value);
    if (!listener->IsInterested()) {
      return elem && matcher_.Matches(*elem);
    }

    if (!elem) {
      *listener << "whose value is not of type '" << GetTypeName() << "'";
      return false;
    }

    testing::StringMatchResultListener elem_listener;
    const bool match = matcher_.MatchAndExplain(*elem, &elem_listener);
    *listener << "whose value " << testing::PrintToString(*elem)
              << (match ? " matches" : " doesn't match");
    testing::internal::PrintIfNotEmpty(elem_listener.str(), listener->stream());
    return match;
  }

  void DescribeTo(std::ostream* os) const {
    *os << "is a variant<> with value of type '" << GetTypeName()
        << "' and the value ";
    matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const {
    *os << "is a variant<> with value of type other than '" << GetTypeName()
        << "' or the value ";
    matcher_.DescribeNegationTo(os);
  }

 private:
  static std::string GetTypeName() {
    return testing::internal::GetTypeName<ad_utility::Last<Ts...>>();
  }

  const ::testing::Matcher<const ad_utility::Last<Ts...>&> matcher_;
};

}  // namespace variant_matcher

template <typename... Ts>
testing::PolymorphicMatcher<variant_matcher::MultiVariantMatcher<Ts...>>
MultiVariantWith(
    const testing::Matcher<const ad_utility::Last<Ts...>&>& matcher) {
  return testing::MakePolymorphicMatcher(
      variant_matcher::MultiVariantMatcher<Ts...>(matcher));
}

// Returns a matcher that accepts a `VarOrTerm`, `GraphTerm` or `Iri`.
auto IsIri = [](const std::string& value) {
  return MultiVariantWith<VarOrTerm, GraphTerm, Iri>(
      AD_PROPERTY(Iri, iri, testing::Eq(value)));
};

// _____________________________________________________________________________

// Returns a matcher that accepts a `VarOrTerm`, `GraphTerm` or `BlankNode`.
auto IsBlankNode = [](bool generated, const std::string& label) {
  return MultiVariantWith<VarOrTerm, GraphTerm, BlankNode>(testing::AllOf(
      AD_PROPERTY(BlankNode, isGenerated, testing::Eq(generated)),
      AD_PROPERTY(BlankNode, label, testing::Eq(label))));
};

// _____________________________________________________________________________

auto IsVariable =
    [](const std::string& value) -> testing::Matcher<const Variable&> {
  return AD_PROPERTY(Variable, name, testing::Eq(value));
};

// Returns a matcher that matches a variant that given a variant checks that it
// contains a variable and that the variable matches.
auto VariableVariant = [](const std::string& value) {
  return testing::VariantWith<Variable>(IsVariable(value));
};

// _____________________________________________________________________________

// Returns a matcher that accepts a `VarOrTerm`, `GraphTerm` or `Literal`.
auto IsLiteral = [](const std::string& value) {
  return MultiVariantWith<VarOrTerm, GraphTerm, Literal>(
      AD_PROPERTY(Literal, literal, testing::Eq(value)));
};

// _____________________________________________________________________________

namespace detail {
auto Expression = [](const std::string& descriptor)
    -> testing::Matcher<const sparqlExpression::SparqlExpressionPimpl&> {
  return AD_PROPERTY(sparqlExpression::SparqlExpressionPimpl, getDescriptor,
                     testing::Eq(descriptor));
};
}

namespace detail {
template <typename T>
auto IsGraphPatternOperation =
    [](auto subMatcher) -> testing::Matcher<const GraphPatternOperation&> {
  return AD_FIELD(GraphPatternOperation, variant_,
                  testing::VariantWith<T>(subMatcher));
};
}

auto BindExpression = [](const string& expression)
    -> testing::Matcher<const GraphPatternOperation::Bind&> {
  return AD_FIELD(GraphPatternOperation::Bind, _expression,
                  detail::Expression(expression));
};

auto Bind = [](const string& variable, const string& expression)
    -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsGraphPatternOperation<GraphPatternOperation::Bind>(
      testing::AllOf(BindExpression(expression),
                     AD_FIELD(GraphPatternOperation::Bind, _target,
                              testing::Eq(variable))));
};

auto LimitOffset =
    [](uint64_t limit, uint64_t textLimit,
       uint64_t offset) -> testing::Matcher<const LimitOffsetClause&> {
  return testing::AllOf(
      AD_FIELD(LimitOffsetClause, _limit, testing::Eq(limit)),
      AD_FIELD(LimitOffsetClause, _textLimit, testing::Eq(textLimit)),
      AD_FIELD(LimitOffsetClause, _offset, testing::Eq(offset)));
};

auto IsVariableOrderKey =
    [](const string& key,
       bool desc) -> testing::Matcher<const VariableOrderKey&> {
  return testing::AllOf(
      AD_FIELD(VariableOrderKey, variable_, testing::Eq(key)),
      AD_FIELD(VariableOrderKey, isDescending_, testing::Eq(desc)));
};

auto VariableOrderKeyVariant =
    [](const string& key, bool desc) -> testing::Matcher<const OrderKey&> {
  return testing::VariantWith<VariableOrderKey>(VariableOrderKey(key, desc));
};

auto IsExpressionOrderKey = [](const string& expr,
                               bool desc) -> testing::Matcher<const OrderKey&> {
  return testing::VariantWith<ExpressionOrderKey>(testing::AllOf(
      AD_FIELD(ExpressionOrderKey, expression_, detail::Expression(expr)),
      AD_FIELD(ExpressionOrderKey, isDescending_, testing::Eq(desc))));
};

using ExpressionOrderKeyTest = std::pair<std::string, bool>;
auto OrderKeys =
    [](const std::vector<
        std::variant<VariableOrderKey, ExpressionOrderKeyTest>>& orderKeys)
    -> testing::Matcher<const vector<OrderKey>&> {
  vector<testing::Matcher<const OrderKey&>> keyMatchers;
  auto variableOrderKey =
      [](const VariableOrderKey& key) -> testing::Matcher<const OrderKey&> {
    return VariableOrderKeyVariant(key.variable_, key.isDescending_);
  };
  auto expressionOrderKey = [](const std::pair<std::string, bool>& key)
      -> testing::Matcher<const OrderKey&> {
    return IsExpressionOrderKey(key.first, key.second);
  };
  for (auto& key : orderKeys) {
    keyMatchers.push_back(std::visit(
        ad_utility::OverloadCallOperator{variableOrderKey, expressionOrderKey},
        key));
  }
  return testing::ElementsAreArray(keyMatchers);
};

auto VariableGroupKey =
    [](const string& key) -> testing::Matcher<const GroupKey&> {
  return testing::VariantWith<Variable>(
      AD_PROPERTY(Variable, name, testing::Eq(key)));
};

auto ExpressionGroupKey =
    [](const string& expr) -> testing::Matcher<const GroupKey&> {
  return testing::VariantWith<sparqlExpression::SparqlExpressionPimpl>(
      detail::Expression(expr));
};

auto AliasGroupKey =
    [](const string& expr,
       const string& variable) -> testing::Matcher<const GroupKey&> {
  return testing::VariantWith<Alias>(
      testing::AllOf(AD_FIELD(Alias, _outVarName, testing::Eq(variable)),
                     AD_FIELD(Alias, _expression, detail::Expression(expr))));
};

auto GroupKeys =
    [](const std::vector<std::variant<
           std::string, std::pair<std::string, std::string>, Variable>>&
           groupKeys) -> testing::Matcher<const vector<GroupKey>&> {
  vector<testing::Matcher<const GroupKey&>> keyMatchers;
  auto variableGroupKey =
      [](const Variable& key) -> testing::Matcher<const GroupKey&> {
    return VariableGroupKey(key.name());
  };
  auto expressionGroupKey =
      [](const std::string& key) -> testing::Matcher<const GroupKey&> {
    return ExpressionGroupKey(key);
  };
  auto aliasGroupKey = [](const std::pair<std::string, std::string>& alias)
      -> testing::Matcher<const GroupKey&> {
    return AliasGroupKey(alias.first, alias.second);
  };
  for (auto& key : groupKeys) {
    keyMatchers.push_back(std::visit(
        ad_utility::OverloadCallOperator{variableGroupKey, expressionGroupKey,
                                         aliasGroupKey},
        key));
  }
  return testing::ElementsAreArray(keyMatchers);
};

auto GroupByVariablesMatch =
    [](const vector<Variable>& vars) -> testing::Matcher<const ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _groupByVariables,
                  testing::UnorderedElementsAreArray(vars));
};

auto Values = [](const vector<string>& vars,
                 const vector<vector<string>>& values)
    -> testing::Matcher<const GraphPatternOperation::Values&> {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  return testing::AllOf(AD_FIELD(
      GraphPatternOperation::Values, _inlineValues,
      testing::AllOf(AD_FIELD(SparqlValues, _variables, testing::Eq(vars)),
                     AD_FIELD(SparqlValues, _values, testing::Eq(values)))));
};

auto InlineData = [](const vector<string>& vars,
                     const vector<vector<string>>& values)
    -> testing::Matcher<const GraphPatternOperation&> {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  return detail::IsGraphPatternOperation<GraphPatternOperation::Values>(
      Values(vars, values));
};

namespace detail {
auto IsSelectBase =
    [](bool distinct,
       bool reduced) -> testing::Matcher<const ParsedQuery::SelectClause&> {
  return testing::AllOf(
      AD_FIELD(ParsedQuery::SelectClause, _distinct, testing::Eq(distinct)),
      AD_FIELD(ParsedQuery::SelectClause, _reduced, testing::Eq(reduced)),
      AD_PROPERTY(ParsedQuery, SelectClause::getAliases, testing::IsEmpty()));
};
}

auto AsteriskSelect = [](bool distinct = false, bool reduced = false)
    -> testing::Matcher<const ParsedQuery::SelectClause&> {
  return testing::AllOf(
      detail::IsSelectBase(distinct, reduced),
      AD_PROPERTY(ParsedQuery, SelectClause::isAsterisk, testing::IsTrue()));
};

auto VariablesSelect =
    [](const vector<string>& variables, bool distinct = false,
       bool reduced =
           false) -> testing::Matcher<const ParsedQuery::SelectClause&> {
  return testing::AllOf(
      detail::IsSelectBase(distinct, reduced),
      AD_PROPERTY(ParsedQuery::SelectClause, getSelectedVariablesAsStrings,
                  testing::Eq(variables)));
};

// A Matcher that matches a SelectClause.
// This matcher cannot be trivially broken down into a combination of existing
// googletest matchers because of the way how the aliases are stored in the
// select clause.
MATCHER_P3(Select, distinct, reduced, selection, "") {
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
          AD_FIELD(ParsedQuery::SelectClause, _distinct, testing::Eq(distinct)),
          AD_PROPERTY(ParsedQuery, SelectClause::getAliases,
                      testing::SizeIs(testing::Eq(alias_counter))),
          AD_FIELD(ParsedQuery::SelectClause, _reduced, testing::Eq(reduced))),
      arg, result_listener);
}

auto SolutionModifier =
    [](const std::vector<std::variant<
           std::string, std::pair<std::string, std::string>, Variable>>&
           groupKeys = {},
       const vector<SparqlFilter>& havingClauses = {},
       const std::vector<std::variant<VariableOrderKey,
                                      ExpressionOrderKeyTest>>& orderKeys = {},
       const LimitOffsetClause& limitOffset = {})
    -> testing::Matcher<const SolutionModifiers&> {
  return testing::AllOf(
      AD_FIELD(SolutionModifiers, groupByVariables_, GroupKeys(groupKeys)),
      AD_FIELD(SolutionModifiers, havingClauses_, testing::Eq(havingClauses)),
      AD_FIELD(SolutionModifiers, orderBy_, OrderKeys(orderKeys)),
      AD_FIELD(SolutionModifiers, limitOffset_, testing::Eq(limitOffset)));
};

auto Triples = [](const vector<SparqlTriple>& triples)
    -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsGraphPatternOperation<
      GraphPatternOperation::BasicGraphPattern>(
      AD_FIELD(GraphPatternOperation::BasicGraphPattern, _triples,
               testing::UnorderedElementsAreArray(triples)));
};

namespace detail {
auto IsOptional =
    [](auto&& subMatcher) -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsGraphPatternOperation<GraphPatternOperation::Optional>(
      AD_FIELD(GraphPatternOperation::Optional, _child, subMatcher));
};
}

auto Group =
    [](auto&& subMatcher) -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsGraphPatternOperation<
      GraphPatternOperation::GroupGraphPattern>(
      AD_FIELD(GraphPatternOperation::GroupGraphPattern, _child, subMatcher));
};

auto Union =
    [](auto&& subMatcher1,
       auto&& subMatcher2) -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsGraphPatternOperation<GraphPatternOperation::Union>(
      testing::AllOf(
          AD_FIELD(GraphPatternOperation::Union, _child1, subMatcher1),
          AD_FIELD(GraphPatternOperation::Union, _child2, subMatcher2)));
};

auto Minus =
    [](auto&& subMatcher) -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsGraphPatternOperation<GraphPatternOperation::Minus>(
      AD_FIELD(GraphPatternOperation::Minus, _child, subMatcher));
};

// TODO: generalize or rename
template <auto SubMatcherLambda>
struct MatcherOverloads {
  testing::Matcher<const GraphPatternOperation&> operator()(
      vector<SparqlFilter>&& filters, const auto&... childMatchers) {
    return SubMatcherLambda(std::move(filters), childMatchers...);
  }

  testing::Matcher<const GraphPatternOperation&> operator()(
      const auto&... childMatchers) {
    return SubMatcherLambda({}, childMatchers...);
  }
};

template <auto SubMatcherLambda>
struct MatcherOverloadss {
  testing::Matcher<const ParsedQuery::GraphPattern&> operator()(
      bool optional, vector<SparqlFilter>&& filters,
      const auto&... childMatchers) {
    return SubMatcherLambda(optional, std::move(filters), childMatchers...);
  }

  testing::Matcher<const ParsedQuery::GraphPattern&> operator()(
      const auto&... childMatchers) {
    return SubMatcherLambda(false, {}, childMatchers...);
  }
};

namespace detail {
auto IsGraphPattern = [](bool optional, const vector<SparqlFilter>& filters,
                         auto&&... childMatchers)
    -> testing::Matcher<const ParsedQuery::GraphPattern&> {
  return testing::AllOf(
      AD_FIELD(ParsedQuery::GraphPattern, _optional, testing::Eq(optional)),
      AD_FIELD(ParsedQuery::GraphPattern, _filters,
               testing::UnorderedElementsAreArray(filters)),
      AD_FIELD(ParsedQuery::GraphPattern, _graphPatterns,
               testing::ElementsAre(childMatchers...)));
};
}

auto GraphPattern = MatcherOverloadss<detail::IsGraphPattern>{};

namespace detail {
auto IsOptionalGraphPattern = [](vector<SparqlFilter>&& filters,
                                 const auto&... childMatchers)
    -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsOptional(
      detail::IsGraphPattern(true, filters, childMatchers...));
};
}

auto OptionalGraphPattern = MatcherOverloads<detail::IsOptionalGraphPattern>{};

namespace detail {
auto IsGroupGraphPattern = [](vector<SparqlFilter>&& filters,
                              const auto&... childMatchers)
    -> testing::Matcher<const GraphPatternOperation&> {
  return Group(detail::IsGraphPattern(false, filters, childMatchers...));
};
}

auto GroupGraphPattern = MatcherOverloads<detail::IsGroupGraphPattern>{};

namespace detail {
auto IsMinusGraphPattern = [](vector<SparqlFilter>&& filters,
                              const auto&... childMatchers)
    -> testing::Matcher<const GraphPatternOperation&> {
  return Minus(detail::IsGraphPattern(false, filters, childMatchers...));
};
}

auto MinusGraphPattern = MatcherOverloads<detail::IsMinusGraphPattern>{};

auto SubSelect =
    [](auto&& selectMatcher,
       auto&& whereMatcher) -> testing::Matcher<const GraphPatternOperation&> {
  return detail::IsGraphPatternOperation<GraphPatternOperation::Subquery>(
      AD_FIELD(GraphPatternOperation::Subquery, _subquery,
               testing::AllOf(
                   AD_PROPERTY(ParsedQuery, hasSelectClause, testing::IsTrue()),
                   AD_PROPERTY(ParsedQuery, selectClause, selectMatcher),
                   AD_FIELD(ParsedQuery, _rootGraphPattern, whereMatcher))));
};

#undef AD_PROPERTY
#undef AD_FIELD
