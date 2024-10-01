// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2022 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#pragma once

#include <gmock/gmock.h>

#include <iostream>
#include <string>
#include <vector>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/Alias.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlParserHelpers.h"
#include "parser/TripleComponent.h"
#include "parser/data/Iri.h"
#include "parser/data/OrderKey.h"
#include "parser/data/Variable.h"
#include "util/GTestHelpers.h"
#include "util/SourceLocation.h"
#include "util/TypeTraits.h"

// Not relevant for the actual test logic, but provides
// human-readable output if a test fails.
inline std::ostream& operator<<(std::ostream& out, const GraphTerm& graphTerm) {
  std::visit(
      [&]<typename T>(const T& object) {
        if constexpr (ad_utility::isSimilar<T, Literal>) {
          out << "Literal " << object.literal();
        } else if constexpr (ad_utility::isSimilar<T, BlankNode>) {
          out << "BlankNode generated: " << object.isGenerated()
              << ", label: " << object.label();
        } else if constexpr (ad_utility::isSimilar<T, Iri>) {
          out << "Iri " << object.iri();
        } else if constexpr (ad_utility::isSimilar<T, Variable>) {
          out << "Variable " << object.name();
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "unknown type");
        }
      },
      static_cast<const GraphTermBase&>(graphTerm));
  return out;
}

// _____________________________________________________________________________
namespace parsedQuery {
inline std::ostream& operator<<(std::ostream& out,
                                const parsedQuery::Bind& bind) {
  out << "Bind " << bind._expression.getDescriptor() << " as "
      << bind._target.name();
  return out;
}

inline std::ostream& operator<<(std::ostream& out,
                                const parsedQuery::Values& values) {
  out << "Values: variables "
      << ::testing::PrintToString(values._inlineValues._variables) << " values "
      << ::testing::PrintToString(values._inlineValues._values);
  return out;
}

inline void PrintTo(const parsedQuery::GraphPattern& pattern,
                    std::ostream* os) {
  auto& s = *os;
  s << ::testing::PrintToString(pattern._graphPatterns);
}

}  // namespace parsedQuery

inline void PrintTo(const Alias& alias, std::ostream* os) {
  (*os) << alias.getDescriptor();
}

inline void PrintTo(const ParsedQuery& pq, std::ostream* os) {
  (*os) << "is select query: " << pq.hasSelectClause() << '\n';
  (*os) << "Variables: " << ::testing::PrintToString(pq.getVisibleVariables())
        << '\n';
  (*os) << "Graph pattern:";
  PrintTo(pq._rootGraphPattern, os);
}

// _____________________________________________________________________________

inline std::ostream& operator<<(std::ostream& out,
                                const VariableOrderKey& orderkey) {
  out << "Order " << (orderkey.isDescending_ ? "DESC" : "ASC") << " by "
      << orderkey.variable_.name();
  return out;
}

inline std::ostream& operator<<(std::ostream& out,
                                const ExpressionOrderKey& expressionOrderKey) {
  out << "Order " << (expressionOrderKey.isDescending_ ? "DESC" : "ASC")
      << " by " << expressionOrderKey.expression_.getDescriptor();
  return out;
}

// _____________________________________________________________________________

namespace sparqlExpression {

inline std::ostream& operator<<(
    std::ostream& out,
    const sparqlExpression::SparqlExpressionPimpl& expression) {
  out << "Expression:" << expression.getDescriptor();
  return out;
}
}  // namespace sparqlExpression

// _____________________________________________________________________________

inline std::ostream& operator<<(std::ostream& out,
                                const ExceptionMetadata& metadata) {
  out << "ExceptionMetadata(\"" << metadata.query_ << "\", "
      << metadata.startIndex_ << ", " << metadata.stopIndex_ << ", "
      << metadata.line_ << ", " << metadata.charPositionInLine_ << ")";
  return out;
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
  EXPECT_EQ(resultOfParseAndText.remainingText_, rest);
}
// _____________________________________________________________________________

namespace matchers {

using testing::Matcher;
// short namespace alias
namespace p = parsedQuery;
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
inline auto NumericLiteralDouble =
    [](double value) -> Matcher<const std::variant<int64_t, double>&> {
  return testing::VariantWith<double>(testing::DoubleEq(value));
};

inline auto NumericLiteralInt =
    [](int64_t value) -> Matcher<const std::variant<int64_t, double>&> {
  return testing::VariantWith<int64_t>(testing::Eq(value));
};
// _____________________________________________________________________________
namespace variant_matcher {
// Implements a matcher that checks the value of arbitrarily deeply nested
// variants that contain a value with the last Type of the Ts. The variant may
// also be an suffix of the Ts so to speak. E.g.
// `MultiVariantMatcher<GraphTerm, GraphTerm, Iri>` can match on a `Iri`,
// `GraphTerm=std::variant<Literal, BlankNode, Iri>` and
// `GraphTerm=std::variant<Variable, GraphTerm>`.
template <typename... Ts>
class MultiVariantMatcher {
 public:
  explicit MultiVariantMatcher(Matcher<const ad_utility::Last<Ts...>&> matcher)
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

    testing::StringMatchResultListener subMatcherListener;
    const bool match = matcher_.MatchAndExplain(*elem, &subMatcherListener);
    *listener << "whose value " << testing::PrintToString(*elem)
              << (match ? " matches" : " doesn't match");
    // First add our own Explanation and then that of the sub matcher.
    *listener << subMatcherListener.str();
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

  const Matcher<const ad_utility::Last<Ts...>&> matcher_;
};

}  // namespace variant_matcher

template <typename... Ts>
testing::PolymorphicMatcher<variant_matcher::MultiVariantMatcher<Ts...>>
MultiVariantWith(const Matcher<const ad_utility::Last<Ts...>&>& matcher) {
  return testing::MakePolymorphicMatcher(
      variant_matcher::MultiVariantMatcher<Ts...>(matcher));
}

// Returns a matcher that accepts a `GraphTerm` or `Iri`.
inline auto Iri = [](const std::string& value) {
  return MultiVariantWith<GraphTerm, ::Iri>(
      AD_PROPERTY(::Iri, iri, testing::Eq(value)));
};

// Returns a matcher that accepts a `VarOrPath` or `PropertyPath`.
inline auto Predicate = [](const std::string& value) {
  return MultiVariantWith<ad_utility::sparql_types::VarOrPath, ::PropertyPath>(
      AD_PROPERTY(::PropertyPath, getIri, testing::Eq(value)));
};

// Returns a matcher that accepts a `VarOrPath` or `PropertyPath`.
inline auto PropertyPath = [](const ::PropertyPath& value) {
  return MultiVariantWith<ad_utility::sparql_types::VarOrPath, ::PropertyPath>(
      ::testing::Eq(value));
};

inline auto TripleComponentIri = [](const std::string& value) {
  return ::testing::Eq(TripleComponent::Iri::fromIriref(value));
};

// _____________________________________________________________________________

// Returns a matcher that accepts a `GraphTerm` or `BlankNode`.
inline auto BlankNode = [](bool generated, const std::string& label) {
  return MultiVariantWith<GraphTerm, ::BlankNode>(testing::AllOf(
      AD_PROPERTY(::BlankNode, isGenerated, testing::Eq(generated)),
      AD_PROPERTY(::BlankNode, label, testing::Eq(label))));
};

inline auto InternalVariable = [](const std::string& label) {
  return MultiVariantWith<GraphTerm, ::Variable>(
      testing::AllOf(AD_PROPERTY(::Variable, name,
                                 testing::StartsWith(INTERNAL_VARIABLE_PREFIX)),
                     AD_PROPERTY(::Variable, name, testing::EndsWith(label))));
};

// _____________________________________________________________________________

inline auto Variable =
    [](const std::string& value) -> Matcher<const ::Variable&> {
  return AD_PROPERTY(::Variable, name, testing::Eq(value));
};

// Returns a matcher that matches a variant that given a variant checks that it
// contains a variable and that the variable matches.
inline auto VariableVariant = [](const std::string& value) {
  return testing::VariantWith<::Variable>(Variable(value));
};

// _____________________________________________________________________________

// Returns a matcher that accepts a `GraphTerm` or `Literal`.
inline auto Literal = [](const std::string& value) {
  return MultiVariantWith<GraphTerm, ::Literal>(
      AD_PROPERTY(::Literal, literal, testing::Eq(value)));
};

// _____________________________________________________________________________

inline auto ConstructClause =
    [](const std::vector<std::array<GraphTerm, 3>>& elems)
    -> Matcher<const std::optional<parsedQuery::ConstructClause>&> {
  return testing::Optional(
      AD_FIELD(parsedQuery::ConstructClause, triples_, testing::Eq(elems)));
};

// _____________________________________________________________________________
namespace detail {
inline auto Expression = [](const std::string& descriptor)
    -> Matcher<const sparqlExpression::SparqlExpressionPimpl&> {
  return AD_PROPERTY(sparqlExpression::SparqlExpressionPimpl, getDescriptor,
                     testing::Eq(descriptor));
};
}  // namespace detail

// A matcher that tests whether a `SparqlExpression::Ptr` (a `unique_ptr`)
// actually (via dynamic cast) stores an element of type `ExpressionT`.
// `ExpressionT` must be a subclass of `SparqlExpression`.
template <typename ExpressionT>
inline auto ExpressionWithType =
    []() -> Matcher<const sparqlExpression::SparqlExpression::Ptr&> {
  return testing::Pointer(
      testing::WhenDynamicCastTo<const ExpressionT*>(testing::NotNull()));
};

namespace detail {
template <typename T>
auto GraphPatternOperation =
    [](auto subMatcher) -> Matcher<const p::GraphPatternOperation&> {
  return testing::VariantWith<T>(subMatcher);
};
}  // namespace detail

inline auto BindExpression =
    [](const string& expression) -> Matcher<const p::Bind&> {
  return AD_FIELD(p::Bind, _expression, detail::Expression(expression));
};

inline auto Bind =
    [](const ::Variable& variable,
       const string& expression) -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::Bind>(
      testing::AllOf(BindExpression(expression),
                     AD_FIELD(p::Bind, _target, testing::Eq(variable))));
};

inline auto LimitOffset =
    [](std::optional<uint64_t> limit, std::optional<uint64_t> textLimit,
       uint64_t offset) -> Matcher<const LimitOffsetClause&> {
  return testing::AllOf(
      AD_FIELD(LimitOffsetClause, _limit, testing::Eq(limit)),
      AD_FIELD(LimitOffsetClause, textLimit_, testing::Eq(textLimit)),
      AD_FIELD(LimitOffsetClause, _offset, testing::Eq(offset)));
};

inline auto VariableOrderKey =
    [](const ::Variable& variable,
       bool desc) -> Matcher<const ::VariableOrderKey&> {
  return testing::AllOf(
      AD_FIELD(::VariableOrderKey, variable_, testing::Eq(variable)),
      AD_FIELD(::VariableOrderKey, isDescending_, testing::Eq(desc)));
};

inline auto VariableOrderKeyVariant =
    [](const ::Variable& key, bool desc) -> Matcher<const OrderKey&> {
  return testing::VariantWith<::VariableOrderKey>(VariableOrderKey(key, desc));
};

inline auto VariableOrderKeys =
    [](const std::vector<std::pair<::Variable, bool>>& orderKeys)
    -> Matcher<const std::vector<::VariableOrderKey>&> {
  vector<Matcher<const ::VariableOrderKey&>> matchers;
  for (auto [key, desc] : orderKeys) {
    matchers.push_back(VariableOrderKey(key, desc));
  }
  return testing::ElementsAreArray(matchers);
};

inline auto ExpressionOrderKey = [](const string& expr,
                                    bool desc) -> Matcher<const OrderKey&> {
  return testing::VariantWith<::ExpressionOrderKey>(testing::AllOf(
      AD_FIELD(::ExpressionOrderKey, expression_, detail::Expression(expr)),
      AD_FIELD(::ExpressionOrderKey, isDescending_, testing::Eq(desc))));
};

using ExpressionOrderKeyTest = std::pair<std::string, bool>;
inline auto OrderKeys =
    [](const std::vector<
           std::variant<::VariableOrderKey, ExpressionOrderKeyTest>>& orderKeys,
       IsInternalSort isInternalSort =
           IsInternalSort::False) -> Matcher<const OrderClause&> {
  vector<Matcher<const OrderKey&>> keyMatchers;
  auto variableOrderKey =
      [](const ::VariableOrderKey& key) -> Matcher<const OrderKey&> {
    return VariableOrderKeyVariant(key.variable_, key.isDescending_);
  };
  auto expressionOrderKey =
      [](const std::pair<std::string, bool>& key) -> Matcher<const OrderKey&> {
    return ExpressionOrderKey(key.first, key.second);
  };
  for (auto& key : orderKeys) {
    keyMatchers.push_back(std::visit(
        ad_utility::OverloadCallOperator{variableOrderKey, expressionOrderKey},
        key));
  }
  return testing::AllOf(
      AD_FIELD(OrderClause, orderKeys, testing::ElementsAreArray(keyMatchers)),
      AD_FIELD(OrderClause, isInternalSort, testing::Eq(isInternalSort)));
};

inline auto VariableGroupKey =
    [](const string& key) -> Matcher<const GroupKey&> {
  return testing::VariantWith<::Variable>(
      AD_PROPERTY(Variable, name, testing::Eq(key)));
};

inline auto ExpressionGroupKey =
    [](const string& expr) -> Matcher<const GroupKey&> {
  return testing::VariantWith<sparqlExpression::SparqlExpressionPimpl>(
      detail::Expression(expr));
};

inline auto AliasGroupKey =
    [](const string& expr,
       const ::Variable& variable) -> Matcher<const GroupKey&> {
  return testing::VariantWith<Alias>(
      testing::AllOf(AD_FIELD(Alias, _target, testing::Eq(variable)),
                     AD_FIELD(Alias, _expression, detail::Expression(expr))));
};

inline auto GroupKeys =
    [](const std::vector<std::variant<
           std::string, std::pair<std::string, ::Variable>, ::Variable>>&
           groupKeys) -> Matcher<const vector<GroupKey>&> {
  vector<Matcher<const GroupKey&>> keyMatchers;
  auto variableGroupKey =
      [](const ::Variable& key) -> Matcher<const GroupKey&> {
    return VariableGroupKey(key.name());
  };
  auto expressionGroupKey =
      [](const std::string& key) -> Matcher<const GroupKey&> {
    return ExpressionGroupKey(key);
  };
  auto aliasGroupKey = [](const std::pair<std::string, ::Variable>& alias)
      -> Matcher<const GroupKey&> {
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

inline auto GroupByVariables =
    [](const vector<::Variable>& vars) -> Matcher<const ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _groupByVariables,
                  testing::UnorderedElementsAreArray(vars));
};

inline auto Values = [](const std::vector<::Variable>& vars,
                        const std::vector<vector<TripleComponent>>& values)
    -> Matcher<const p::Values&> {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  using SparqlValues = p::SparqlValues;
  return testing::AllOf(AD_FIELD(
      p::Values, _inlineValues,
      testing::AllOf(AD_FIELD(SparqlValues, _variables, testing::Eq(vars)),
                     AD_FIELD(SparqlValues, _values, testing::Eq(values)))));
};

inline auto InlineData = [](const std::vector<::Variable>& vars,
                            const vector<vector<TripleComponent>>& values)
    -> Matcher<const p::GraphPatternOperation&> {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  return detail::GraphPatternOperation<p::Values>(Values(vars, values));
};

inline auto Service =
    [](const TripleComponent::Iri& iri,
       const std::vector<::Variable>& variables,
       const std::string& graphPattern, const std::string& prologue = "",
       bool silent = false) -> Matcher<const p::GraphPatternOperation&> {
  auto serviceMatcher = testing::AllOf(
      AD_FIELD(p::Service, serviceIri_, testing::Eq(iri)),
      AD_FIELD(p::Service, visibleVariables_,
               testing::UnorderedElementsAreArray(variables)),
      AD_FIELD(p::Service, graphPatternAsString_, testing::Eq(graphPattern)),
      AD_FIELD(p::Service, prologue_, testing::Eq(prologue)),
      AD_FIELD(p::Service, silent_, testing::Eq(silent)));
  return detail::GraphPatternOperation<p::Service>(serviceMatcher);
};

namespace detail {
inline auto SelectBase =
    [](bool distinct,
       bool reduced) -> Matcher<const ParsedQuery::SelectClause&> {
  return testing::AllOf(
      AD_FIELD(p::SelectClause, distinct_, testing::Eq(distinct)),
      AD_FIELD(p::SelectClause, reduced_, testing::Eq(reduced)),
      AD_PROPERTY(p::SelectClause, getAliases, testing::IsEmpty()));
};
}  // namespace detail

inline auto AsteriskSelect = [](bool distinct = false,
                                bool reduced =
                                    false) -> Matcher<const p::SelectClause&> {
  return testing::AllOf(
      detail::SelectBase(distinct, reduced),
      AD_PROPERTY(p::SelectClause, isAsterisk, testing::IsTrue()));
};

inline auto VariablesSelect =
    [](const vector<string>& variables, bool distinct = false,
       bool reduced = false) -> Matcher<const p::SelectClause&> {
  return testing::AllOf(
      detail::SelectBase(distinct, reduced),
      AD_PROPERTY(p::SelectClause, getSelectedVariablesAsStrings,
                  testing::Eq(variables)));
};

namespace detail {
// A Matcher that matches a SelectClause.
// This matcher cannot be trivially broken down into a combination of existing
// googletest matchers because of the way how the aliases are stored in the
// select clause.
MATCHER_P4(Select, distinct, reduced, selection, hiddenAliases, "") {
  const auto& selectedVariables = arg.getSelectedVariables();
  if (selection.size() != selectedVariables.size()) {
    *result_listener << "where the number of selected variables is "
                     << selectedVariables.size() << ", but " << selection.size()
                     << " were expected";
    return false;
  }
  size_t alias_counter = 0;
  for (size_t i = 0; i < selection.size(); i++) {
    if (holds_alternative<::Variable>(selection[i])) {
      if (get<::Variable>(selection[i]) != selectedVariables[i]) {
        *result_listener << "where Variable#" << i << " = "
                         << testing::PrintToString(selectedVariables[i]);
        return false;
      }
    } else {
      auto pair = get<std::pair<string, ::Variable>>(selection[i]);
      if (alias_counter >= arg.getAliases().size()) {
        *result_listener << "where selected Variables contain less Aliases ("
                         << testing::PrintToString(alias_counter)
                         << ") than provided to matcher";
        return false;
      }
      if (pair.first !=
              arg.getAliases()[alias_counter]._expression.getDescriptor() ||
          pair.second != arg.getAliases()[alias_counter++]._target ||
          pair.second != selectedVariables[i]) {
        *result_listener << "where Alias#" << i << " = "
                         << testing::PrintToString(
                                arg.getAliases()[alias_counter - 1]);
        return false;
      }
    }
  }

  size_t i = 0;
  for (const auto& [descriptor, variable] : hiddenAliases) {
    if (alias_counter >= arg.getAliases().size()) {
      *result_listener << "where selected variables contain less aliases ("
                       << testing::PrintToString(alias_counter)
                       << ") than provided to matcher";
      return false;
    }
    if (descriptor !=
            arg.getAliases()[alias_counter]._expression.getDescriptor() ||
        variable != arg.getAliases()[alias_counter]._target) {
      *result_listener << "where hidden alias#" << i << " = "
                       << testing::PrintToString(
                              arg.getAliases()[alias_counter]);
      return false;
    }
    alias_counter++;
    i++;
  }
  return testing::ExplainMatchResult(
      testing::AllOf(
          AD_FIELD(p::SelectClause, distinct_, testing::Eq(distinct)),
          AD_PROPERTY(p::SelectClause, getAliases,
                      testing::SizeIs(testing::Eq(alias_counter))),
          AD_FIELD(p::SelectClause, reduced_, testing::Eq(reduced))),
      arg, result_listener);
}
}  // namespace detail

inline auto Select =
    [](std::vector<std::variant<::Variable, std::pair<string, ::Variable>>>
           selection,
       bool distinct = false, bool reduced = false,
       std::vector<std::pair<string, ::Variable>> hiddenAliases = {})
    -> Matcher<const p::SelectClause&> {
  return testing::SafeMatcherCast<const p::SelectClause&>(detail::Select(
      distinct, reduced, std::move(selection), std::move(hiddenAliases)));
};

// Return a `Matcher` that tests whether the descriptor of the expression of a
// `SparqlFilter` matches the given `expectedDescriptor`.
inline auto stringMatchesFilter =
    [](const std::string& expectedDescriptor) -> Matcher<const SparqlFilter&> {
  auto inner = AD_PROPERTY(sparqlExpression::SparqlExpressionPimpl,
                           getDescriptor, testing::Eq(expectedDescriptor));
  return AD_FIELD(SparqlFilter, expression_, inner);
};

// Return a `Matcher` that tests whether the descriptors of the expressions of
// the given `vector` of `SparqlFilter`s  matches the given
// `expectedDescriptors`.
inline auto stringsMatchFilters =
    [](const std::vector<std::string>& expectedDescriptors)
    -> Matcher<const std::vector<SparqlFilter>&> {
  auto matchers =
      ad_utility::transform(expectedDescriptors, stringMatchesFilter);
  return testing::ElementsAreArray(matchers);
};

inline auto SolutionModifier =
    [](const std::vector<std::variant<
           std::string, std::pair<std::string, ::Variable>, ::Variable>>&
           groupKeys = {},
       const vector<std::string>& havingClauses = {},
       const std::vector<std::variant<::VariableOrderKey,
                                      ExpressionOrderKeyTest>>& orderKeys = {},
       const LimitOffsetClause& limitOffset = {})
    -> Matcher<const SolutionModifiers&> {
  return testing::AllOf(
      AD_FIELD(SolutionModifiers, groupByVariables_, GroupKeys(groupKeys)),
      AD_FIELD(SolutionModifiers, havingClauses_,
               stringsMatchFilters(havingClauses)),
      AD_FIELD(SolutionModifiers, orderBy_, OrderKeys(orderKeys)),
      AD_FIELD(SolutionModifiers, limitOffset_, testing::Eq(limitOffset)));
};

inline auto Triples = [](const vector<SparqlTriple>& triples)
    -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::BasicGraphPattern>(
      AD_FIELD(p::BasicGraphPattern, _triples,
               testing::UnorderedElementsAreArray(triples)));
};

namespace detail {
inline auto Optional =
    [](auto&& subMatcher) -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::Optional>(
      AD_FIELD(p::Optional, _child, subMatcher));
};
}  // namespace detail

inline auto Group =
    [](auto&& subMatcher,
       p::GroupGraphPattern::GraphSpec graphSpec =
           std::monostate{}) -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::GroupGraphPattern>(::testing::AllOf(
      AD_FIELD(p::GroupGraphPattern, _child, subMatcher),
      AD_FIELD(p::GroupGraphPattern, graphSpec_, ::testing::Eq(graphSpec))));
};

inline auto Union =
    [](auto&& subMatcher1,
       auto&& subMatcher2) -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::Union>(
      testing::AllOf(AD_FIELD(p::Union, _child1, subMatcher1),
                     AD_FIELD(p::Union, _child2, subMatcher2)));
};

inline auto Minus =
    [](auto&& subMatcher) -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::Minus>(
      AD_FIELD(p::Minus, _child, subMatcher));
};

inline auto RootGraphPattern = [](const Matcher<const p::GraphPattern&>& m)
    -> Matcher<const ::ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _rootGraphPattern, m);
};

template <auto SubMatcherLambda>
struct MatcherWithDefaultFilters {
  Matcher<const p::GraphPatternOperation&> operator()(
      vector<std::string>&& filters, const auto&... childMatchers) {
    return SubMatcherLambda(std::move(filters), childMatchers...);
  }

  Matcher<const p::GraphPatternOperation&> operator()(
      const auto&... childMatchers) {
    return SubMatcherLambda({}, childMatchers...);
  }
};

template <auto SubMatcherLambda>
struct MatcherWithDefaultFiltersAndOptional {
  Matcher<const ParsedQuery::GraphPattern&> operator()(
      bool optional, vector<std::string>&& filters,
      const auto&... childMatchers) {
    return SubMatcherLambda(optional, std::move(filters), childMatchers...);
  }

  Matcher<const ParsedQuery::GraphPattern&> operator()(
      const auto&... childMatchers) {
    return SubMatcherLambda(false, {}, childMatchers...);
  }
};

namespace detail {
inline auto GraphPattern =
    [](bool optional, const vector<std::string>& filters,
       auto&&... childMatchers) -> Matcher<const ParsedQuery::GraphPattern&> {
  return testing::AllOf(
      AD_FIELD(ParsedQuery::GraphPattern, _optional, testing::Eq(optional)),
      AD_FIELD(ParsedQuery::GraphPattern, _filters,
               stringsMatchFilters(filters)),
      AD_FIELD(ParsedQuery::GraphPattern, _graphPatterns,
               testing::ElementsAre(childMatchers...)));
};
}  // namespace detail

inline auto GraphPattern =
    MatcherWithDefaultFiltersAndOptional<detail::GraphPattern>{};

namespace detail {
inline auto OptionalGraphPattern = [](vector<std::string>&& filters,
                                      const auto&... childMatchers)
    -> Matcher<const p::GraphPatternOperation&> {
  return detail::Optional(
      detail::GraphPattern(true, filters, childMatchers...));
};
}  // namespace detail

inline auto OptionalGraphPattern =
    MatcherWithDefaultFilters<detail::OptionalGraphPattern>{};

namespace detail {
inline auto GroupGraphPattern = [](vector<std::string>&& filters,
                                   const auto&... childMatchers)
    -> Matcher<const p::GraphPatternOperation&> {
  return Group(detail::GraphPattern(false, filters, childMatchers...));
};

inline auto GroupGraphPatternWithGraph =
    [](vector<std::string>&& filters, p::GroupGraphPattern::GraphSpec graph,
       const auto&... childMatchers)
    -> Matcher<const p::GraphPatternOperation&> {
  return Group(detail::GraphPattern(false, filters, childMatchers...), graph);
};

}  // namespace detail

inline auto GroupGraphPattern =
    MatcherWithDefaultFilters<detail::GroupGraphPattern>{};
inline auto GroupGraphPatternWithGraph =
    MatcherWithDefaultFilters<detail::GroupGraphPatternWithGraph>{};

namespace detail {
inline auto MinusGraphPattern = [](vector<std::string>&& filters,
                                   const auto&... childMatchers)
    -> Matcher<const p::GraphPatternOperation&> {
  return Minus(detail::GraphPattern(false, filters, childMatchers...));
};
}  // namespace detail

inline auto MinusGraphPattern =
    MatcherWithDefaultFilters<detail::MinusGraphPattern>{};

inline auto SubSelect =
    [](auto&& selectMatcher,
       auto&& whereMatcher) -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::Subquery>(AD_PROPERTY(
      p::Subquery, get,
      testing::AllOf(
          AD_PROPERTY(ParsedQuery, hasSelectClause, testing::IsTrue()),
          AD_PROPERTY(ParsedQuery, selectClause, selectMatcher),
          AD_FIELD(ParsedQuery, _rootGraphPattern, whereMatcher))));
};

// Return a matcher that matches a `DatasetClause` with given
inline auto datasetClausesMatcher(
    ScanSpecificationAsTripleComponent::Graphs defaultGraphs = std::nullopt,
    ScanSpecificationAsTripleComponent::Graphs namedGraphs = std::nullopt)
    -> Matcher<const ::ParsedQuery::DatasetClauses&> {
  using DS = ParsedQuery::DatasetClauses;
  using namespace ::testing;
  return AllOf(Field(&DS::defaultGraphs_, Eq(defaultGraphs)),
               Field(&DS::namedGraphs_, Eq(namedGraphs)));
}

inline auto SelectQuery =
    [](const Matcher<const p::SelectClause&>& selectMatcher,
       const Matcher<const p::GraphPattern&>& graphPatternMatcher,
       ScanSpecificationAsTripleComponent::Graphs defaultGraphs = std::nullopt,
       ScanSpecificationAsTripleComponent::Graphs namedGraphs =
           std::nullopt) -> Matcher<const ::ParsedQuery&> {
  using namespace ::testing;
  auto datasetMatcher = datasetClausesMatcher(defaultGraphs, namedGraphs);
  return AllOf(AD_PROPERTY(ParsedQuery, hasSelectClause, testing::IsTrue()),
               AD_PROPERTY(ParsedQuery, selectClause, selectMatcher),
               AD_FIELD(ParsedQuery, datasetClauses_, datasetMatcher),
               RootGraphPattern(graphPatternMatcher));
};

namespace pq {

// This is implemented as a separated Matcher because it generates some overhead
// in the tests.
inline auto OriginalString =
    [](const std::string& originalString) -> Matcher<const ::ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _originalString, testing::Eq(originalString));
};

inline auto LimitOffset = [](const LimitOffsetClause& limitOffset = {})
    -> Matcher<const ::ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _limitOffset, testing::Eq(limitOffset));
};
inline auto Having = [](const vector<std::string>& havingClauses = {})
    -> Matcher<const ::ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _havingClauses,
                  stringsMatchFilters(havingClauses));
};
inline auto OrderKeys =
    [](const std::vector<std::pair<::Variable, bool>>& orderKeys = {})
    -> Matcher<const ::ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _orderBy, VariableOrderKeys(orderKeys));
};
inline auto GroupKeys = GroupByVariables;
}  // namespace pq

// _____________________________________________________________________________
inline auto ConstructQuery(
    const std::vector<std::array<GraphTerm, 3>>& elems,
    const Matcher<const p::GraphPattern&>& m,
    ScanSpecificationAsTripleComponent::Graphs defaultGraphs = std::nullopt,
    ScanSpecificationAsTripleComponent::Graphs namedGraphs = std::nullopt)
    -> Matcher<const ::ParsedQuery&> {
  auto datasetMatcher = datasetClausesMatcher(defaultGraphs, namedGraphs);
  return testing::AllOf(
      AD_PROPERTY(ParsedQuery, hasConstructClause, testing::IsTrue()),
      AD_PROPERTY(ParsedQuery, constructClause,
                  AD_FIELD(parsedQuery::ConstructClause, triples_,
                           testing::ElementsAreArray(elems))),
      AD_FIELD(ParsedQuery, datasetClauses_, datasetMatcher),
      RootGraphPattern(m));
}

// _____________________________________________________________________________
inline auto VisibleVariables =
    [](const std::vector<::Variable>& elems) -> Matcher<const ParsedQuery&> {
  return AD_PROPERTY(ParsedQuery, getVisibleVariables, testing::Eq(elems));
};

inline auto UpdateQuery =
    [](const std::vector<SparqlTripleSimple>& toDelete,
       const std::vector<SparqlTripleSimple>& toInsert,
       const Matcher<const p::GraphPattern&>& graphPatternMatcher)
    -> Matcher<const ::ParsedQuery&> {
  return testing::AllOf(
      AD_PROPERTY(ParsedQuery, hasUpdateClause, testing::IsTrue()),
      AD_PROPERTY(ParsedQuery, updateClause,
                  AD_FIELD(parsedQuery::UpdateClause, toDelete_,
                           testing::ElementsAreArray(toDelete))),
      AD_PROPERTY(ParsedQuery, updateClause,
                  AD_FIELD(parsedQuery::UpdateClause, toInsert_,
                           testing::ElementsAreArray(toInsert))),
      RootGraphPattern(graphPatternMatcher));
};

template <typename T>
auto inline Variant = []() { return testing::VariantWith<T>(testing::_); };

auto inline GraphRefIri = [](const string& iri) {
  return testing::VariantWith<GraphRef>(AD_PROPERTY(
      TripleComponent::Iri, toStringRepresentation, testing::Eq(iri)));
};

}  // namespace matchers
