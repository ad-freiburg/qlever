// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2022 Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//   2022 Julian Mundhahs (mundhahj@tf.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_TEST_SPARQLANTLRPARSERTESTHELPERS_H
#define QLEVER_TEST_SPARQLANTLRPARSERTESTHELPERS_H

#include <gmock/gmock.h>

#include <iostream>
#include <string>
#include <typeindex>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/Alias.h"
#include "parser/DatasetClauses.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlParserHelpers.h"
#include "parser/TripleComponent.h"
#include "parser/data/Iri.h"
#include "parser/data/OrderKey.h"
#include "rdfTypes/Variable.h"
#include "util/SourceLocation.h"
#include "util/TypeTraits.h"

// Not relevant for the actual test logic, but provides
// human-readable output if a test fails.
inline std::ostream& operator<<(std::ostream& out, const GraphTerm& graphTerm) {
  std::visit(
      [&](const auto& object) {
        using T = std::decay_t<decltype(object)>;
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
template <typename Result, typename Matcher>
void expectCompleteParse(
    const Result& resultOfParseAndText, Matcher&& matcher,
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
template <typename Result, typename Matcher>
void expectIncompleteParse(
    const Result& resultOfParseAndText, const std::string& rest,
    Matcher&& matcher,
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

template <typename Current, typename... Others, typename T>
constexpr const ad_utility::Last<Current, Others...>* unwrapVariant(
    const T& arg) {
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
inline auto Predicate = [](const ad_utility::triple_component::Iri& value) {
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
  return MultiVariantWith<GraphTerm, ::Variable>(testing::AllOf(
      AD_PROPERTY(::Variable, name,
                  testing::StartsWith(QLEVER_INTERNAL_VARIABLE_PREFIX)),
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
    [](const std::string& expression) -> Matcher<const p::Bind&> {
  return AD_FIELD(p::Bind, _expression, detail::Expression(expression));
};

inline auto Bind = [](const ::Variable& variable, const std::string& expression)
    -> Matcher<const p::GraphPatternOperation&> {
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
  std::vector<Matcher<const ::VariableOrderKey&>> matchers;
  for (auto [key, desc] : orderKeys) {
    matchers.push_back(VariableOrderKey(key, desc));
  }
  return testing::ElementsAreArray(matchers);
};

inline auto ExpressionOrderKey = [](const std::string& expr,
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
  std::vector<Matcher<const OrderKey&>> keyMatchers;
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
    [](const std::string& key) -> Matcher<const GroupKey&> {
  return testing::VariantWith<::Variable>(
      AD_PROPERTY(Variable, name, testing::Eq(key)));
};

inline auto ExpressionGroupKey =
    [](const std::string& expr) -> Matcher<const GroupKey&> {
  return testing::VariantWith<sparqlExpression::SparqlExpressionPimpl>(
      detail::Expression(expr));
};

inline auto AliasGroupKey =
    [](const std::string& expr,
       const ::Variable& variable) -> Matcher<const GroupKey&> {
  return testing::VariantWith<Alias>(
      testing::AllOf(AD_FIELD(Alias, _target, testing::Eq(variable)),
                     AD_FIELD(Alias, _expression, detail::Expression(expr))));
};

inline auto GroupKeys =
    [](const std::vector<std::variant<
           std::string, std::pair<std::string, ::Variable>, ::Variable>>&
           groupKeys) -> Matcher<const std::vector<GroupKey>&> {
  std::vector<Matcher<const GroupKey&>> keyMatchers;
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
    [](const std::vector<::Variable>& vars) -> Matcher<const ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _groupByVariables,
                  testing::UnorderedElementsAreArray(vars));
};

// Test that a `ParsedQuery` contains the `warnings` in any order. The
// `warnings` can be substrings of the full warning messages.
inline auto WarningsOfParsedQuery = [](const std::vector<std::string>& warnings)
    -> Matcher<const ParsedQuery&> {
  auto matchers = ad_utility::transform(
      warnings, [](const std::string& s) { return ::testing::HasSubstr(s); });
  return AD_PROPERTY(ParsedQuery, warnings,
                     testing::UnorderedElementsAreArray(matchers));
};

inline auto Values = [](const std::vector<::Variable>& vars,
                        const std::vector<std::vector<TripleComponent>>& values)
    -> Matcher<const p::Values&> {
  // TODO Refactor GraphPatternOperation::Values / SparqlValues s.t. this
  //  becomes a trivial Eq matcher.
  using SparqlValues = p::SparqlValues;
  return testing::AllOf(AD_FIELD(
      p::Values, _inlineValues,
      testing::AllOf(AD_FIELD(SparqlValues, _variables, testing::Eq(vars)),
                     AD_FIELD(SparqlValues, _values, testing::Eq(values)))));
};

inline auto InlineData =
    [](const std::vector<::Variable>& vars,
       const std::vector<std::vector<TripleComponent>>& values)
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
    [](const std::vector<std::string>& variables, bool distinct = false,
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
      auto pair = get<std::pair<std::string, ::Variable>>(selection[i]);
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
    [](std::vector<std::variant<::Variable, std::pair<std::string, ::Variable>>>
           selection,
       bool distinct = false, bool reduced = false,
       std::vector<std::pair<std::string, ::Variable>> hiddenAliases = {})
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
       const std::vector<std::string>& havingClauses = {},
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

inline auto Triples = [](const std::vector<SparqlTriple>& triples)
    -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::BasicGraphPattern>(
      AD_FIELD(p::BasicGraphPattern, _triples,
               testing::UnorderedElementsAreArray(triples)));
};

// Same as above, but the triples have to be in the same order as specified.
// In particular, this also makes the GTest output more readable in case of a
// test failure.
inline auto OrderedTriples = [](const std::vector<SparqlTriple>& triples)
    -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::BasicGraphPattern>(AD_FIELD(
      p::BasicGraphPattern, _triples, testing::ElementsAreArray(triples)));
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

inline auto Load = [](const TripleComponent::Iri& iri,
                      bool silent) -> Matcher<const p::GraphPatternOperation&> {
  return detail::GraphPatternOperation<p::Load>(
      testing::AllOf(AD_FIELD(p::Load, iri_, testing::Eq(iri)),
                     AD_FIELD(p::Load, silent_, testing::Eq(silent))));
};

inline auto RootGraphPattern = [](const Matcher<const p::GraphPattern&>& m)
    -> Matcher<const ::ParsedQuery&> {
  return AD_FIELD(ParsedQuery, _rootGraphPattern, m);
};

template <auto SubMatcherLambda>
struct MatcherWithDefaultFilters {
  Matcher<const p::GraphPatternOperation&> operator()(
      std::vector<std::string>&& filters, const auto&... childMatchers) {
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
      bool optional, std::vector<std::string>&& filters,
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
    [](bool optional, const std::vector<std::string>& filters,
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
inline auto OptionalGraphPattern = [](std::vector<std::string>&& filters,
                                      const auto&... childMatchers)
    -> Matcher<const p::GraphPatternOperation&> {
  return detail::Optional(
      detail::GraphPattern(true, filters, childMatchers...));
};
}  // namespace detail

inline auto OptionalGraphPattern =
    MatcherWithDefaultFilters<detail::OptionalGraphPattern>{};

namespace detail {
inline auto GroupGraphPattern = [](std::vector<std::string>&& filters,
                                   const auto&... childMatchers)
    -> Matcher<const p::GraphPatternOperation&> {
  return Group(detail::GraphPattern(false, filters, childMatchers...));
};

inline auto GroupGraphPatternWithGraph =
    [](std::vector<std::string>&& filters,
       p::GroupGraphPattern::GraphSpec graph, const auto&... childMatchers)
    -> Matcher<const p::GraphPatternOperation&> {
  return Group(detail::GraphPattern(false, filters, childMatchers...), graph);
};

}  // namespace detail

inline auto GroupGraphPattern =
    MatcherWithDefaultFilters<detail::GroupGraphPattern>{};
inline auto GroupGraphPatternWithGraph =
    MatcherWithDefaultFilters<detail::GroupGraphPatternWithGraph>{};

namespace detail {
inline auto MinusGraphPattern = [](std::vector<std::string>&& filters,
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
    ScanSpecificationAsTripleComponent::Graphs activeDefaultGraphs =
        std::nullopt,
    ScanSpecificationAsTripleComponent::Graphs namedGraphs = std::nullopt)
    -> Matcher<const ::ParsedQuery::DatasetClauses&> {
  using DS = ParsedQuery::DatasetClauses;
  using namespace ::testing;
  return AllOf(AD_PROPERTY(DS, activeDefaultGraphs, Eq(activeDefaultGraphs)),
               AD_PROPERTY(DS, namedGraphs, Eq(namedGraphs)));
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
inline auto Having = [](const std::vector<std::string>& havingClauses = {})
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

// Matcher for an ASK query.
inline auto AskQuery =
    [](const Matcher<const p::GraphPattern&>& graphPatternMatcher,
       ScanSpecificationAsTripleComponent::Graphs defaultGraphs = std::nullopt,
       ScanSpecificationAsTripleComponent::Graphs namedGraphs =
           std::nullopt) -> Matcher<const ::ParsedQuery&> {
  using namespace ::testing;
  auto datasetMatcher = datasetClausesMatcher(defaultGraphs, namedGraphs);
  return AllOf(AD_PROPERTY(ParsedQuery, hasAskClause, testing::IsTrue()),
               AD_FIELD(ParsedQuery, datasetClauses_, datasetMatcher),
               pq::LimitOffset(LimitOffsetClause{1u, 0, std::nullopt}),
               RootGraphPattern(graphPatternMatcher));
};

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

// A matcher for a `DescribeQuery`
inline auto DescribeQuery(
    const Matcher<const p::GraphPattern&>& describeMatcher,
    const ScanSpecificationAsTripleComponent::Graphs& defaultGraphs =
        std::nullopt,
    const ScanSpecificationAsTripleComponent::Graphs& namedGraphs =
        std::nullopt) {
  using Var = ::Variable;
  return ConstructQuery({{Var{"?subject"}, Var{"?predicate"}, Var{"?object"}}},
                        describeMatcher, defaultGraphs, namedGraphs);
}

// Match a `Describe` clause.
inline auto Describe = [](const std::vector<p::Describe::VarOrIri>& resources,
                          const p::DatasetClauses& datasetClauses,
                          const Matcher<const ParsedQuery&>& subquery)
    -> Matcher<const p::GraphPattern&> {
  using namespace ::testing;
  auto getSubquery = [](const p::Subquery& subquery) -> const ParsedQuery& {
    return subquery.get();
  };
  return GraphPattern(detail::GraphPatternOperation<p::Describe>(AllOf(
      AD_FIELD(p::Describe, resources_, Eq(resources)),
      AD_FIELD(p::Describe, datasetClauses_, Eq(datasetClauses)),
      AD_FIELD(p::Describe, whereClause_, ResultOf(getSubquery, subquery)))));
};

// _____________________________________________________________________________
inline auto VisibleVariables =
    [](const std::vector<::Variable>& elems) -> Matcher<const ParsedQuery&> {
  return AD_PROPERTY(ParsedQuery, getVisibleVariables, testing::Eq(elems));
};

using namespace updateClause;

// Match a `updateClause::GraphUpdate` clause.
inline auto MatchGraphUpdate(
    const Matcher<const updateClause::GraphUpdate::Triples&>& toDelete,
    const Matcher<const updateClause::GraphUpdate::Triples&>& toInsert)
    -> Matcher<const updateClause::GraphUpdate&> {
  using namespace testing;
  return AllOf(AD_FIELD(GraphUpdate, toInsert_, toInsert),
               AD_FIELD(GraphUpdate, toDelete_, toDelete));
}

// Same as above, but only match the triples exactly (without checking for blank
// nodes, local vocab, etc.)
inline auto GraphUpdate(
    const std::vector<SparqlTripleSimpleWithGraph>& toDelete,
    const std::vector<SparqlTripleSimpleWithGraph>& toInsert)
    -> Matcher<const updateClause::GraphUpdate&> {
  auto getVec = [](const GraphUpdate::Triples& tr) -> decltype(auto) {
    return tr.triples_;
  };
  using namespace testing;
  return matchers::MatchGraphUpdate(
      ResultOf(getVec, ElementsAreArray(toDelete)),
      ResultOf(getVec, ElementsAreArray(toInsert)));
}

inline auto EmptyDatasets = [] {
  return AllOf(AD_PROPERTY(ParsedQuery::DatasetClauses, activeDefaultGraphs,
                           testing::Eq(std::nullopt)),
               AD_PROPERTY(ParsedQuery::DatasetClauses, namedGraphs,
                           testing::Eq(std::nullopt)));
};

using Graphs = ad_utility::HashSet<TripleComponent>;

inline auto UpdateClause =
    [](const Matcher<const updateClause::GraphUpdate&>& opMatcher,
       const Matcher<const p::GraphPattern&>& graphPatternMatcher,
       const Matcher<const ::ParsedQuery::DatasetClauses&>& datasetMatcher =
           EmptyDatasets()) -> Matcher<const ::ParsedQuery&> {
  return testing::AllOf(
      AD_PROPERTY(ParsedQuery, hasUpdateClause, testing::IsTrue()),
      AD_PROPERTY(ParsedQuery, updateClause,
                  AD_FIELD(parsedQuery::UpdateClause, op_, opMatcher)),
      AD_FIELD(ParsedQuery, datasetClauses_, datasetMatcher),
      RootGraphPattern(graphPatternMatcher));
};

template <typename T>
auto inline Variant = []() { return testing::VariantWith<T>(testing::_); };

auto inline GraphRefIri = [](const std::string& iri) {
  return testing::VariantWith<GraphRef>(AD_PROPERTY(
      TripleComponent::Iri, toStringRepresentation, testing::Eq(iri)));
};

inline auto Quads = [](const ad_utility::sparql_types::Triples& freeTriples,
                       const std::vector<Quads::GraphBlock>& graphs)
    -> Matcher<const ::Quads&> {
  return AllOf(
      AD_FIELD(Quads, freeTriples_, testing::ElementsAreArray(freeTriples)),
      AD_FIELD(Quads, graphTriples_, testing::ElementsAreArray(graphs)));
};

// Some helper matchers for testing SparqlExpressions
namespace builtInCall {
using namespace sparqlExpression;

// Return a matcher that checks whether a given `SparqlExpression::Ptr` actually
// (via `dynamic_cast`) points to an object of type `Expression`, and that this
// `Expression` matches the `matcher`.
template <typename Expression, typename Matcher = decltype(testing::_)>
auto matchPtr(Matcher matcher = Matcher{})
    -> testing::Matcher<const SparqlExpression::Ptr&> {
  return testing::Pointee(
      testing::WhenDynamicCastTo<const Expression&>(matcher));
}

// Return a matcher that matches a `SparqlExpression::Ptr` that stores a
// `VariableExpression` with the given  `variable`.
inline auto variableExpressionMatcher = [](const ::Variable& variable) {
  return matchPtr<VariableExpression>(
      AD_PROPERTY(VariableExpression, value, testing::Eq(variable)));
};

// Return a matcher that matches a `SparqlExpression::Ptr`that stores an
// `Expression` (template argument), the children of which match the
// `childrenMatchers`.
template <typename Expression, typename... ChildrenMatchers>
auto matchPtrWithChildren(ChildrenMatchers&&... childrenMatchers)
    -> Matcher<const SparqlExpression::Ptr&> {
  return matchPtr<Expression>(
      AD_PROPERTY(SparqlExpression, childrenForTesting,
                  testing::ElementsAre(childrenMatchers...)));
}

// Same as `matchPtrWithChildren` above, but the children are all variables.
template <typename Expression>
auto matchPtrWithVariables(const std::same_as<::Variable> auto&... children)
    -> Matcher<const SparqlExpression::Ptr&> {
  return matchPtrWithChildren<Expression>(
      variableExpressionMatcher(children)...);
}

// Return a matcher  that checks whether a given `SparqlExpression::Ptr` points
// (via `dynamic_cast`) to an object of the same type that a call to the
// `makeFunction` yields. The matcher also checks that the expression's children
// match the `childrenMatchers`.
auto matchNaryWithChildrenMatchers(auto makeFunction,
                                   auto&&... childrenMatchers)
    -> Matcher<const SparqlExpression::Ptr&> {
  using namespace sparqlExpression;
  auto typeIdLambda = [](const auto& ptr) {
    return std::type_index{typeid(*ptr)};
  };

  [[maybe_unused]] auto makeDummyChild = [](auto&&) -> SparqlExpression::Ptr {
    return std::make_unique<VariableExpression>(::Variable{"?x"});
  };
  auto expectedTypeIndex =
      typeIdLambda(makeFunction(makeDummyChild(childrenMatchers)...));
  Matcher<const SparqlExpression::Ptr&> typeIdMatcher =
      ::testing::ResultOf(typeIdLambda, ::testing::Eq(expectedTypeIndex));
  return ::testing::AllOf(typeIdMatcher,
                          ::testing::Pointee(AD_PROPERTY(
                              SparqlExpression, childrenForTesting,
                              ::testing::ElementsAre(childrenMatchers...))));
}

inline auto idExpressionMatcher = [](Id id) {
  return matchPtr<IdExpression>(
      AD_PROPERTY(IdExpression, value, testing::Eq(id)));
};

// Return a matcher  that checks whether a given `SparqlExpression::Ptr` points
// (via `dynamic_cast`) to an object of the same type that a call to the
// `makeFunction` yields. The matcher also checks that the expression's children
// are the `variables`.
auto matchNary(auto makeFunction,
               QL_CONCEPT_OR_NOTHING(
                   ad_utility::SimilarTo<::Variable>) auto&&... variables)
    -> Matcher<const sparqlExpression::SparqlExpression::Ptr&> {
  using namespace sparqlExpression;
  return matchNaryWithChildrenMatchers(makeFunction,
                                       variableExpressionMatcher(variables)...);
}
template <typename F>
auto matchUnary(F makeFunction) -> Matcher<const SparqlExpression::Ptr&> {
  return matchNary(makeFunction, ::Variable{"?x"});
}

template <typename T>
auto matchLiteralExpression(const T& value)
    -> Matcher<const SparqlExpression::Ptr&> {
  using Expr = sparqlExpression::detail::LiteralExpression<T>;
  return testing::Pointee(testing::WhenDynamicCastTo<const Expr&>(
      AD_PROPERTY(Expr, value, testing::Eq(value))));
}
}  // namespace builtInCall

// Match an EXISTS function
inline auto Exists(Matcher<const ParsedQuery&> pattern) {
  return testing::Pointee(
      testing::WhenDynamicCastTo<const sparqlExpression::ExistsExpression&>(
          AD_PROPERTY(sparqlExpression::ExistsExpression, argument, pattern)));
}

// Match a NOT EXISTS function
inline auto NotExists(Matcher<const ParsedQuery&> pattern) {
  return builtInCall::matchNaryWithChildrenMatchers(
      &sparqlExpression::makeUnaryNegateExpression, Exists(pattern));
}

// Return a matcher that checks if a `GraphPattern` contains an EXISTS filter
// that matches the given `matcher` when comparing the inner parsed query of the
// `ExistsExpression`.
inline auto ContainsExistsFilter =
    [](const Matcher<const ParsedQuery&>& matcher)
    -> Matcher<const parsedQuery::GraphPattern&> {
  return AD_FIELD(parsedQuery::GraphPattern, _filters,
                  ::testing::ElementsAre(AD_FIELD(
                      SparqlFilter, expression_,
                      AD_PROPERTY(sparqlExpression::SparqlExpressionPimpl,
                                  getExistsExpressions,
                                  ::testing::ElementsAre(Exists(matcher))))));
};

// Check that the given filters are set on the graph pattern.
inline auto Filters = [](const auto&... filterMatchers)
    -> Matcher<const ParsedQuery::GraphPattern&> {
  return AD_FIELD(ParsedQuery::GraphPattern, _filters,
                  testing::ElementsAre(filterMatchers...));
};

// Matcher for `FILTER EXISTS` filters.
inline auto ExistsFilter =
    [](const Matcher<const parsedQuery::GraphPattern&>& m,
       ScanSpecificationAsTripleComponent::Graphs defaultGraphs = std::nullopt,
       ScanSpecificationAsTripleComponent::Graphs namedGraphs =
           std::nullopt) -> Matcher<const SparqlFilter&> {
  return AD_FIELD(SparqlFilter, expression_,
                  AD_PROPERTY(sparqlExpression::SparqlExpressionPimpl, getPimpl,
                              Exists(SelectQuery(VariablesSelect({}), m,
                                                 defaultGraphs, namedGraphs))));
};

// A helper matcher for a graph pattern that targets all triples in `graph`.
inline auto SelectAllPattern =
    [](parsedQuery::GroupGraphPattern::GraphSpec graph,
       std::optional<std::string>&& filter =
           std::nullopt) -> Matcher<const parsedQuery::GraphPattern&> {
  return GraphPattern(
      false, filter ? std::vector{filter.value()} : std::vector<std::string>{},
      Group(GraphPattern(Triples(
                {{{::Variable("?s"), ::Variable("?p"), ::Variable("?o")}}})),
            std::move(graph)));
};

// Matcher for a `ParsedQuery` with a clear of `graph`.
inline auto Clear = [](const parsedQuery::GroupGraphPattern::GraphSpec& graph,
                       std::optional<std::string>&& filter = std::nullopt) {
  // The `GraphSpec` type is the same variant as
  // `SparqlTripleSimpleWithGraph::Graph` so it can be used for both.
  return UpdateClause(
      GraphUpdate(
          {{{::Variable("?s")}, {::Variable("?p")}, {::Variable("?o")}, graph}},
          {}),
      SelectAllPattern(graph, AD_FWD(filter)));
};

// Matcher for a `ParsedQuery` with an add of all triples in `from` to `to`.
inline auto AddAll = [](const SparqlTripleSimpleWithGraph::Graph& from,
                        const SparqlTripleSimpleWithGraph::Graph& to) {
  return UpdateClause(GraphUpdate({}, {SparqlTripleSimpleWithGraph(
                                          ::Variable("?s"), ::Variable("?p"),
                                          ::Variable("?o"), to)}),
                      SelectAllPattern(from));
};

}  // namespace matchers

namespace sparqlParserTestHelpers {

using namespace sparqlParserHelpers;
namespace m = matchers;
using Parser = SparqlAutomaticParser;
using namespace std::literals;
using Var = Variable;

const ad_utility::HashMap<std::string, std::string> defaultPrefixMap{
    {std::string{QLEVER_INTERNAL_PREFIX_NAME},
     std::string{QLEVER_INTERNAL_PREFIX_IRI}}};

template <auto F, bool testInsideConstructTemplate = false>
auto parse =
    [](const std::string& input, SparqlQleverVisitor::PrefixMap prefixes = {},
       std::optional<ParsedQuery::DatasetClauses> clauses = std::nullopt,
       SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
           SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False) {
      // We might parse updates here, should we move the blank node manager out
      // to make it testable/accessible?
      static ad_utility::BlankNodeManager blankNodeManager;
      static EncodedIriManager encodedIriManager;
      ParserAndVisitor p{
          &blankNodeManager,   &encodedIriManager, input,
          std::move(prefixes), std::move(clauses), disableSomeChecks};
      if (testInsideConstructTemplate) {
        p.visitor_.setParseModeToInsideConstructTemplateForTesting();
      }
      return p.parseTypesafe(F);
    };

const auto parseBlankNode = parse<&Parser::blankNode>;
const auto parseBlankNodeConstruct = parse<&Parser::blankNode, true>;
const auto parseCollection = parse<&Parser::collection>;
const auto parseCollectionConstruct = parse<&Parser::collection, true>;
const auto parseConstructTriples = parse<&Parser::constructTriples>;
const auto parseGraphNode = parse<&Parser::graphNode>;
const auto parseGraphNodeConstruct = parse<&Parser::graphNode, true>;
const auto parseObjectList = parse<&Parser::objectList>;
const auto parsePropertyList = parse<&Parser::propertyList>;
const auto parsePropertyListNotEmpty = parse<&Parser::propertyListNotEmpty>;
const auto parseSelectClause = parse<&Parser::selectClause>;
const auto parseTriplesSameSubject = parse<&Parser::triplesSameSubject>;
const auto parseTriplesSameSubjectConstruct =
    parse<&Parser::triplesSameSubject, true>;
const auto parseVariable = parse<&Parser::var>;
const auto parseVarOrTerm = parse<&Parser::varOrTerm>;
const auto parseVerb = parse<&Parser::verb>;

template <auto Clause, bool parseInsideConstructTemplate = false,
          typename Value = decltype(parse<Clause>("").resultOfParse_)>
struct ExpectCompleteParse {
  SparqlQleverVisitor::PrefixMap prefixMap_ = {};
  SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False;

  auto operator()(const std::string& input, const Value& value,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    return operator()(input, value, prefixMap_, l);
  }

  auto operator()(const std::string& input,
                  const testing::Matcher<const Value&>& matcher,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    return operator()(input, matcher, prefixMap_, l);
  }

  auto operator()(const std::string& input, const Value& value,
                  SparqlQleverVisitor::PrefixMap prefixMap,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    return operator()(input, testing::Eq(value), std::move(prefixMap), l);
  }

  auto operator()(const std::string& input,
                  const testing::Matcher<const Value&>& matcher,
                  SparqlQleverVisitor::PrefixMap prefixMap,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    auto tr = generateLocationTrace(l, "successful parsing was expected here");
    EXPECT_NO_THROW({
      return expectCompleteParse(
          parse<Clause, parseInsideConstructTemplate>(
              input, std::move(prefixMap), std::nullopt, disableSomeChecks),
          matcher, l);
    });
  }

  auto operator()(const std::string& input,
                  const testing::Matcher<const Value&>& matcher,
                  ParsedQuery::DatasetClauses activeDatasetClauses,
                  ad_utility::source_location l =
                      ad_utility::source_location::current()) const {
    auto tr = generateLocationTrace(l, "successful parsing was expected here");
    EXPECT_NO_THROW({
      return expectCompleteParse(
          parse<Clause, parseInsideConstructTemplate>(
              input, {}, std::move(activeDatasetClauses), disableSomeChecks),
          matcher, l);
    });
  }
};

template <auto Clause>
struct ExpectParseFails {
  SparqlQleverVisitor::PrefixMap prefixMap_ = {};
  SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False;

  auto operator()(
      const std::string& input,
      const testing::Matcher<const std::string&>& messageMatcher = ::testing::_,
      ad_utility::source_location l = ad_utility::source_location::current()) {
    return operator()(input, prefixMap_, messageMatcher, l);
  }

  auto operator()(
      const std::string& input, SparqlQleverVisitor::PrefixMap prefixMap,
      const testing::Matcher<const std::string&>& messageMatcher = ::testing::_,
      ad_utility::source_location l = ad_utility::source_location::current()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE(
        parse<Clause>(input, std::move(prefixMap), {}, disableSomeChecks),
        messageMatcher);
  }
};

// TODO: make function that creates both the complete and fails parser. and use
// them with structured binding.

const auto nil = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>";
const auto first = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>";
const auto rest = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>";
const auto type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::SizeIs;
using ::testing::StrEq;
}  // namespace sparqlParserTestHelpers

#endif  // QLEVER_TEST_SPARQLANTLRPARSERTESTHELPERS_H
