//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "./util/GTestHelpers.h"
#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralElementOperation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryPlanner.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock.h"
#include "parser/SparqlParser.h"

using ad_utility::source_location;

namespace queryPlannerTestHelpers {
using namespace ::testing;

// Most of the following matchers work on `QueryExecutionTree`s, so we introduce
// an alias.
using QetMatcher = Matcher<const QueryExecutionTree&>;

/// Returns a matcher that checks that a given `QueryExecutionTree`'s
/// `rootOperation` can by dynamically cast to `OperationType`, and that
/// `matcher` matches the result of this cast.
template <typename OperationType>
QetMatcher RootOperation(auto matcher) {
  auto getRootOperation =
      [](const QueryExecutionTree& tree) -> const ::Operation& {
    return *tree.getRootOperation().get();
  };
  return ResultOf(getRootOperation,
                  WhenDynamicCastTo<const OperationType&>(matcher));
}

// Return a matcher that test whether a given `QueryExecutionTree` contains a
// `OperationType` operation the children of which match the
// `childMatcher`s. Note that the child matchers are not ordered.
template <typename OperationType>
inline auto MatchTypeAndUnorderedChildren =
    [](const std::same_as<QetMatcher> auto&... childMatchers) {
      return RootOperation<OperationType>(
          AllOf(Property("getChildren", &Operation::getChildren,
                         UnorderedElementsAre(Pointee(childMatchers)...))));
    };

// Similar to `MatchTypeAndUnorderedChildren`, but here the children have to
// appear in exact the correct order.
template <typename OperationType>
inline auto MatchTypeAndOrderedChildren =
    [](const std::same_as<QetMatcher> auto&... childMatchers) {
      return RootOperation<OperationType>(
          AllOf(Property("getChildren", &Operation::getChildren,
                         ElementsAre(Pointee(childMatchers)...))));
    };

/// Return a matcher that checks that a given `QueryExecutionTree` consists of a
/// single `IndexScan` with the given `subject`, `predicate`, and `object`, and
/// that the `ScanType` of this `IndexScan` is any of the given
/// `allowedPermutations`.
inline auto IndexScan =
    [](TripleComponent subject, TripleComponent predicate,
       TripleComponent object,
       const std::vector<Permutation::Enum>& allowedPermutations = {})
    -> QetMatcher {
  size_t numVariables = static_cast<size_t>(subject.isVariable()) +
                        static_cast<size_t>(predicate.isVariable()) +
                        static_cast<size_t>(object.isVariable());
  auto permutationMatcher = allowedPermutations.empty()
                                ? ::testing::A<Permutation::Enum>()
                                : AnyOfArray(allowedPermutations);
  return RootOperation<::IndexScan>(
      AllOf(AD_PROPERTY(IndexScan, permutation, permutationMatcher),
            AD_PROPERTY(IndexScan, getResultWidth, Eq(numVariables)),
            AD_PROPERTY(IndexScan, getSubject, Eq(subject)),
            AD_PROPERTY(IndexScan, getPredicate, Eq(predicate)),
            AD_PROPERTY(IndexScan, getObject, Eq(object))));
};

inline auto Bind = [](const QetMatcher& childMatcher,
                      std::string_view expression,
                      Variable target) -> QetMatcher {
  auto innerMatcher =
      AllOf(AD_FIELD(parsedQuery::Bind, _target, Eq(target)),
            AD_FIELD(parsedQuery::Bind, _expression,
                     AD_PROPERTY(sparqlExpression::SparqlExpressionPimpl,
                                 getDescriptor, Eq(expression))));
  return RootOperation<::Bind>(AllOf(
      AD_PROPERTY(::Bind, bind, AllOf(innerMatcher)),
      AD_PROPERTY(Operation, getChildren, ElementsAre(Pointee(childMatcher)))));
};

inline auto NeutralElementOperation = []() {
  return RootOperation<::NeutralElementOperation>(
      An<const ::NeutralElementOperation&>());
};

// Same as above, but the subject, predicate, and object are passed in as
// strings. The strings are automatically converted a matching
// `TripleComponent`.
inline auto IndexScanFromStrings =
    [](std::string_view subject, std::string_view predicate,
       std::string_view object,
       const std::vector<Permutation::Enum>& allowedPermutations = {})
    -> QetMatcher {
  auto strToComp = [](std::string_view s) -> TripleComponent {
    if (s.starts_with("?")) {
      return ::Variable{std::string{s}};
    }
    return s;
  };
  return IndexScan(strToComp(subject), strToComp(predicate), strToComp(object),
                   allowedPermutations);
};

// For the following Join algorithms the order of the children is not important.
inline auto MultiColumnJoin = MatchTypeAndUnorderedChildren<::MultiColumnJoin>;
inline auto Join = MatchTypeAndUnorderedChildren<::Join>;
inline auto CartesianProductJoin =
    MatchTypeAndUnorderedChildren<::CartesianProductJoin>;

/// Parse the given SPARQL `query`, pass it to a `QueryPlanner` with empty
/// execution context, and return the resulting `QueryExecutionTree`
QueryExecutionTree parseAndPlan(std::string query) {
  ParsedQuery pq = SparqlParser::parseQuery(std::move(query));
  // TODO<joka921> make it impossible to pass `nullptr` here, properly mock a
  // queryExecutionContext.
  return QueryPlanner{nullptr}.createExecutionTree(pq);
}

// Check that the `QueryExecutionTree` that is obtained by parsing and planning
// the `query` matches the `matcher`.
void expect(std::string query, auto matcher,
            source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expect");
  auto qet = parseAndPlan(std::move(query));
  EXPECT_THAT(qet, matcher);
}
}  // namespace queryPlannerTestHelpers
