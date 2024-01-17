//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "./IndexTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralElementOperation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryPlanner.h"
#include "engine/Sort.h"
#include "engine/TextIndexScanForEntity.h"
#include "engine/TextIndexScanForWord.h"
#include "engine/TransitivePath.h"
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

inline auto TextIndexScanForWord = [](Variable textRecordVar,
                                      string word) -> QetMatcher {
  return RootOperation<::TextIndexScanForWord>(AllOf(
      AD_PROPERTY(::TextIndexScanForWord, getResultWidth,
                  Eq(1 + word.ends_with('*'))),
      AD_PROPERTY(::TextIndexScanForWord, textRecordVar, Eq(textRecordVar)),
      AD_PROPERTY(::TextIndexScanForWord, word, word)));
};

inline auto TextIndexScanForEntity =
    [](Variable textRecordVar, std::variant<Variable, std::string> entity,
       string word) -> QetMatcher {
  // TODO: Implement AD_THROWING_PROPERTY(..., Exception matcher) and use it
  // here to test the contract-checks in entityVariable() and fixedEntity().
  if (std::holds_alternative<Variable>(entity)) {
    return RootOperation<::TextIndexScanForEntity>(AllOf(
        AD_PROPERTY(::TextIndexScanForEntity, getResultWidth,
                    Eq(2 + std::holds_alternative<Variable>(entity))),
        AD_PROPERTY(::TextIndexScanForEntity, textRecordVar, Eq(textRecordVar)),
        AD_PROPERTY(::TextIndexScanForEntity, entityVariable,
                    std::get<Variable>(entity)),
        AD_PROPERTY(::TextIndexScanForEntity, word, word),
        AD_PROPERTY(::TextIndexScanForEntity, hasFixedEntity, false)));
  } else {
    return RootOperation<::TextIndexScanForEntity>(AllOf(
        AD_PROPERTY(::TextIndexScanForEntity, getResultWidth,
                    Eq(2 + std::holds_alternative<Variable>(entity))),
        AD_PROPERTY(::TextIndexScanForEntity, textRecordVar, Eq(textRecordVar)),
        AD_PROPERTY(::TextIndexScanForEntity, fixedEntity,
                    std::get<std::string>(entity)),
        AD_PROPERTY(::TextIndexScanForEntity, word, word),
        AD_PROPERTY(::TextIndexScanForEntity, hasFixedEntity, true)));
  }
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

// Return a matcher that matches a query execution tree that consists of
// multiple JOIN operations that join the `children`. The `INTERNAL SORT BY`
// operations required for the joins are also ignored by this matcher.
inline auto UnorderedJoins = [](auto&&... children) -> QetMatcher {
  using Vec = std::vector<std::reference_wrapper<const QueryExecutionTree>>;
  auto collectChildrenRecursive = [](const QueryExecutionTree& tree,
                                     Vec& children, const auto& self) -> void {
    const Operation* operation = tree.getRootOperation().get();
    auto join = dynamic_cast<const ::Join*>(operation);
    // Also allow the INTERNAL SORT BY operations that are needed for the joins.
    // TODO<joka921> is this the right place to also check that those have the
    // correct columns?
    auto sort = dynamic_cast<const ::Sort*>(operation);
    if (!join && !sort) {
      children.push_back(tree);
    } else {
      for (const auto& child : operation->getChildren()) {
        self(*child, children, self);
      }
    }
  };

  auto collectChildren = [&collectChildrenRecursive](const auto& tree) {
    Vec children;
    collectChildrenRecursive(tree, children, collectChildrenRecursive);
    return children;
  };

  return ResultOf(collectChildren, UnorderedElementsAre(children...));
};

inline auto CartesianProductJoin =
    MatchTypeAndUnorderedChildren<::CartesianProductJoin>;

inline auto TransitivePathSideMatcher = [](TransitivePathSide side) {
  return AllOf(AD_FIELD(TransitivePathSide, value_, Eq(side.value_)),
               AD_FIELD(TransitivePathSide, subCol_, Eq(side.subCol_)),
               AD_FIELD(TransitivePathSide, outputCol_, Eq(side.outputCol_)));
};

// Match a TransitivePath operation
inline auto TransitivePath =
    [](TransitivePathSide left, TransitivePathSide right, size_t minDist,
       size_t maxDist, const std::same_as<QetMatcher> auto&... childMatchers) {
      return RootOperation<::TransitivePath>(AllOf(
          Property("getChildren", &Operation::getChildren,
                   ElementsAre(Pointee(childMatchers)...)),
          AD_PROPERTY(TransitivePath, getMinDist, Eq(minDist)),
          AD_PROPERTY(TransitivePath, getMaxDist, Eq(maxDist)),
          AD_PROPERTY(TransitivePath, getLeft, TransitivePathSideMatcher(left)),
          AD_PROPERTY(TransitivePath, getRight,
                      TransitivePathSideMatcher(right))));
    };

/// Parse the given SPARQL `query`, pass it to a `QueryPlanner` with empty
/// execution context, and return the resulting `QueryExecutionTree`
QueryExecutionTree parseAndPlan(std::string query, QueryExecutionContext* qec) {
  ParsedQuery pq = SparqlParser::parseQuery(std::move(query));
  // TODO<joka921> make it impossible to pass `nullptr` here, properly mock a
  // queryExecutionContext.
  return QueryPlanner{qec}.createExecutionTree(pq);
}

// Check that the `QueryExecutionTree` that is obtained by parsing and planning
// the `query` matches the `matcher`.
void expect(std::string query, auto matcher,
            std::optional<QueryExecutionContext*> optQec = std::nullopt,
            source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expect");
  QueryExecutionContext* qec = optQec.value_or(ad_utility::testing::getQec());
  auto qet = parseAndPlan(std::move(query), qec);
  qet.getRootOperation()->createRuntimeInfoFromEstimates(
      qet.getRootOperation()->getRuntimeInfoPointer());
  EXPECT_THAT(qet, matcher);
}
}  // namespace queryPlannerTestHelpers
