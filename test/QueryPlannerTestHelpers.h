//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>

#include <optional>

#include "./util/GTestHelpers.h"
#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Minus.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralElementOperation.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryPlanner.h"
#include "engine/Service.h"
#include "engine/Sort.h"
#include "engine/SpatialJoin.h"
#include "engine/TextIndexScanForEntity.h"
#include "engine/TextIndexScanForWord.h"
#include "engine/TextLimit.h"
#include "engine/TransitivePathBase.h"
#include "engine/Union.h"
#include "global/RuntimeParameters.h"
#include "parser/SparqlParser.h"
#include "util/IndexTestHelpers.h"

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

// Match the `getChildren` method of an `Operation`.
inline Matcher<const ::Operation&> children(
    const std::same_as<QetMatcher> auto&... childMatchers) {
  return Property("getChildren", &Operation::getChildren,
                  ElementsAre(Pointee(childMatchers)...));
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
      return RootOperation<OperationType>(AllOf(children(childMatchers...)));
    };

/// Return a matcher that checks that a given `QueryExecutionTree` consists of a
/// single `IndexScan` with the given `subject`, `predicate`, and `object`, and
/// that the `ScanType` of this `IndexScan` is any of the given
/// `allowedPermutations`.
constexpr auto IndexScan =
    [](TripleComponent subject, TripleComponent predicate,
       TripleComponent object,
       const std::vector<Permutation::Enum>& allowedPermutations = {},
       const ScanSpecificationAsTripleComponent::Graphs& graphs = std::nullopt,
       const std::vector<Variable>& additionalVariables = {},
       const std::vector<ColumnIndex>& additionalColumns = {}) -> QetMatcher {
  size_t numVariables = static_cast<size_t>(subject.isVariable()) +
                        static_cast<size_t>(predicate.isVariable()) +
                        static_cast<size_t>(object.isVariable()) +
                        additionalColumns.size();
  auto permutationMatcher = allowedPermutations.empty()
                                ? ::testing::A<Permutation::Enum>()
                                : AnyOfArray(allowedPermutations);
  return RootOperation<::IndexScan>(
      AllOf(AD_PROPERTY(IndexScan, permutation, permutationMatcher),
            AD_PROPERTY(IndexScan, getResultWidth, Eq(numVariables)),
            AD_PROPERTY(IndexScan, subject, Eq(subject)),
            AD_PROPERTY(IndexScan, predicate, Eq(predicate)),
            AD_PROPERTY(IndexScan, object, Eq(object)),
            AD_PROPERTY(IndexScan, additionalVariables,
                        ElementsAreArray(additionalVariables)),
            AD_PROPERTY(IndexScan, additionalColumns,
                        ElementsAreArray(additionalColumns)),
            AD_PROPERTY(IndexScan, graphsToFilter, Eq(graphs))));
};

// Match the `NeutralElementOperation`.
constexpr auto NeutralElement = []() -> QetMatcher {
  return MatchTypeAndOrderedChildren<::NeutralElementOperation>();
};

constexpr auto TextIndexScanForWord = [](Variable textRecordVar,
                                         string word) -> QetMatcher {
  return RootOperation<::TextIndexScanForWord>(AllOf(
      AD_PROPERTY(::TextIndexScanForWord, getResultWidth,
                  Eq(1 + word.ends_with('*'))),
      AD_PROPERTY(::TextIndexScanForWord, textRecordVar, Eq(textRecordVar)),
      AD_PROPERTY(::TextIndexScanForWord, word, word)));
};

// Matcher for the `TextLimit` Operation.
constexpr auto TextLimit = [](const size_t n, const QetMatcher& childMatcher,
                              const Variable& textRecVar,
                              const vector<Variable>& entityVars,
                              const vector<Variable>& scoreVars) -> QetMatcher {
  return RootOperation<::TextLimit>(AllOf(
      AD_PROPERTY(::TextLimit, getTextLimit, Eq(n)), children(childMatcher),
      AD_PROPERTY(::TextLimit, getTextRecordVariable, Eq(textRecVar)),
      AD_PROPERTY(::TextLimit, getEntityVariables,
                  UnorderedElementsAreArray(entityVars)),
      AD_PROPERTY(::TextLimit, getScoreVariables,
                  UnorderedElementsAreArray(scoreVars))));
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
      AD_PROPERTY(::Bind, bind, AllOf(innerMatcher)), children(childMatcher)));
};

// Matcher for a `CountAvailablePredicates` operation. The case of 0 children
// means that it's a full scan.
inline auto CountAvailablePredicates =
    [](size_t subjectColumnIdx, const Variable& predicateVar,
       const Variable& countVar,
       const std::same_as<QetMatcher> auto&... childMatchers)
        requires(sizeof...(childMatchers) <= 1) {
  return RootOperation<::CountAvailablePredicates>(AllOf(
      AD_PROPERTY(::CountAvailablePredicates, subjectColumnIndex,
                  Eq(subjectColumnIdx)),
      AD_PROPERTY(::CountAvailablePredicates, predicateVariable,
                  Eq(predicateVar)),
      AD_PROPERTY(::CountAvailablePredicates, countVariable, Eq(countVar)),
      children(childMatchers...)));
};

// Same as above, but the subject, predicate, and object are passed in as
// strings. The strings are automatically converted a matching
// `TripleComponent`.
inline auto IndexScanFromStrings =
    [](std::string_view subject, std::string_view predicate,
       std::string_view object,
       const std::vector<Permutation::Enum>& allowedPermutations = {},
       const std::optional<ad_utility::HashSet<std::string>> graphs =
           std::nullopt,
       const std::vector<Variable>& additionalVariables = {},
       const std::vector<ColumnIndex>& additionalColumns = {}) -> QetMatcher {
  auto strToComp = [](std::string_view s) -> TripleComponent {
    if (s.starts_with("?")) {
      return ::Variable{std::string{s}};
    } else if (s.starts_with('<')) {
      return TripleComponent::Iri::fromIriref(s);
    }
    return s;
  };

  ScanSpecificationAsTripleComponent::Graphs graphsOut = std::nullopt;
  if (graphs.has_value()) {
    graphsOut.emplace();
    for (const auto& graphIn : graphs.value()) {
      graphsOut->insert(strToComp(graphIn));
    }
  }
  return IndexScan(strToComp(subject), strToComp(predicate), strToComp(object),
                   allowedPermutations, graphsOut, additionalVariables,
                   additionalColumns);
};

// For the following Join algorithms the order of the children is not important.
inline auto MultiColumnJoin = MatchTypeAndUnorderedChildren<::MultiColumnJoin>;
inline auto Join = MatchTypeAndUnorderedChildren<::Join>;

constexpr auto OptionalJoin = MatchTypeAndOrderedChildren<::OptionalJoin>;

constexpr auto Minus = MatchTypeAndOrderedChildren<::Minus>;

// Return a matcher that matches a query execution tree that consists of
// multiple JOIN operations that join the `children`. The `INTERNAL SORT BY`
// operations required for the joins are also ignored by this matcher.
inline auto UnorderedJoins = [](auto&&... children) -> QetMatcher {
  using Vec = std::vector<std::reference_wrapper<const QueryExecutionTree>>;
  auto collectChildrenRecursive = [](const QueryExecutionTree& tree,
                                     Vec& children, const auto& self) -> void {
    const Operation* operation = tree.getRootOperation().get();
    auto join = dynamic_cast<const ::Join*>(operation);
    auto multiColJoin = dynamic_cast<const ::MultiColumnJoin*>(operation);
    // Also allow the INTERNAL SORT BY operations that are needed for the joins.
    // TODO<joka921> is this the right place to also check that those have the
    // correct columns?
    auto sort = dynamic_cast<const ::Sort*>(operation);
    if (!join && !sort && !multiColJoin) {
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
      return RootOperation<::TransitivePathBase>(
          AllOf(children(childMatchers...),
                AD_PROPERTY(TransitivePathBase, getMinDist, Eq(minDist)),
                AD_PROPERTY(TransitivePathBase, getMaxDist, Eq(maxDist)),
                AD_PROPERTY(TransitivePathBase, getLeft,
                            TransitivePathSideMatcher(left)),
                AD_PROPERTY(TransitivePathBase, getRight,
                            TransitivePathSideMatcher(right))));
    };

// Match a SpatialJoin operation, set arguments to ignore to -1
inline auto SpatialJoin =
    [](size_t maxDist, size_t maxResults,
       const std::same_as<QetMatcher> auto&... childMatchers) {
      return RootOperation<::SpatialJoin>(
          AllOf(children(childMatchers...),
                AD_PROPERTY(SpatialJoin, onlyForTestingGetConfig,
                            Eq(std::pair(maxDist, maxResults)))));
    };

// Match a GroupBy operation
static constexpr auto GroupBy =
    [](const std::vector<Variable>& groupByVariables,
       const std::vector<std::string>& aliases,
       const QetMatcher& childMatcher) -> QetMatcher {
  // TODO<joka921> Also test the aliases.
  auto aliasesToStrings = [](const std::vector<Alias>& aliases) {
    std::vector<std::string> result;
    std::ranges::transform(aliases, std::back_inserter(result),
                           &Alias::getDescriptor);
    return result;
  };

  return RootOperation<::GroupBy>(
      AllOf(children(childMatcher),
            AD_PROPERTY(::GroupBy, groupByVariables,
                        UnorderedElementsAreArray(groupByVariables)),
            AD_PROPERTY(::GroupBy, aliases,
                        ResultOf(aliasesToStrings, ContainerEq(aliases)))));
};

// Match a sort operation. Currently, this is only required by the binary search
// version of the transitive path operation. This matcher checks only the
// children of the sort operation.
inline auto Sort = MatchTypeAndUnorderedChildren<::Sort>;

// Match a `Filter` operation. The matching of the expression is currently only
// done via the descriptor.
constexpr auto Filter = [](std::string_view descriptor,
                           const QetMatcher& childMatcher) {
  return RootOperation<::Filter>(
      AllOf(children(childMatcher),
            AD_PROPERTY(::Operation, getDescriptor, HasSubstr(descriptor))));
};

// Match an `OrderBy` operation
constexpr auto OrderBy = [](const ::OrderBy::SortedVariables& sortedVariables,
                            const QetMatcher& childMatcher) {
  return RootOperation<::OrderBy>(
      AllOf(children(childMatcher),
            AD_PROPERTY(::OrderBy, getSortedVariables, Eq(sortedVariables))));
};

// Match a `UNION` operation.
constexpr auto Union = MatchTypeAndOrderedChildren<::Union>;

// Match a `SERVICE` operation.
constexpr auto Service = [](const std::optional<QetMatcher>& siblingMatcher,
                            std::string_view graphPatternAsString) {
  const auto optSiblingMatcher =
      [&]() -> Matcher<const std::shared_ptr<QueryExecutionTree>&> {
    if (siblingMatcher.has_value()) {
      return Pointee(siblingMatcher.value());
    }
    return IsNull();
  }();

  return RootOperation<::Service>(
      AllOf(AD_PROPERTY(::Service, getSiblingTree, optSiblingMatcher),
            AD_PROPERTY(::Service, getGraphPatternAsString,
                        Eq(graphPatternAsString))));
};

/// Parse the given SPARQL `query`, pass it to a `QueryPlanner` with empty
/// execution context, and return the resulting `QueryExecutionTree`
QueryExecutionTree parseAndPlan(std::string query, QueryExecutionContext* qec) {
  ParsedQuery pq = SparqlParser::parseQuery(std::move(query));
  // TODO<joka921> make it impossible to pass `nullptr` here, properly mock a
  // queryExecutionContext.
  return QueryPlanner{qec, std::make_shared<ad_utility::CancellationHandle<>>()}
      .createExecutionTree(pq);
}

// Check that the `QueryExecutionTree` that is obtained by parsing and planning
// the `query` matches the `matcher`. The query planning budget can be
// controlled to choose between the greedy and the dynamic programming planner.
// This function only serves as a common implementation, for the actual tests
// the three functions below should be used.
void expectWithGivenBudget(std::string query, auto matcher,
                           std::optional<QueryExecutionContext*> optQec,
                           size_t queryPlanningBudget,
                           source_location l = source_location::current()) {
  auto budgetBackup = RuntimeParameters().get<"query-planning-budget">();
  RuntimeParameters().set<"query-planning-budget">(queryPlanningBudget);
  auto cleanup = absl::Cleanup{[budgetBackup]() {
    RuntimeParameters().set<"query-planning-budget">(budgetBackup);
  }};
  auto trace = generateLocationTrace(
      l, absl::StrCat("expect with budget ", queryPlanningBudget));
  QueryExecutionContext* qec = optQec.value_or(ad_utility::testing::getQec());
  auto qet = parseAndPlan(std::move(query), qec);
  qet.getRootOperation()->createRuntimeInfoFromEstimates(
      qet.getRootOperation()->getRuntimeInfoPointer());
  EXPECT_THAT(qet, matcher);
}

// Same as `expectWithGivenBudget` above, but always use the greedy query
// planner.
void expectGreedy(std::string query, auto matcher,
                  std::optional<QueryExecutionContext*> optQec = std::nullopt,
                  source_location l = source_location::current()) {
  expectWithGivenBudget(std::move(query), std::move(matcher), optQec, 0, l);
}
// Same as `expectWithGivenBudget` above, but always use the dynamic programming
// query planner.
void expectDynamicProgramming(
    std::string query, auto matcher,
    std::optional<QueryExecutionContext*> optQec = std::nullopt,
    source_location l = source_location::current()) {
  expectWithGivenBudget(std::move(query), std::move(matcher), optQec,
                        std::numeric_limits<size_t>::max(), l);
}

// Same as `expectWithGivenBudget` above, but run the test for different query
// planning budgets. This is guaranteed to run with both the greedy query
// planner and the dynamic-programming based query planner.
void expect(std::string query, auto matcher,
            std::optional<QueryExecutionContext*> optQec = std::nullopt,
            source_location l = source_location::current()) {
  auto e = [&](size_t budget) {
    expectWithGivenBudget(query, matcher, optQec, budget, l);
  };
  e(0);
  e(1);
  e(4);
  e(16);
  e(64'000'000);
}
}  // namespace queryPlannerTestHelpers
