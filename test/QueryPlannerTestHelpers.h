//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_QUERYPLANNERTESTHELPERS_H
#define QLEVER_TEST_QUERYPLANNERTESTHELPERS_H

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>

#include <optional>
#include <variant>

#include "./util/GTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "engine/Bind.h"
#include "engine/CartesianProductJoin.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/Describe.h"
#include "engine/Distinct.h"
#include "engine/ExistsJoin.h"
#include "engine/ExplicitIdTableOperation.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/MaterializedViews.h"
#include "engine/Minus.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralElementOperation.h"
#include "engine/NeutralOptional.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/PathSearch.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryPlanner.h"
#include "engine/Sort.h"
#include "engine/SpatialJoin.h"
#include "engine/TextIndexScanForEntity.h"
#include "engine/TextIndexScanForWord.h"
#include "engine/TextLimit.h"
#include "engine/TransitivePathBase.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "global/RuntimeParameters.h"
#include "parser/SparqlParser.h"
#include "rdfTypes/Iri.h"
#include "util/Exception.h"
#include "util/IndexTestHelpers.h"
#include "util/TypeTraits.h"

using ad_utility::source_location;

namespace queryPlannerTestHelpers {
using namespace qlever;
using namespace ::testing;

// Most of the following matchers work on `QueryExecutionTree`s, so we introduce
// an alias.
using QetMatcher = Matcher<const QueryExecutionTree&>;

// Return a matcher that checks that the `rootOperation` of a given
// `QueryExecutionTree` matches the given `matcher`.
inline QetMatcher RootOperationBase(Matcher<const Operation&> matcher) {
  auto getRootOperation =
      [](const QueryExecutionTree& tree) -> const qlever::Operation& {
    return *tree.getRootOperation().get();
  };
  return ResultOf(getRootOperation, matcher);
}
/// Returns a matcher that checks that a given `QueryExecutionTree`'s
/// `rootOperation` can by dynamically cast to `OperationType`, and that
/// `matcher` matches the result of this cast.
template <typename OperationType, typename M>
QetMatcher RootOperation(M matcher) {
  return RootOperationBase(WhenDynamicCastTo<const OperationType&>(matcher));
}

// Match the `getChildren` method of an `Operation`.
CPP_template(typename... ChildArgs)(
    requires(...&& ql::concepts::same_as<QetMatcher,
                                         ChildArgs>))  //
    inline Matcher<const qlever::Operation&> children(
        const ChildArgs&... childMatchers) {
  return Property("getChildren", &Operation::getChildren,
                  ElementsAre(Pointee(childMatchers)...));
}

// Return a matcher that test whether a given `QueryExecutionTree` contains a
// `OperationType` operation the children of which match the
// `childMatcher`s. Note that the child matchers are not ordered.
template <typename OperationType>
struct MatchTypeAndUnorderedChildrenImpl {
  CPP_template(typename... ChildArgs)(
      requires(...&& ql::concepts::same_as<QetMatcher, ChildArgs>)) auto
  operator()(const ChildArgs&... childMatchers) const {
    return RootOperation<OperationType>(
        AllOf(Property("getChildren", &Operation::getChildren,
                       UnorderedElementsAre(Pointee(childMatchers)...))));
  }
};

template <typename OperationType>
auto MatchTypeAndUnorderedChildren =
    MatchTypeAndUnorderedChildrenImpl<OperationType>{};

// Similar to `MatchTypeAndUnorderedChildren`, but here the children have to
// appear in exact the correct order.
template <typename OperationType>
struct MatchTypeAndOrderedChildrenImpl {
  CPP_template(typename... ChildArgs)(
      requires(...&& ql::concepts::same_as<QetMatcher, ChildArgs>)) auto
  operator()(const ChildArgs&... childMatchers) const {
    return RootOperation<OperationType>(AllOf(children(childMatchers...)));
  }
};
template <typename OperationType>
auto MatchTypeAndOrderedChildren =
    MatchTypeAndOrderedChildrenImpl<OperationType>{};

/// Return a matcher that checks that a given `QueryExecutionTree` consists
/// of a single `IndexScan` with the given `subject`, `predicate`, and
/// `object`, and that the `ScanType` of this `IndexScan` is any of the
/// given `allowedPermutations`.
constexpr auto IndexScan =
    [](TripleComponent subject, TripleComponent predicate,
       TripleComponent object,
       const std::vector<Permutation::Enum>& allowedPermutations = {},
       const IndexScan::Graphs& graphs = IndexScan::Graphs::All(),
       const std::vector<Variable>& additionalVariables = {},
       const std::vector<ColumnIndex>& additionalColumns = {},
       const std::optional<size_t>& strippedSize = std::nullopt,
       const std::optional<std::string>& materializedView =
           std::nullopt) -> QetMatcher {
  // TODO<RobinTF> The matcher should be changed so that numVariables can
  // properly account for stripped columns. Also `strippedSize` should be
  // replaced by an explicit listing of the variables that are kept instead of
  // just the size.
  size_t numVariables = strippedSize.value_or(
      static_cast<size_t>(subject.isVariable()) +
      static_cast<size_t>(predicate.isVariable()) +
      static_cast<size_t>(object.isVariable()) + additionalColumns.size());
  auto permutationMatcher = allowedPermutations.empty()
                                ? ::testing::A<Permutation::Enum>()
                                : AnyOfArray(allowedPermutations);
  Matcher<Permutation> materializedViewMatcher =
      materializedView.has_value()
          ? AD_PROPERTY(Permutation, materializedView,
                        Pointee(AD_PROPERTY(MaterializedView, name,
                                            Eq(materializedView))))
          : AD_PROPERTY(Permutation, materializedView, Eq(nullptr));
  return RootOperation<qlever::IndexScan>(
      AllOf(AD_PROPERTY(
                IndexScan, permutation,
                AllOf(AD_PROPERTY(Permutation, permutation, permutationMatcher),
                      materializedViewMatcher)),
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
  return MatchTypeAndOrderedChildren<qlever::NeutralElementOperation>();
};

constexpr auto TextIndexScanForWord = [](Variable textRecordVar,
                                         std::string word) -> QetMatcher {
  return RootOperation<qlever::TextIndexScanForWord>(
      AllOf(AD_PROPERTY(qlever::TextIndexScanForWord, getResultWidth,
                        Eq(2 + ql::ends_with(word, '*'))),
            AD_PROPERTY(qlever::TextIndexScanForWord, textRecordVar,
                        Eq(textRecordVar)),
            AD_PROPERTY(qlever::TextIndexScanForWord, word, word)));
};

constexpr auto TextIndexScanForWordConf =
    [](TextIndexScanForWordConfiguration conf) -> QetMatcher {
  return RootOperation<qlever::TextIndexScanForWord>(
      AD_PROPERTY(qlever::TextIndexScanForWord, getConfig, conf));
};

// Matcher for the `TextLimit` Operation.
constexpr auto TextLimit =
    [](const size_t n, const QetMatcher& childMatcher,
       const Variable& textRecVar, const std::vector<Variable>& entityVars,
       const std::vector<Variable>& scoreVars) -> QetMatcher {
  return RootOperation<qlever::TextLimit>(AllOf(
      AD_PROPERTY(qlever::TextLimit, getTextLimit, Eq(n)),
      children(childMatcher),
      AD_PROPERTY(qlever::TextLimit, getTextRecordVariable, Eq(textRecVar)),
      AD_PROPERTY(qlever::TextLimit, getEntityVariables,
                  UnorderedElementsAreArray(entityVars)),
      AD_PROPERTY(qlever::TextLimit, getScoreVariables,
                  UnorderedElementsAreArray(scoreVars))));
};

inline auto TextIndexScanForEntity =
    [](Variable textRecordVar, std::variant<Variable, std::string> entity,
       std::string word) -> QetMatcher {
  // TODO: Implement AD_THROWING_PROPERTY(..., Exception matcher) and use it
  // here to test the contract-checks in entityVariable() and fixedEntity().
  if (std::holds_alternative<Variable>(entity)) {
    return RootOperation<qlever::TextIndexScanForEntity>(AllOf(
        AD_PROPERTY(qlever::TextIndexScanForEntity, getResultWidth,
                    Eq(2 + std::holds_alternative<Variable>(entity))),
        AD_PROPERTY(qlever::TextIndexScanForEntity, textRecordVar,
                    Eq(textRecordVar)),
        AD_PROPERTY(qlever::TextIndexScanForEntity, entityVariable,
                    std::get<Variable>(entity)),
        AD_PROPERTY(qlever::TextIndexScanForEntity, word, word),
        AD_PROPERTY(qlever::TextIndexScanForEntity, hasFixedEntity, false)));
  } else {
    return RootOperation<qlever::TextIndexScanForEntity>(AllOf(
        AD_PROPERTY(qlever::TextIndexScanForEntity, getResultWidth,
                    Eq(2 + std::holds_alternative<Variable>(entity))),
        AD_PROPERTY(qlever::TextIndexScanForEntity, textRecordVar,
                    Eq(textRecordVar)),
        AD_PROPERTY(qlever::TextIndexScanForEntity, fixedEntity,
                    std::get<std::string>(entity)),
        AD_PROPERTY(qlever::TextIndexScanForEntity, word, word),
        AD_PROPERTY(qlever::TextIndexScanForEntity, hasFixedEntity, true)));
  }
};

constexpr auto TextIndexScanForEntityConf =
    [](TextIndexScanForEntityConfiguration conf) -> QetMatcher {
  return RootOperation<qlever::TextIndexScanForEntity>(
      AD_PROPERTY(qlever::TextIndexScanForEntity, getConfig, conf));
};

inline auto Bind = [](const QetMatcher& childMatcher,
                      std::string_view expression,
                      Variable target) -> QetMatcher {
  auto innerMatcher =
      AllOf(AD_FIELD(parsedQuery::Bind, _target, Eq(target)),
            AD_FIELD(parsedQuery::Bind, _expression,
                     AD_PROPERTY(sparqlExpression::SparqlExpressionPimpl,
                                 getDescriptor, Eq(expression))));
  return RootOperation<qlever::Bind>(
      AllOf(AD_PROPERTY(qlever::Bind, bind, AllOf(innerMatcher)),
            children(childMatcher)));
};

// Matcher for a `CountAvailablePredicatesMatcher` operation. The case of 0
// children means that it's a full scan.
struct CountAvailablePredicatesMatcher {
  template <
      QL_CONCEPT_OR_TYPENAME(ql::concepts::same_as<QetMatcher>)... ChildArgs>
  auto operator()(size_t subjectColumnIdx, const Variable& predicateVar,
                  const Variable& countVar,
                  const ChildArgs&... childMatchers) const
      QL_CONCEPT_OR_NOTHING(requires(sizeof...(childMatchers) <= 1)) {
    return RootOperation<qlever::CountAvailablePredicates>(
        AllOf(AD_PROPERTY(qlever::CountAvailablePredicates, subjectColumnIndex,
                          Eq(subjectColumnIdx)),
              AD_PROPERTY(qlever::CountAvailablePredicates, predicateVariable,
                          Eq(predicateVar)),
              AD_PROPERTY(qlever::CountAvailablePredicates, countVariable,
                          Eq(countVar)),
              children(childMatchers...)));
  }
};
constexpr inline CountAvailablePredicatesMatcher countAvailablePredicates;

// Class used to indicate only named graphs are allowed when using
// `IndexScanFromStrings`.
class NamedTag {};

// Same as above, but the subject, predicate, and object are passed in as
// strings. The strings are automatically converted a matching
// `TripleComponent`.
inline auto IndexScanFromStrings =
    [](std::string_view subject, std::string_view predicate,
       std::string_view object,
       const std::vector<Permutation::Enum>& allowedPermutations = {},
       const std::variant<std::monostate, NamedTag,
                          ad_utility::HashSet<std::string>>
           graphs = std::monostate{},
       const std::vector<Variable>& additionalVariables = {},
       const std::vector<ColumnIndex>& additionalColumns = {},
       const std::optional<size_t>& strippedSize = std::nullopt,
       const std::optional<std::string>& materializedView =
           std::nullopt) -> QetMatcher {
  auto strToComp = [](std::string_view s) -> TripleComponent {
    if (ql::starts_with(s, "?")) {
      return ::Variable{std::string{s}};
    } else if (ql::starts_with(s, '<')) {
      return TripleComponent::Iri::fromIriref(s);
    }
    return s;
  };

  IndexScan::Graphs graphsOut = IndexScan::Graphs::All();
  if (std::holds_alternative<NamedTag>(graphs)) {
    graphsOut = IndexScan::Graphs::Blacklist(
        TripleComponent{TripleComponent::Iri::fromIriref(DEFAULT_GRAPH_IRI)});
  } else if (std::holds_alternative<ad_utility::HashSet<std::string>>(graphs)) {
    ad_utility::HashSet<TripleComponent> whitelist;
    for (const auto& graphIn :
         std::get<ad_utility::HashSet<std::string>>(graphs)) {
      whitelist.insert(strToComp(graphIn));
    }
    graphsOut = IndexScan::Graphs::Whitelist(std::move(whitelist));
  }
  return IndexScan(strToComp(subject), strToComp(predicate), strToComp(object),
                   allowedPermutations, graphsOut, additionalVariables,
                   additionalColumns, strippedSize, materializedView);
};

// For the following Join algorithms the order of the children is not
// important.
inline auto MultiColumnJoin =
    MatchTypeAndUnorderedChildren<qlever::MultiColumnJoin>;
inline auto Join = MatchTypeAndUnorderedChildren<qlever::Join>;

constexpr auto OptionalJoin = MatchTypeAndOrderedChildren<qlever::OptionalJoin>;

constexpr auto NeutralOptional =
    MatchTypeAndOrderedChildren<qlever::NeutralOptional>;

constexpr auto Minus = MatchTypeAndOrderedChildren<qlever::Minus>;

// Return a matcher that matches a query execution tree that consists of
// multiple JOIN operations that join the `children`. The `INTERNAL SORT BY`
// operations required for the joins are also ignored by this matcher.
inline auto UnorderedJoins = [](auto&&... children) -> QetMatcher {
  using Vec = std::vector<std::reference_wrapper<const QueryExecutionTree>>;
  auto collectChildrenRecursive = [](const QueryExecutionTree& tree,
                                     Vec& children, const auto& self) -> void {
    const Operation* operation = tree.getRootOperation().get();
    auto join = dynamic_cast<const qlever::Join*>(operation);
    auto multiColJoin = dynamic_cast<const qlever::MultiColumnJoin*>(operation);
    // Also allow the INTERNAL SORT BY operations that are needed for the
    // joins.
    // TODO<joka921> is this the right place to also check that those have
    // the correct columns?
    auto sort = dynamic_cast<const qlever::Sort*>(operation);
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
    MatchTypeAndUnorderedChildren<qlever::CartesianProductJoin>;

inline auto TransitivePathSideMatcher = [](TransitivePathSide side) {
  return AllOf(AD_FIELD(TransitivePathSide, value_, Eq(side.value_)),
               AD_FIELD(TransitivePathSide, subCol_, Eq(side.subCol_)),
               AD_FIELD(TransitivePathSide, outputCol_, Eq(side.outputCol_)));
};

// Match a TransitivePath operation
struct TransitivePath {
  template <
      QL_CONCEPT_OR_TYPENAME(ql::concepts::same_as<QetMatcher>)... ChildArgs>
  auto operator()(TransitivePathSide left, TransitivePathSide right,
                  size_t minDist, size_t maxDist,
                  const ChildArgs&... childMatchers) const {
    return RootOperation<qlever::TransitivePathBase>(
        AllOf(children(childMatchers...),
              AD_PROPERTY(TransitivePathBase, getMinDist, Eq(minDist)),
              AD_PROPERTY(TransitivePathBase, getMaxDist, Eq(maxDist)),
              AD_PROPERTY(TransitivePathBase, getLeft,
                          TransitivePathSideMatcher(left)),
              AD_PROPERTY(TransitivePathBase, getRight,
                          TransitivePathSideMatcher(right))));
  }
};
constexpr inline TransitivePath transitivePath;

inline auto PathSearchConfigMatcher = [](PathSearchConfiguration config) {
  auto sourceMatcher =
      AD_FIELD(PathSearchConfiguration, sources_, Eq(config.sources_));
  auto targetMatcher =
      AD_FIELD(PathSearchConfiguration, targets_, Eq(config.targets_));
  return AllOf(
      AD_FIELD(PathSearchConfiguration, algorithm_, Eq(config.algorithm_)),
      sourceMatcher, targetMatcher,
      AD_FIELD(PathSearchConfiguration, start_, Eq(config.start_)),
      AD_FIELD(PathSearchConfiguration, end_, Eq(config.end_)),
      AD_FIELD(PathSearchConfiguration, pathColumn_, Eq(config.pathColumn_)),
      AD_FIELD(PathSearchConfiguration, edgeColumn_, Eq(config.edgeColumn_)),
      AD_FIELD(PathSearchConfiguration, edgeProperties_,
               UnorderedElementsAreArray(config.edgeProperties_)));
};

// Match a PathSearch operation
struct PathSearch {
  template <
      QL_CONCEPT_OR_TYPENAME(ql::concepts::same_as<QetMatcher>)... ChildArgs>
  auto operator()(PathSearchConfiguration config, bool sourceBound,
                  bool targetBound, const ChildArgs&... childMatchers) const {
    return RootOperation<qlever::PathSearch>(
        AllOf(children(childMatchers...),
              AD_PROPERTY(qlever::PathSearch, getConfig,
                          PathSearchConfigMatcher(config)),
              AD_PROPERTY(qlever::PathSearch, isSourceBound, Eq(sourceBound)),
              AD_PROPERTY(qlever::PathSearch, isTargetBound, Eq(targetBound))));
  }
};
constexpr inline PathSearch pathSearch;

inline auto ValuesClause = [](std::string cacheKey) {
  return RootOperation<qlever::Values>(
      AllOf(AD_PROPERTY(Values, getCacheKey, cacheKey)));
};

// Match a SpatialJoin operation, set arguments to ignore to -1
template <bool Substitute = false>
struct SpatialJoinMatcher {
  template <
      QL_CONCEPT_OR_TYPENAME(ql::concepts::same_as<QetMatcher>)... ChildArgs>
  auto operator()(double maxDist, size_t maxResults, Variable left,
                  Variable right, std::optional<Variable> distanceVariable,
                  PayloadVariables payloadVariables,
                  SpatialJoinAlgorithm algorithm,
                  std::optional<SpatialJoinType> joinType,
                  const ChildArgs&... childMatchers) const {
    return RootOperation<qlever::SpatialJoin>(AllOf(
        children(childMatchers...),
        AD_PROPERTY(qlever::SpatialJoin, onlyForTestingGetTask,
                    Pair(DoubleNear(maxDist, 0.01), Eq(maxResults))),
        AD_PROPERTY(qlever::SpatialJoin, onlyForTestingGetVariables,
                    Eq(std::pair(left, right))),
        AD_PROPERTY(qlever::SpatialJoin, onlyForTestingGetDistanceVariable,
                    Eq(distanceVariable)),
        AD_PROPERTY(qlever::SpatialJoin, onlyForTestingGetPayloadVariables,
                    Eq(payloadVariables)),
        AD_PROPERTY(qlever::SpatialJoin, getAlgorithm, Eq(algorithm)),
        AD_PROPERTY(qlever::SpatialJoin, getJoinType, Eq(joinType)),
        AD_PROPERTY(qlever::SpatialJoin, getSubstitutesFilterOp,
                    Eq(Substitute))));
  }
};
constexpr inline SpatialJoinMatcher spatialJoin;
constexpr inline SpatialJoinMatcher<true> spatialJoinFilterSubstitute;

// Match a GroupBy operation
static constexpr auto GroupBy =
    [](const std::vector<Variable>& groupByVariables,
       const std::vector<std::string>& aliases,
       const QetMatcher& childMatcher) -> QetMatcher {
  // TODO<joka921> Also test the aliases.
  auto aliasesToStrings = [](const std::vector<Alias>& aliases) {
    std::vector<std::string> result;
    ql::ranges::transform(aliases, std::back_inserter(result),
                          &Alias::getDescriptor);
    return result;
  };

  return RootOperation<qlever::GroupBy>(
      AllOf(children(childMatcher),
            AD_PROPERTY(qlever::GroupBy, groupByVariables,
                        UnorderedElementsAreArray(groupByVariables)),
            AD_PROPERTY(qlever::GroupBy, aliases,
                        ResultOf(aliasesToStrings, ContainerEq(aliases)))));
};

// Match a sort operation. Currently, this is only required by the binary
// search version of the transitive path operation. This matcher checks only
// the children of the sort operation.
inline auto Sort = MatchTypeAndUnorderedChildren<qlever::Sort>;

// Match a `Filter` operation. The matching of the expression is currently
// only done via the descriptor.
constexpr auto Filter = [](std::string_view descriptor,
                           const QetMatcher& childMatcher) {
  return RootOperation<qlever::Filter>(AllOf(
      children(childMatcher),
      AD_PROPERTY(qlever::Operation, getDescriptor, HasSubstr(descriptor))));
};

// Match an `OrderBy` operation
constexpr auto OrderBy =
    [](const qlever::OrderBy::SortedVariables& sortedVariables,
       const QetMatcher& childMatcher) {
      return RootOperation<qlever::OrderBy>(
          AllOf(children(childMatcher),
                AD_PROPERTY(qlever::OrderBy, getSortedVariables,
                            Eq(sortedVariables))));
    };

// Match a `UNION` operation.
constexpr auto Union = MatchTypeAndOrderedChildren<qlever::Union>;

// Match a `DISTINCT` operation.
constexpr auto Distinct = [](const std::vector<ColumnIndex>& distinctColumns,
                             const QetMatcher& childMatcher) {
  return RootOperation<qlever::Distinct>(
      AllOf(children(childMatcher),
            AD_PROPERTY(qlever::Distinct, getDistinctColumns,
                        UnorderedElementsAreArray(distinctColumns))));
};

// Match a `DESCRIBE` operation
inline QetMatcher Describe(
    const Matcher<const parsedQuery::Describe&>& describeMatcher,
    const QetMatcher& childMatcher) {
  return RootOperation<qlever::Describe>(
      AllOf(children(childMatcher),
            AD_PROPERTY(qlever::Describe, getDescribe, describeMatcher)));
}

// Match an `ExistsJoin`
inline QetMatcher ExistsJoin(const QetMatcher& leftChild,
                             const QetMatcher& rightChild) {
  return RootOperation<qlever::ExistsJoin>(
      AllOf(children(leftChild, rightChild)));
}

// Match an `ExplicitIdTableOperation`, but only test its size estimate (which
// is equal to the actual number of rows in the result). More detailed tests for
// this operation can be found in `ExplicitIdTableOperationTest.cpp` and
// `NamedResultCacheTest.cpp`.
inline QetMatcher ExplicitIdTableOperation(size_t sizeEstimate) {
  auto p = AD_PROPERTY(qlever::ExplicitIdTableOperation, sizeEstimate,
                       ::testing::Eq(sizeEstimate));
  return RootOperation<qlever::ExplicitIdTableOperation>(p);
}

//
inline QetMatcher QetWithWarnings(
    const std::vector<std::string>& warningSubstrings,
    const QetMatcher& actualMatcher) {
  auto warningMatchers = ad_utility::transform(
      warningSubstrings,
      [](const std::string& s) { return ::testing::HasSubstr(s); });
  return AllOf(RootOperationBase(
                   AD_PROPERTY(qlever::Operation, collectWarnings,
                               UnorderedElementsAreArray(warningMatchers))),
               actualMatcher);
}

// Matcher for checking stripped variables in a QueryExecutionTree
inline QetMatcher HasStrippedVariables(
    const std::vector<Variable>& expectedStrippedVariables) {
  return AD_PROPERTY(QueryExecutionTree, getStrippedVariables,
                     UnorderedElementsAreArray(expectedStrippedVariables));
}

// Matcher for checking that a QueryExecutionTree has no stripped variables
inline QetMatcher HasNoStrippedVariables() {
  return AD_PROPERTY(QueryExecutionTree, getStrippedVariables, IsEmpty());
}

// Matcher that asserts that a QueryExecutionTree exposes the correct set of
// variables, without checking their correct order.
inline QetMatcher hasVariables(ad_utility::HashSet<std::string> expected) {
  ad_utility::HashSet<Variable> vars;
  for (const auto& var : expected) {
    vars.insert(Variable{var});
  }
  auto getVariables = [](const QueryExecutionTree& qet) {
    ad_utility::HashSet<Variable> res;
    for (const auto& [var, _] : qet.getVariableColumns()) {
      res.insert(var);
    }
    return res;
  };
  return ::testing::ResultOf(getVariables, UnorderedElementsAreArray(vars));
};

// A query planner class mocking the filter substitute generation for testing
// the substitution behavior.
class QueryPlannerWithMockFilterSubstitute : public QueryPlanner {
  using QueryPlanner::QueryPlanner;

  FiltersAndOptionalSubstitutes seedFilterSubstitutes(
      const std::vector<SparqlFilter>& filters) const override {
    FiltersAndOptionalSubstitutes plans;
    plans.reserve(filters.size());

    const auto equalTo =
        ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(
            "equal-to");

    for (const auto& [i, filterExpression] :
         ::ranges::views::enumerate(filters)) {
      using namespace sparqlExpression;
      auto eqExpr = dynamic_cast<const EqualExpression*>(
          filterExpression.expression_.getPimpl());

      // Substitute `?a = ?b` with `?a <equal-to> ?b`
      if (eqExpr != nullptr) {
        auto vars = eqExpr->containedVariables();
        AD_CORRECTNESS_CHECK(vars.size() == 2);

        // Construct index scan
        SparqlTripleSimple triple{{*vars[0]}, {equalTo}, {*vars[1]}};
        SubtreePlan plan{getQec(),
                         std::make_shared<qlever::IndexScan>(
                             getQec(), Permutation::Enum::PSO, triple)};

        // Set marker for included filter
        plan._idsOfIncludedFilters |= 1ull << i;
        plan.containsFilterSubstitute_ = true;

        plans.emplace_back(filterExpression, plan);
      } else {
        plans.emplace_back(filterExpression, std::nullopt);
      }
    }
    return plans;
  };
};

/// Parse the given SPARQL `query`, pass it to a `QueryPlanner` with empty
/// execution context, and return the resulting `QueryExecutionTree`
template <typename QueryPlannerClass = QueryPlanner>
inline QueryExecutionTree parseAndPlan(std::string query,
                                       QueryExecutionContext* qec) {
  static EncodedIriManager ev;
  ParsedQuery pq = SparqlParser::parseQuery(&ev, std::move(query));
  // TODO<joka921> make it impossible to pass `nullptr` here, properly mock
  // a queryExecutionContext.
  auto tree =
      QueryPlannerClass{qec,
                        std::make_shared<ad_utility::CancellationHandle<>>()}
          .createExecutionTree(pq);
  tree.isRoot() = true;
  return tree;
}

// Check that the `QueryExecutionTree` that is obtained by parsing and
// planning the `query` matches the `matcher`. The query planning budget can
// be controlled to choose between the greedy and the dynamic programming
// planner. This function only serves as a common implementation, for the
// actual tests the three functions below should be used.
template <typename QueryPlannerClass = QueryPlanner>
void expectWithGivenBudget(std::string query, auto matcher,
                           std::optional<QueryExecutionContext*> optQec,
                           size_t queryPlanningBudget,
                           source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto budgetBackup =
      getRuntimeParameter<&RuntimeParameters::queryPlanningBudget_>();
  setRuntimeParameter<&RuntimeParameters::queryPlanningBudget_>(
      queryPlanningBudget);
  auto cleanup = absl::Cleanup{[budgetBackup]() {
    setRuntimeParameter<&RuntimeParameters::queryPlanningBudget_>(budgetBackup);
  }};
  auto trace = generateLocationTrace(
      l, absl::StrCat("expect with budget ", queryPlanningBudget));
  // Note: we cannot use `value_or` here, because it eagerly evaluates `getQec`
  // which overwrites the global singleton index. The index is used to check
  // invariants which require the correct index to be set.
  // TODO<joka921>: revert to `value_or` with #2476
  QueryExecutionContext* qec =
      optQec.has_value() ? *optQec : ad_utility::testing::getQec();
  auto qet = parseAndPlan<QueryPlannerClass>(std::move(query), qec);
  qet.getRootOperation()->createRuntimeInfoFromEstimates(
      qet.getRootOperation()->getRuntimeInfoPointer());
  EXPECT_THAT(qet, matcher);
}

// Same as `expectWithGivenBudget` but allows multiple budgets to be tested.
template <typename QueryPlannerClass = QueryPlanner>
void expectWithGivenBudgets(std::string query, auto matcher,
                            std::optional<QueryExecutionContext*> optQec,
                            std::vector<size_t> queryPlanningBudgets,
                            source_location l = AD_CURRENT_SOURCE_LOC()) {
  for (size_t budget : queryPlanningBudgets) {
    expectWithGivenBudget<QueryPlannerClass>(query, matcher, optQec, budget, l);
  }
}

// Same as `expectWithGivenBudget` above, but always use the greedy query
// planner.
template <typename QueryPlannerClass = QueryPlanner>
void expectGreedy(std::string query, auto matcher,
                  std::optional<QueryExecutionContext*> optQec = std::nullopt,
                  source_location l = AD_CURRENT_SOURCE_LOC()) {
  expectWithGivenBudget<QueryPlannerClass>(std::move(query), std::move(matcher),
                                           optQec, 0, l);
}
// Same as `expectWithGivenBudget` above, but always use the dynamic
// programming query planner.
template <typename QueryPlannerClass = QueryPlanner>
void expectDynamicProgramming(
    std::string query, auto matcher,
    std::optional<QueryExecutionContext*> optQec = std::nullopt,
    source_location l = AD_CURRENT_SOURCE_LOC()) {
  expectWithGivenBudget<QueryPlannerClass>(
      std::move(query), std::move(matcher), optQec,
      std::numeric_limits<size_t>::max(), l);
}

// Same as `expectWithGivenBudget` above, but run the test for different
// query planning budgets. This is guaranteed to run with both the greedy
// query planner and the dynamic-programming based query planner.
template <typename QueryPlannerClass = QueryPlanner>
void expect(std::string query, auto matcher,
            std::optional<QueryExecutionContext*> optQec = std::nullopt,
            source_location l = AD_CURRENT_SOURCE_LOC()) {
  expectWithGivenBudgets<QueryPlannerClass>(
      std::move(query), std::move(matcher), std::move(optQec),
      {0, 1, 4, 16, 64'000'000}, l);
}
}  // namespace queryPlannerTestHelpers

#endif  // QLEVER_TEST_QUERYPLANNERTESTHELPERS_H
