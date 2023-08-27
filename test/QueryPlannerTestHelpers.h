//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "./util/GTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/MultiColumnJoin.h"
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

/// Return a matcher that checks that a given `QueryExecutionTree` consists of a
/// single `IndexScan` with the given `subject`, `predicate`, and `object`, and
/// that the `ScanType` of this `IndexScan` is any of the given
/// `allowedPermutations`.
QetMatcher IndexScan(
    TripleComponent subject, TripleComponent predicate, TripleComponent object,
    const std::vector<Permutation::Enum>& allowedPermutations = {}) {
  size_t numVariables = static_cast<size_t>(subject.isVariable()) +
                        static_cast<size_t>(predicate.isVariable()) +
                        static_cast<size_t>(object.isVariable());
  auto permutationMatcher = allowedPermutations.empty()
                                ? ::testing::A<Permutation::Enum>()
                                : AnyOfArray(allowedPermutations);
  return RootOperation<::IndexScan>(
      AllOf(Property(&IndexScan::permutation, permutationMatcher),
            Property(&IndexScan::getResultWidth, Eq(numVariables)),
            Property(&IndexScan::getSubject, Eq(subject)),
            Property(&IndexScan::getPredicate, Eq(predicate)),
            Property(&IndexScan::getObject, Eq(object))));
}

// Return a matcher that test whether a given `QueryExecutionTree` contains a
// `MultiColumnJoin` operation the two children of which match the
// `childMatcher`s. Note that the child matchers are not ordered, so
// `childMatcher1` might refer to the left or right child of the
// `MultiColumnJoin`, and `childMatcher2` always refers to the other child.
QetMatcher MultiColumnJoin(const QetMatcher& childMatcher1,
                           const QetMatcher& childMatcher2) {
  return RootOperation<::MultiColumnJoin>(AllOf(Property(
      &Operation::getChildren,
      UnorderedElementsAre(Pointee(childMatcher1), Pointee(childMatcher2)))));
}

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
