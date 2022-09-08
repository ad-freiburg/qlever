//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryPlanner.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock.h"
#include "parser/SparqlParser.h"

namespace queryPlannerTestHelpers {
using namespace ::testing;

/// Returns a matcher that checks that a given `QueryExecutionTree`'s
/// `rootOperation` can by dynamically cast to `OperationType`, and that
/// `matcher` matches the result of this cast.
template <typename OperationType>
auto RootOperation(auto matcher) -> Matcher<const QueryExecutionTree&> {
  auto getRootOperation =
      [](const QueryExecutionTree& tree) -> const ::Operation& {
    return *tree.getRootOperation().get();
  };
  return ResultOf(getRootOperation,
                  WhenDynamicCastTo<const OperationType&>(matcher));
}

/// Return a matcher that checks that a given `QueryExecutionTree` consists of a
/// single `IndexScan` with the given `subject`, `predicate`, and `object`, and
/// that the `ScanType` of this `IndexScan` is any of the given `scanTypes`.
auto IndexScan(TripleComponent subject, std::string predicate,
               TripleComponent object,
               const std::vector<IndexScan::ScanType>& scanTypes = {})
    -> Matcher<const QueryExecutionTree&> {
  auto typeMatcher =
      scanTypes.empty() ? A<IndexScan::ScanType>() : AnyOfArray(scanTypes);
  return RootOperation<::IndexScan>(
      AllOf(Property(&IndexScan::getType, typeMatcher),
            Property(&IndexScan::getSubject, Eq(subject)),
            Property(&IndexScan::getPredicate, Eq(predicate)),
            Property(&IndexScan::getObject, Eq(object))));
}

/// Parse the given SPARQL `query`, pass it to a `QueryPlanner` with empty
/// execution context, and return the resulting `QueryExecutionTree`
QueryExecutionTree parseAndPlan(std::string query) {
  ParsedQuery pq = SparqlParser{std::move(query)}.parse();
  // TODO<joka921> make it impossible to pass `nullptr` here, properly mock a
  // queryExecutionContext.
  return QueryPlanner{nullptr}.createExecutionTree(pq);
}

// TODO<joka921> use `source_location` as soon as qup42's PR is integrated...
// Check that the `QueryExecutionTree` that is obtained by parsing and planning
// the `query` matches the `matcher`.
void expect(std::string query, auto matcher) {
  auto qet = parseAndPlan(std::move(query));
  EXPECT_THAT(qet, matcher);
}
}  // namespace queryPlannerTestHelpers