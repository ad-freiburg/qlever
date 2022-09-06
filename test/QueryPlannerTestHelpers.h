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

template <typename OperationType>
auto RootOperation(auto matcher) {
  auto getRootOperation =
      [](const QueryExecutionTree& tree) -> const ::Operation& {
    return *tree.getRootOperation().get();
  };
  return ResultOf(getRootOperation,
                  WhenDynamicCastTo<const OperationType&>(matcher));
}

auto IndexScan(TripleComponent subject, std::string predicate,
               TripleComponent object,
               std::vector<IndexScan::ScanType> types = {}) {
  auto typeMatcher =
      types.empty() ? A<IndexScan::ScanType>() : AnyOfArray(types);
  return RootOperation<::IndexScan>(
      AllOf(Property(&IndexScan::getType, typeMatcher),
            Property(&IndexScan::getSubject, Eq(subject)),
            Property(&IndexScan::getPredicate, Eq(predicate)),
            Property(&IndexScan::getObject, Eq(object))));
}

QueryExecutionTree parseAndPlan(std::string query) {
  ParsedQuery pq = SparqlParser{std::move(query)}.parse();
  pq.expandPrefixes();
  // TODO<joka921> make it impossible to pass `nullptr` here, properly mock a
  // queryExecutionContext.
  return QueryPlanner{nullptr}.createExecutionTree(pq);
}

// TODO<joka921> use `source_location` as soon as qup42's PR is integrated...
void expect(std::string query, auto matcher) {
  auto qet = parseAndPlan(std::move(query));
  EXPECT_THAT(qet, matcher);
}
}  // namespace queryPlannerTestHelpers