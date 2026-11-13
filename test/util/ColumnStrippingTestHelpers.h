// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_COLUMNSTRIPPINGTESTHELPERS_H
#define QLEVER_TEST_UTIL_COLUMNSTRIPPINGTESTHELPERS_H

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "backports/algorithm.h"
#include "engine/QueryExecutionTree.h"
#include "engine/operation/Operation.h"
#include "engine/sparqlExpressions/ExistsExpression.h"

namespace column_stripping_test_helpers {

// Match a tree whose root operation is of type `Operation`.
template <typename Operation>
auto hasRootOperation() {
  return ::testing::Truly([](const QueryExecutionTree* tree) {
    return std::dynamic_pointer_cast<Operation>(tree->getRootOperation()) !=
           nullptr;
  });
}

// Match a tree whose variables are exactly `expectedVariables`.
inline auto hasVariables(std::vector<Variable> expectedVariables) {
  return ::testing::Truly([expectedVariables = std::move(expectedVariables)](
                              const QueryExecutionTree* tree) {
    const auto& columns = tree->getVariableColumns();
    return columns.size() == expectedVariables.size() &&
           ql::ranges::all_of(expectedVariables, [&](const auto& variable) {
             return columns.contains(variable);
           });
  });
}

// Match a tree whose fully materialized result matches `resultMatcher`.
template <typename ResultMatcher>
auto hasExpectedResult(ResultMatcher resultMatcher) {
  return ::testing::ResultOf(
      [](const QueryExecutionTree* tree) {
        return tree->getResult(false)->idTableView();
      },
      std::move(resultMatcher));
}

// Combine `hasRootOperation`, `hasVariables` and `hasExpectedResult`.
template <typename Operation, typename ResultMatcher>
auto isStrippedColumns(std::vector<Variable> expectedVariables,
                       ResultMatcher resultMatcher) {
  return ::testing::AllOf(hasRootOperation<Operation>(),
                          hasVariables(std::move(expectedVariables)),
                          hasExpectedResult(std::move(resultMatcher)));
}

// Return the number of operations of type `OperationType` in the tree with the
// root operation `root` (`root` included).
template <typename OperationType>
size_t countOperations(const Operation& root) {
  size_t count = dynamic_cast<const OperationType*>(&root) != nullptr ? 1 : 0;
  for (const auto* child : root.getChildren()) {
    count += countOperations<OperationType>(*child->getRootOperation());
  }
  return count;
}

// Overload for a `QueryExecutionTree`.
template <typename OperationType>
size_t countOperations(const QueryExecutionTree& tree) {
  return countOperations<OperationType>(*tree.getRootOperation());
}

// Create the expression `EXISTS { ?a <predicate> ?x }`, which is correlated
// with the variable `?a`. Return the expression and the variable that the
// `ExistsJoin` writes the result of the `EXISTS` to.
inline std::pair<sparqlExpression::SparqlExpressionPimpl, Variable>
makeExistsExpression(const std::string& predicate) {
  ParsedQuery query;
  query._rootGraphPattern._graphPatterns.push_back(
      parsedQuery::BasicGraphPattern{{SparqlTriple{
          TripleComponent{Variable{"?a"}},
          ad_utility::triple_component::Iri::fromIriref(predicate),
          TripleComponent{Variable{"?x"}}}}});
  // Select the visible variables via `*`, exactly as the parser does.
  query.selectClause().setAsterisk();
  query.selectClause().addVisibleVariable(Variable{"?a"});
  auto exists =
      std::make_shared<sparqlExpression::ExistsExpression>(std::move(query));
  Variable existsVar = exists->variable();
  return {
      sparqlExpression::SparqlExpressionPimpl{
          std::move(exists), absl::StrCat("EXISTS { ?a ", predicate, " ?x }")},
      existsVar};
}
}  // namespace column_stripping_test_helpers

#endif  // QLEVER_TEST_UTIL_COLUMNSTRIPPINGTESTHELPERS_H
