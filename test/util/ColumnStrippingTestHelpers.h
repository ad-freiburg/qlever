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

#include <gmock/gmock.h>

#include <algorithm>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

namespace column_stripping_test_helpers {

// Check whether root operation of tree is of the expected operation type.
template <typename Operation>
auto hasRootOperation() {
  return ::testing::Truly([](const QueryExecutionTree* tree) {
    return std::dynamic_pointer_cast<Operation>(tree->getRootOperation()) !=
           nullptr;
  });
}

// Check whether the stripped columns contain the expected variables.
inline auto hasVariables(std::vector<Variable> expectedVariables) {
  return ::testing::Truly([expectedVariables = std::move(expectedVariables)](
                              const QueryExecutionTree* tree) {
    const auto& columns = tree->getVariableColumns();
    return columns.size() == expectedVariables.size() &&
           std::ranges::all_of(expectedVariables, [&](const auto& variable) {
             return columns.contains(variable);
           });
  });
}

// Check whether the stripped table contains the expected row content.
template <typename ResultMatcher>
auto hasExpectedResult(ResultMatcher resultMatcher) {
  return ::testing::ResultOf(
      [](const QueryExecutionTree* tree) {
        return tree->getResult(false)->idTableView();
      },
      std::move(resultMatcher));
}

// Combines `hasRootOperation`, `hasVariables` and `hasExpectedResult` in one
// check.
template <typename Operation, typename ResultMatcher>
auto isStrippedColumns(std::vector<Variable> expectedVariables,
                       ResultMatcher resultMatcher) {
  return ::testing::AllOf(hasRootOperation<Operation>(),
                          hasVariables(std::move(expectedVariables)),
                          hasExpectedResult(std::move(resultMatcher)));
}
}  // namespace column_stripping_test_helpers

#endif  // QLEVER_TEST_UTIL_COLUMNSTRIPPINGTESTHELPERS_H
