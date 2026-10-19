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

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

// Matcher to test column stripping.
// It checks the number of columns, expected Variables
// and table content.
/*MATCHER_P2(HasExpectedTableResult, expectedVariables, expectedResult, "") {
  const auto& columns = arg->getVariableColumns();
  if (columns.size() != expectedVariables.size()) {
    *result_listener << "expected " << expectedVariables.size()
                     << " variables, but got " << columns.size();
    return false;
  }
  for (const auto& variable : expectedVariables) {
    if (!columns.contains(variable)) {
      *result_listener << "an expected variable is missing";
      return false;
    }
  }
  if (arg->getResult(false)->idTableView() != expectedResult.get()) {
    *result_listener << "the result table does not match";
    return false;
  }
  return true;
}*/

template <typename Operation>
class IsStrippedColumnsMatcher {
 private:
  std::vector<Variable> expectedVariables_;
  std::reference_wrapper<const IdTable> expectedResult_;

 public:
  IsStrippedColumnsMatcher(std::vector<Variable> expectedVariables,
                           std::reference_wrapper<const IdTable> expectedResult)
      : expectedVariables_{std::move(expectedVariables)},
        expectedResult_{expectedResult} {}

  bool MatchAndExplain(const QueryExecutionTree* tree,
                       ::testing::MatchResultListener* result_listener) const {
    auto operation =
        std::dynamic_pointer_cast<Operation>(tree->getRootOperation());
    if (!operation) {
      *result_listener << "root operation has the wrong type";
      return false;
    }

    const auto& columns = tree->getVariableColumns();
    if (columns.size() != expectedVariables_.size()) {
      *result_listener << "expected " << expectedVariables_.size()
                       << " variables, but got " << columns.size();
      return false;
    }

    for (const auto& variable : expectedVariables_) {
      if (!columns.contains(variable)) {
        *result_listener << "an expected variable is missing";
        return false;
      }
    }

    if (tree->getResult(false)->idTableView() != expectedResult_.get()) {
      *result_listener << "the result table does not match";
      return false;
    }

    return true;
  }

  void DescribeTo(std::ostream* os) const {
    *os << "has the expected operation type, variables, and result table";
  }

  void DescribeNegationTo(std::ostream* os) const {
    *os << "does not have the expected operation type, variables, "
           "and result table";
  }
};

template <typename Operation>
auto isStrippedColumns(std::vector<Variable> expectedVariables,
                       std::reference_wrapper<const IdTable> expectedResult) {
  return ::testing::MakePolymorphicMatcher(IsStrippedColumnsMatcher<Operation>{
      std::move(expectedVariables), expectedResult});
}

#endif  // QLEVER_TEST_UTIL_COLUMNSTRIPPINGTESTHELPERS_H
