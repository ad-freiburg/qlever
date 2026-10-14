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

// Matcher to test column stripping.
// It checks the number of columns, expected Variables
// and table content.
MATCHER_P2(HasOperationResult, expectedVariables, expectedResult, "") {
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
}

#endif  // QLEVER_TEST_UTIL_COLUMNSTRIPPINGTESTHELPERS_H
