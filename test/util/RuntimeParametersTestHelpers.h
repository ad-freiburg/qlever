//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H
#define QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H

#include <absl/cleanup/cleanup.h>

#include "global/RuntimeParameters.h"

// Set a runtime parameter to a specific value for the duration of the current
// scope. The original value is restored when the scope is left.
template <typename ParameterName, typename Value>
[[nodiscard]] auto setRuntimeParameterForTest(Value&& value) {
  auto originalValue = RuntimeParameters().get<ParameterName>();
  RuntimeParameters().set<ParameterName>(AD_FWD(value));
  return absl::Cleanup{[originalValue = std::move(originalValue)]() {
    RuntimeParameters().set<ParameterName>(originalValue);
  }};
}

#endif  // QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H
