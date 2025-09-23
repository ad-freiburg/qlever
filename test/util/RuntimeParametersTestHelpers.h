//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H
#define QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H

#include <absl/cleanup/cleanup.h>

#include "global/RuntimeParameters.h"

template <auto ParameterPtr, typename Value>
[[nodiscard]] auto setNewRuntimeParameterForTest(Value&& value) {
  auto originalValue =
      std::invoke(ParameterPtr, *getRuntimeParameters().rlock()).get();
  std::invoke(ParameterPtr, *getRuntimeParameters().wlock()).set(AD_FWD(value));
  return absl::Cleanup{[originalValue = std::move(originalValue)]() {
    std::invoke(ParameterPtr, *getRuntimeParameters().wlock())
        .set(std::move(originalValue));
  }};
}

#endif  // QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H
