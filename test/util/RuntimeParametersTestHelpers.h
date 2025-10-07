//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H
#define QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H

#include <absl/cleanup/cleanup.h>

#include "global/RuntimeParameters.h"

// Set the runtime parameter specified by the `ParameterPtr` to the given
// `Value` and return a cleanup that will restore the original value of the
// parameter when it is destroyed.
template <auto ParameterPtr, typename Value>
[[nodiscard]] auto setRuntimeParameterForTest(Value&& value) {
  auto originalValue =
      std::invoke(ParameterPtr, *globalRuntimeParameters.rlock()).get();
  std::invoke(ParameterPtr, *globalRuntimeParameters.wlock())
      .set(AD_FWD(value));
  return absl::Cleanup{[originalValue = std::move(originalValue)]() {
    std::invoke(ParameterPtr, *globalRuntimeParameters.wlock())
        .set(std::move(originalValue));
  }};
}

#endif  // QLEVER_TEST_UTIL_RUNTIMEPARAMETERSTESTHELPERS_H
