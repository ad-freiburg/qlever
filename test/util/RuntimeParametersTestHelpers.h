//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/cleanup/cleanup.h>

#include "global/RuntimeParameters.h"

template <ad_utility::ParameterName Name, typename Value>
[[nodiscard]] auto setRuntimeParameterForTest(Value&& value) {
  auto originalValue = RuntimeParameters().get<Name>();
  RuntimeParameters().set<Name>(AD_FWD(value));
  return absl::Cleanup{[originalValue = std::move(originalValue)]() {
    RuntimeParameters().set<Name>(originalValue);
  }};
}
