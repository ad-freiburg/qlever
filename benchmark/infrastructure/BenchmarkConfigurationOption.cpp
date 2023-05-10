// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"

namespace ad_benchmark {
// ____________________________________________________________________________
bool BenchmarkConfigurationOption::hasValue() const {
  return value_.has_value();
}

// ____________________________________________________________________________
void BenchmarkConfigurationOption::setValueWithJson(
    const nlohmann::json& json) {
  // We set the value using the function, that we set at the beginning.
  setWithJson_(value_, json);
}

// ____________________________________________________________________________
std::string_view BenchmarkConfigurationOption::getIdentifier() const {
  return static_cast<std::string_view>(identifier_);
}

}  // namespace ad_benchmark
