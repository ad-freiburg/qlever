// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "../benchmark/infrastructure/BenchmarkResultToString.h"
#include "BenchmarkConfigurationOption.h"

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

// ____________________________________________________________________________
BenchmarkConfigurationOption::operator std::string() const {
  return absl::StrCat(
      "Benchmark configuration option '", identifier_, "'\n",
      addIndentation(absl::StrCat("Value type: ", valueType_.name(),
                                  "\nDescription: ", description_),
                     1));
}

}  // namespace ad_benchmark
