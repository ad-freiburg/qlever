// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>

#include <variant>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "../benchmark/infrastructure/BenchmarkResultToString.h"
#include "BenchmarkConfigurationOption.h"
#include "util/ConstexprUtils.h"

namespace ad_benchmark {
// ____________________________________________________________________________
bool BenchmarkConfigurationOption::hasValue() const {
  // We only have `std::monostate`, if the value wasn't set.
  return !std::holds_alternative<std::monostate>(value_);
}

// ____________________________________________________________________________
void BenchmarkConfigurationOption::setValueWithJson(
    const nlohmann::json& json) {
  /*
  Interpreting a json value requires a compile time value, that specifies the
  return type. We can find out our type using `type_` and
  `std::variant_alternative`, but that isn't compile time, because `type_` is
  bound to a non constexpr class. However, because we only have a finite
  amount of types, that we want to interpret the json as, we can simply cheat
  with `ad_utility::RuntimeValueToCompileTimeValue`.
  */
  ad_utility::RuntimeValueToCompileTimeValue<std::variant_size_v<ValueType> -
                                             1>(
      type_, [&json, this]<size_t index>() {
        value_ = json.get<std::variant_alternative_t<index, ValueType>>();
      });
}

// ____________________________________________________________________________
std::string_view BenchmarkConfigurationOption::getIdentifier() const {
  return static_cast<std::string_view>(identifier_);
}

// ____________________________________________________________________________
BenchmarkConfigurationOption::operator std::string() const {
  return absl::StrCat(
      "Benchmark configuration option '", identifier_, "'\n",
      addIndentation(absl::StrCat("Value type: ", typesForValueToString(type_),
                                  "\nDescription: ", description_),
                     1));
}

}  // namespace ad_benchmark
