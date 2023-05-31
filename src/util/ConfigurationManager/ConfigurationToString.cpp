// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include "util/Algorithm.h"
#include "util/ConfigurationManager/ConfigurationOption.h"
#include "util/ConfigurationManager/ConfigurationToString.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"

namespace ad_utility {
// ___________________________________________________________________________
std::string benchmarkConfigurationOptionValueTypeToString(
    const ad_benchmark::BenchmarkConfigurationOption::ValueType& val) {
  // Converts a type in `ValueType` to their string representation.
  auto variantSubTypeToString = []<typename T>(
                                    const std::optional<T>& variantEntry,
                                    auto&& variantSubTypetoString) {
    if (!variantEntry.has_value()) {
      // Return a `None`, if the `optional` is empty.
      return std::string{"None"};
    } else {
      // Return the internal value of the `std::optional`.
      if constexpr (std::is_same_v<T, std::string>) {
        // Add "", so that it's more obvious, that it's a string.
        return absl::StrCat("\"", variantEntry.value(), "\"");
      } else if constexpr (std::is_same_v<T, bool>) {
        return variantEntry.value() ? std::string{"true"}
                                    : std::string{"false"};
      } else if constexpr (std::is_arithmetic_v<T>) {
        return std::to_string(variantEntry.value());
      } else if constexpr (ad_utility::isVector<T>) {
        std::ostringstream stream;
        stream << "{";
        forEachExcludingTheLastOne(
            variantEntry.value(),
            [&stream, &variantSubTypetoString](const auto& entry) {
              stream << variantSubTypetoString(std::optional{entry},
                                               variantSubTypetoString)
                     << ", ";
            },
            [&stream, &variantSubTypetoString](const auto& entry) {
              stream << variantSubTypetoString(std::optional{entry},
                                               variantSubTypetoString);
            });
        stream << "}";
        return stream.str();
      } else {
        // A possible variant has no conversion.
        AD_CONTRACT_CHECK(false);
      }
    }
  };

  return std::visit(
      [&variantSubTypeToString](const auto& v) {
        return variantSubTypeToString(v, variantSubTypeToString);
      },
      val);
}

// ___________________________________________________________________________
std::string getDefaultValueBenchmarkConfigurationOptions(
    const ad_benchmark::BenchmarkConfiguration& config) {
  /*
  Because we want to create a list, we don't know how many entries there will be
  and need a string stream.
  */
  std::ostringstream stream;

  // Prints the default value of a configuration option and the accompanying
  // text.
  auto defaultConfigurationOptionToString =
      [](const ad_benchmark::BenchmarkConfigurationOption& option) {
        return absl::StrCat(
            "Configuration option '", option.getIdentifier(),
            "' was not set at runtime, using default value '",
            option.visitDefaultValue([](const auto& opt) {
              return benchmarkConfigurationOptionValueTypeToString(opt);
            }),
            "'.");
      };

  forEachExcludingTheLastOne(
      config.getConfigurationOptions(),
      [&stream, &defaultConfigurationOptionToString](
          const ad_benchmark::BenchmarkConfigurationOption& option) {
        if (option.hasDefaultValue() && !option.wasSetAtRuntime()) {
          stream << defaultConfigurationOptionToString(option) << "\n";
        }
      },
      [&stream, &defaultConfigurationOptionToString](
          const ad_benchmark::BenchmarkConfigurationOption& option) {
        if (option.hasDefaultValue() && !option.wasSetAtRuntime()) {
          stream << defaultConfigurationOptionToString(option);
        }
      });

  return stream.str();
}
}  // namespace ad_utility
