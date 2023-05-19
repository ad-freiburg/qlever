// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <bits/ranges_algo.h>

#include <sstream>
#include <string>
#include <type_traits>
#include <variant>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "../benchmark/infrastructure/BenchmarkToString.h"
#include "BenchmarkConfigurationOption.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"

namespace ad_benchmark {
// ____________________________________________________________________________
BenchmarkConfigurationOption::BenchmarkConfigurationOption(
    std::string_view identifier, std::string_view description,
    const ValueTypeIndexes& type, const ValueType& defaultValue)
    : identifier_{identifier},
      description_{description},
      type_{type},
      value_{defaultValue},
      defaultValue_{defaultValue} {
  // The `identifier` must be a string unlike `""`.
  AD_CONTRACT_CHECK(identifier != "");

  /*
  Is the default value of the right type? `std::monostate` is always alright,
  because it signifies, that we have no default value.
  */
  if (type != defaultValue.index() &&
      !std::holds_alternative<std::monostate>(defaultValue)) {
    throw ad_utility::Exception(absl::StrCat(
        "Error while constructing configuraion option: Configuration option '",
        identifier, "' was given a default value of type '",
        typesForValueToString(defaultValue.index()),
        "', but the configuration option was set to only ever hold values of "
        "type '",
        typesForValueToString(type), "'."));
  }
}

// ____________________________________________________________________________
bool BenchmarkConfigurationOption::wasSetAtRuntime() const {
  return configurationOptionWasSet_;
}

// ____________________________________________________________________________
bool BenchmarkConfigurationOption::hasDefaultValue() const {
  // We only have `std::monostate`, if no default value was given.
  return !std::holds_alternative<std::monostate>(defaultValue_);
}

// ____________________________________________________________________________
bool BenchmarkConfigurationOption::hasValue() const {
  return wasSetAtRuntime() || hasDefaultValue();
}

// ____________________________________________________________________________
void BenchmarkConfigurationOption::setValueWithJson(
    const nlohmann::json& json) {
  // Manually checks, if the json represents one of the `ValueTupeIndexes`.
  auto isValueTypeIndex = [](const nlohmann::json& json,
                             const ValueTypeIndexes& index,
                             auto&& isValueTypeIndex) -> bool {
    switch (index) {
      case boolean:
        return json.is_boolean();
        break;
      case string:
        return json.is_string();
        break;
      case integer:
        return json.is_number_integer();
        break;
      case floatingPoint:
        return json.is_number_float();
        break;
      case booleanList:
      case stringList:
      case integerList:
      case floatingPointList:
        return json.is_array() &&
               std::ranges::all_of(
                   json, [&index, &isValueTypeIndex](const auto& entry) {
                     return isValueTypeIndex(
                         entry, static_cast<ValueTypeIndexes>(index - 4),
                         AD_FWD(isValueTypeIndex));
                   });
        break;
      default:
        return false;
    };
  };

  /*
  Check: Does the json, that we got, actually represent the type of value, this
  option is meant to hold?
  */
  if (!isValueTypeIndex(json, type_, isValueTypeIndex)) {
    // The less and more detailed exception share the same beginning in their
    // error message.
    const std::string& commonPrefix{absl::StrCat(
        "The type of value, that configuration option '", identifier_,
        "' can hold, is '", typesForValueToString(type_),
        "'. The given json however represents a value of ")};

    // Does the json represent one of the types in our `ValueType`? If yes, we
    // can create a better exception message.
    for (size_t index = ValueTypeIndexes::boolean;
         index <= ValueTypeIndexes::floatingPointList; index++) {
      if (isValueTypeIndex(json, static_cast<ValueTypeIndexes>(index),
                           isValueTypeIndex)) {
        throw ad_utility::Exception(absl::StrCat(
            commonPrefix, " type '", typesForValueToString(index), "'."));
      }
    }

    // We have no idea, what the type the json value is describing. At least, we
    // don't have enough information to name it.
    throw ad_utility::Exception(absl::StrCat(commonPrefix, " unknown type."));
  }

  /*
  Interpreting a json value requires a compile time value, that specifies
  the return type. We can find out our type using `type_` and
  `std::variant_alternative`, but that isn't compile time, because `type_`
  is bound to a non constexpr class. However, because we only have a finite
  amount of types, that we want to interpret the json as, we can simply
  cheat with `ad_utility::RuntimeValueToCompileTimeValue`.
  */
  ad_utility::RuntimeValueToCompileTimeValue<std::variant_size_v<ValueType> -
                                             1>(
      type_, [&json, this]<size_t index>() {
        value_ = json.get<std::variant_alternative_t<index, ValueType>>();
      });
  configurationOptionWasSet_ = true;
}

// ____________________________________________________________________________
std::string_view BenchmarkConfigurationOption::getIdentifier() const {
  return static_cast<std::string_view>(identifier_);
}

// ____________________________________________________________________________
BenchmarkConfigurationOption::operator std::string() const {
  // Converts a type in `ValueType` to their string representation.
  auto variantSubTypeToString =
      []<typename T>(const T& val,
                     auto&& variantSubTypetoString) -> std::string {
    if constexpr (std::is_same_v<T, std::string>) {
      return val;
    } else if constexpr (std::is_same_v<T, bool>) {
      return val ? "true" : "false";
    } else if constexpr (std::is_arithmetic_v<T>) {
      return std::to_string(val);
    } else if constexpr (ad_utility::isVector<T>) {
      std::ostringstream stream;
      stream << "{";
      forEachExcludingTheLastOne(
          val,
          [&stream, &variantSubTypetoString](const auto& val) {
            stream << variantSubTypetoString(val, variantSubTypetoString)
                   << ", ";
          },
          [&stream, &variantSubTypetoString](const auto& val) {
            stream << variantSubTypetoString(val, variantSubTypetoString);
          });
      stream << "}";
      return stream.str();
    } else {
      // Should be a `std::monostate`.
      return "None";
    }
  };

  // Converts an `ValueType` to it's string representation.
  auto variantToString =
      [&variantSubTypeToString](const ValueType& val) -> std::string {
    return std::visit(
        [&variantSubTypeToString](const auto& val) {
          return variantSubTypeToString(val, variantSubTypeToString);
        },
        val);
  };

  // The actual string representation.
  return absl::StrCat(
      "Benchmark configuration option '", identifier_, "'\n",
      addIndentation(
          absl::StrCat("Value type: ", typesForValueToString(type_),
                       "\nDefault value: ", variantToString(defaultValue_),
                       "\nCurrently held value: ", variantToString(value_),
                       "\nDescription: ", description_),
          1));
}

// ____________________________________________________________________________
auto BenchmarkConfigurationOption::getActualValueType() const
    -> ValueTypeIndexes {
  return type_;
}

}  // namespace ad_benchmark
