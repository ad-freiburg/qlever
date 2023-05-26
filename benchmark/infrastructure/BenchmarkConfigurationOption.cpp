// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <bits/ranges_algo.h>

#include <array>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
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
std::string BenchmarkConfigurationOption::valueTypeToString(const ValueType& value) {
  auto toStringVisitor = []<typename T>(const std::optional<T>&) {
    if constexpr (std::is_same_v<T, bool>) {
      return "boolean";
    } else if constexpr (std::is_same_v<T, std::string>) {
      return "string";
    } else if constexpr (std::is_same_v<T, int>) {
      return "integer";
    } else if constexpr (std::is_same_v<T, float>) {
      return "float";
    } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
      return "list of booleans";
    } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
      return "list of strings";
    } else if constexpr (std::is_same_v<T, std::vector<int>>) {
      return "list of integers";
    } else if constexpr (std::is_same_v<T, std::vector<float>>) {
      return "list of floats";
    }
  };

  return std::visit(toStringVisitor, value);
}

// ____________________________________________________________________________
auto BenchmarkConfigurationOption::createValueTypeWithEmptyOptional(const size_t& typeIndex)
    -> ValueType {
  ValueType toReturn;

  /*
  Set `toReturn` to a an empty `std::optional` of the type, that is at index position `typeIndex` in
  `ValueType`.
  */
  ad_utility::RuntimeValueToCompileTimeValue<std::variant_size_v<ValueType> - 1>(
      typeIndex,
      [&toReturn]<size_t index, typename Type = std::variant_alternative_t<index, ValueType>>() {
        toReturn = Type(std::nullopt);
      });

  return toReturn;
}

// ____________________________________________________________________________
bool BenchmarkConfigurationOption::wasSetAtRuntime() const { return configurationOptionWasSet_; }

// ____________________________________________________________________________
bool BenchmarkConfigurationOption::hasDefaultValue() const {
  return std::visit([](const auto& optional) { return optional.has_value(); }, defaultValue_);
}

// ____________________________________________________________________________
bool BenchmarkConfigurationOption::hasValue() const {
  return wasSetAtRuntime() || hasDefaultValue();
}

// ____________________________________________________________________________
void BenchmarkConfigurationOption::setValueWithJson(const nlohmann::json& json) {
  /*
  Manually checks, if the json represents one of the possibilites of
  `ValueType`, without the `std::optional`.
  */
  auto isValueTypeSubType = []<typename T>(const nlohmann::json& j,
                                           auto& isValueTypeSubType) -> bool {
    if constexpr (std::is_same_v<T, bool>) {
      return j.is_boolean();
    } else if constexpr (std::is_same_v<T, std::string>) {
      return j.is_string();
    } else if constexpr (std::is_same_v<T, int>) {
      return j.is_number_integer();
    } else if constexpr (std::is_same_v<T, float>) {
      return j.is_number_float();
    } else if constexpr (ad_utility::isVector<T>) {
      return j.is_array() &&
             [&j, &isValueTypeSubType]<typename InnerType>(const std::vector<InnerType>&) {
               return std::ranges::all_of(j, [&isValueTypeSubType](const auto& entry) {
                 return isValueTypeSubType.template operator()<InnerType>(
                     entry, AD_FWD(isValueTypeSubType));
               });
             }(T{});
    }
  };

  /*
  Check: Does the json, that we got, actually represent the type of value, this
  option is meant to hold?
  */
  if (!std::visit(
          [&isValueTypeSubType,
           &json]<typename TypeInsideOptional>(const std::optional<TypeInsideOptional>&) {
            return isValueTypeSubType.operator()<TypeInsideOptional>(json, isValueTypeSubType);
          },
          value_)) {
    // The less and more detailed exception share the same beginning in their
    // error message.
    const std::string& commonPrefix{absl::StrCat(
        "The type of value, that configuration option '", identifier_, "' can hold, is '",
        valueTypeToString(value_), "'. The given json however represents a value of ")};

    // Does the json represent one of the types in our `ValueType`? If yes, we
    // can create a better exception message.
    ad_utility::ConstexprForLoop(
        std::make_index_sequence<std::variant_size_v<ValueType>>{},
        [&isValueTypeSubType, &json, &
         commonPrefix ]<size_t TypeIndex, typename TypeOptional =
                                              std::variant_alternative_t<TypeIndex, ValueType>>() {
          if (isValueTypeSubType.template operator()<typename TypeOptional::value_type>(
                  json, isValueTypeSubType)) {
            throw ad_utility::Exception(absl::StrCat(
                commonPrefix, " type '", valueTypeToString(TypeOptional(std::nullopt)), "'."));
          }
        });

    // We have no idea, what the type the json value is describing. At least, we
    // don't have enough information to name it.
    throw ad_utility::Exception(absl::StrCat(commonPrefix, " unknown type."));
  }

  std::visit([&json, this]<typename T>(
                 const std::optional<T>&) { value_ = std::optional<T>{json.get<T>()}; },
             value_);
  configurationOptionWasSet_ = true;
}

// ____________________________________________________________________________
std::string_view BenchmarkConfigurationOption::getIdentifier() const {
  return static_cast<std::string_view>(identifier_);
}

// ____________________________________________________________________________
BenchmarkConfigurationOption::operator std::string() const {
  return absl::StrCat(
      "Benchmark configuration option '", identifier_, "'\n",
      addIndentation(
          absl::StrCat(
              "Value type: ", valueTypeToString(value_),
              "\nDefault value: ", benchmarkConfigurationOptionValueTypeToString(defaultValue_),
              "\nCurrently held value: ", benchmarkConfigurationOptionValueTypeToString(value_),
              "\nDescription: ", description_),
          1));
}

// ____________________________________________________________________________
auto BenchmarkConfigurationOption::getActualValueType() const -> size_t { return type_; }

}  // namespace ad_benchmark
