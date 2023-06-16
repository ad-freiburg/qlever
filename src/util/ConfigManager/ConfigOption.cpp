// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/ConfigManager/ConfigOption.h"

#include <absl/strings/str_cat.h>

#include <array>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "global/ValueId.h"
#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"

namespace ad_utility {
// ____________________________________________________________________________
std::string ConfigOption::availableTypesToString(const AvailableTypes& value) {
  auto toStringVisitor = []<typename T>(const T&) {
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
    } else {
      // It must be a list of floats.
      AD_CONTRACT_CHECK((std::is_same_v<T, std::vector<float>>));
      return "list of floats";
    }
  };

  return std::visit(toStringVisitor, value);
}

// ____________________________________________________________________________
bool ConfigOption::wasSetAtRuntime() const {
  return configurationOptionWasSet_;
}

// ____________________________________________________________________________
bool ConfigOption::hasDefaultValue() const {
  return std::visit([](const auto& d) { return d.defaultValue_.has_value(); },
                    data_);
}

// ____________________________________________________________________________
bool ConfigOption::hasSetDereferencedVariablePointer() const {
  return wasSetAtRuntime() || hasDefaultValue();
}

// ____________________________________________________________________________
void ConfigOption::setValueWithJson(const nlohmann::json& json) {
  /*
  Manually checks, if the json represents one of the possibilites of
  `AvailableTypes`.
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
    } else {
      // Only the vector type remains.
      AD_CONTRACT_CHECK(ad_utility::isVector<T>);

      return j.is_array() && [&j, &isValueTypeSubType]<typename InnerType>(
                                 const std::vector<InnerType>&) {
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
          [&isValueTypeSubType, &json]<typename T>(const T&) {
            return isValueTypeSubType.operator()<typename T::Type>(
                json, isValueTypeSubType);
          },
          data_)) {
    // Does the json represent one of the types in our `AvailableTypes`? If yes,
    // we can create a better exception message.
    ad_utility::ConstexprForLoop(
        std::make_index_sequence<std::variant_size_v<AvailableTypes>>{},
        [&isValueTypeSubType, &json,
         this ]<size_t TypeIndex,
                typename AlternativeType =
                    std::variant_alternative_t<TypeIndex, AvailableTypes>>() {
          if (isValueTypeSubType.template operator()<AlternativeType>(
                  json, isValueTypeSubType)) {
            throw ConfigOptionSetWrongJsonTypeException(
                identifier_, getActualValueTypeAsString(),
                availableTypesToString(AlternativeType{}));
          }
        });

    throw ConfigOptionSetWrongJsonTypeException(
        identifier_, getActualValueTypeAsString(), "unknown");
  }

  std::visit(
      [&json, this]<typename T>(const Data<T>&) { setValue(json.get<T>()); },
      data_);
  configurationOptionWasSet_ = true;
}

// ____________________________________________________________________________
std::string_view ConfigOption::getIdentifier() const {
  return static_cast<std::string_view>(identifier_);
}

// ____________________________________________________________________________
std::string ConfigOption::contentOfAvailableTypesToString(
    const std::optional<AvailableTypes>& value) {
  if (value.has_value()) {
    // Converts a `AvailableTypes` to their string representation.
    auto availableTypesToString = []<typename T>(
                                      const T& content,
                                      auto&& variantSubTypetoString) {
      // Return the internal value of the `std::optional`.
      if constexpr (std::is_same_v<T, std::string>) {
        // Add "", so that it's more obvious, that it's a string.
        return absl::StrCat("\"", content, "\"");
      } else if constexpr (std::is_same_v<T, bool>) {
        return content ? std::string{"true"} : std::string{"false"};
      } else if constexpr (std::is_arithmetic_v<T>) {
        return std::to_string(content);
      } else if constexpr (ad_utility::isVector<T>) {
        std::ostringstream stream;
        stream << "{";
        forEachExcludingTheLastOne(
            content,
            [&stream, &variantSubTypetoString](const auto& entry) {
              stream << variantSubTypetoString(entry, variantSubTypetoString)
                     << ", ";
            },
            [&stream, &variantSubTypetoString](const auto& entry) {
              stream << variantSubTypetoString(entry, variantSubTypetoString);
            });
        stream << "}";
        return stream.str();
      } else {
        // A possible alternative has no conversion.
        AD_CONTRACT_CHECK(false);
      }
    };

    return std::visit(
        [&availableTypesToString](const auto& v) {
          return availableTypesToString(v, availableTypesToString);
        },
        value.value());
  } else {
    return "None";
  }
}
// ____________________________________________________________________________
std::string ConfigOption::getValueAsString() const {
  return std::visit(
      [](const auto& d) {
        return contentOfAvailableTypesToString(*d.variablePointer_);
      },
      data_);
}

// ____________________________________________________________________________
nlohmann::json ConfigOption::getValueAsJson() const {
  return std::visit(
      [](const auto& d) { return nlohmann::json(*d.variablePointer_); }, data_);
}

// ____________________________________________________________________________
std::string ConfigOption::getDefaultValueAsString() const {
  return std::visit(
      [](const auto& d) {
        return d.defaultValue_.has_value()
                   ? contentOfAvailableTypesToString(d.defaultValue_.value())
                   : contentOfAvailableTypesToString(std::nullopt);
      },
      data_);
}

// ____________________________________________________________________________
nlohmann::json ConfigOption::getDefaultValueAsJson() const {
  return std::visit(
      [](const auto& d) {
        return d.defaultValue_.has_value()
                   ? nlohmann::json(d.defaultValue_.value())
                   : nlohmann::json(nlohmann::json::value_t::null);
      },
      data_);
}

// ____________________________________________________________________________
nlohmann::json ConfigOption::getDummyValueAsJson() const {
  return std::visit(
      []<typename T>(const Data<T>&) {
        if constexpr (std::is_same_v<T, bool>) {
          return nlohmann::json(false);
        } else if constexpr (std::is_same_v<T, std::string>) {
          return nlohmann::json("Example string");
        } else if constexpr (std::is_same_v<T, int>) {
          return nlohmann::json(42);
        } else if constexpr (std::is_same_v<T, float>) {
          return nlohmann::json(4.2);
        } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
          return nlohmann::json(std::vector{true, false});
        } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
          return nlohmann::json(std::vector{"Example", "string", "list"});
        } else if constexpr (std::is_same_v<T, std::vector<int>>) {
          return nlohmann::json(std::vector{40, 41, 42});
        } else {
          // Must be a vector of floats.
          AD_CONTRACT_CHECK((std::is_same_v<T, std::vector<float>>));
          return nlohmann::json(std::vector{40.0, 41.1, 42.2});
        }
      },
      data_);
}

// ____________________________________________________________________________
std::string ConfigOption::getDummyValueAsString() const {
  return std::visit(
      /*
      We could directly return a string, but by converting a value, we don't
      have to keep an eye on how the class represents it's values as strings.
      */
      []<typename T>(const Data<T>&) {
        if constexpr (std::is_same_v<T, bool>) {
          return contentOfAvailableTypesToString(false);
        } else if constexpr (std::is_same_v<T, std::string>) {
          return contentOfAvailableTypesToString("Example string");
        } else if constexpr (std::is_same_v<T, int>) {
          return contentOfAvailableTypesToString(42);
        } else if constexpr (std::is_same_v<T, float>) {
          return contentOfAvailableTypesToString(static_cast<float>(4.2));
        } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
          return contentOfAvailableTypesToString(std::vector{true, false});
        } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
          return contentOfAvailableTypesToString(
              std::vector<std::string>{"Example", "string", "list"});
        } else if constexpr (std::is_same_v<T, std::vector<int>>) {
          return contentOfAvailableTypesToString(std::vector{40, 41, 42});
        } else {
          // Must be a vector of floats.
          AD_CONTRACT_CHECK((std::is_same_v<T, std::vector<float>>));
          return contentOfAvailableTypesToString(
              std::vector{static_cast<float>(40.0), static_cast<float>(41.1),
                          static_cast<float>(42.2)});
        }
      },
      data_);
}

// ____________________________________________________________________________
ConfigOption::operator std::string() const {
  return absl::StrCat(
      "Configuration option '", identifier_, "'\n",
      ad_utility::addIndentation(
          absl::StrCat("Value type: ", getActualValueTypeAsString(),
                       "\nDefault value: ", getDefaultValueAsString(),
                       "\nCurrently held value: ", getValueAsString(),
                       "\nDescription: ", description_),
          1));
}

// ____________________________________________________________________________
auto ConfigOption::getActualValueType() const -> size_t {
  return data_.index();
}

// ____________________________________________________________________________
std::string ConfigOption::getActualValueTypeAsString() const {
  return std::visit(
      [](const auto& d) { return availableTypesToString(*d.variablePointer_); },
      data_);
}
}  // namespace ad_utility
