// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>

#include <any>
#include <concepts>
#include <optional>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <variant>

#include "global/ValueId.h"
#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_utility {
// Forward declaration, so that `makeConfigOption` can be friend
// of the class.
class ConfigOption;

template <typename T>
ConfigOption makeConfigOption(
    std::string_view identifier, std::string_view description, T* variablePointer,
    const std::optional<T>& defaultValue = std::optional<T>(std::nullopt));

/*
Describes a configuration option. A configuration option can only hold/parse/set values of a
specific type, decided when creating the object.
*/
class ConfigOption {
 public:
  // All possible types, that an option can hold.
  using AvailableTypes =
      std::variant<bool, std::string, int, float, std::vector<bool>, std::vector<std::string>,
                   std::vector<int>, std::vector<float>>;

 private:
  /*
  Holds the type dependant data of the class, because the class is not a templated class, but
  sometimes behaves like one.
  */
  template <typename T>
  struct Data {
    // The type, that `Data` is using.
    using Type = T;

    // What this configuration option was set to. Can be empty.
    std::optional<T> value_;

    // The default value of the configuration option, if there is one.
    std::optional<T> defaultValue_;

    /*
    The variable, this points to, will be overwritten with `value_`, everytime `value_` is set. This
    should allow for easier access of the configuration option value.
    */
    T* variablePointer_;
  };
  using Datatype = LiftedVariant<AvailableTypes, Data>;
  Datatype data_;

  // The name of the configuration option.
  const std::string identifier_;

  // Describes what the option does. Would also be a good place th write out the
  // default value, if there is one.
  const std::string description_;

  // Has this option been set at runtime? Any `set` function will set this to
  // true, if they are used.
  bool configurationOptionWasSet_ = false;

 public:
  // No consturctor. Must be created using `makeConfigOption`.
  ConfigOption() = delete;

  // Was the configuration option set to a value at runtime?
  bool wasSetAtRuntime() const;

  /*
  Does the configuration option hold a default value?
  */
  bool hasDefaultValue() const;

  /*
  @brief Does the configuration option hold a value, regardless, if it's the
  default value, or a value given at runtime?
  */
  bool hasValue() const;

  /*
  @brief Sets the value held by the configuration option. Throws an exception,
  should the given value have a different type, than what the configuration
  option was set to.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes> void setValue(const T& value) {
    // Only set our value, if the given value is of the right type.
    if (getActualValueType() == getIndexOfTypeInAvailableTypes<T>()) {
      std::visit(
          [&value]<typename D>(Data<D>& d) {
            if constexpr (std::is_same_v<T, D>) {
              d.value_ = value;
            }
          },
          data_);
      updateVariablePointer();
      configurationOptionWasSet_ = true;
    } else {
      throw ConfigOptionSetWrongTypeException(identifier_, getActualValueTypeAsString(),
                                              availableTypesToString(value));
    }
  }

  /*
  @brief Interprets the value in the json as the type of the value, that this
  configuration option is meant to hold, and sets the internal value to it.
  */
  void setValueWithJson(const nlohmann::json& json);

  /*
  @brief Return the default value of the the configuration option. If
  there is no default value, or `T` is the wrong type, then it will throw an
  exception.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes>
  std::decay_t<T> getDefaultValue() const {
    if (hasDefaultValue() && std::holds_alternative<Data<T>>(data_)) {
      return std::get<Data<T>>(data_).defaultValue_.value();
    } else if (!hasDefaultValue()) {
      throw ConfigOptionValueNotSetException(identifier_, "default value");
    } else {
      // They used the wrong type.
      throw ConfigOptionGetWrongTypeException(identifier_, getActualValueTypeAsString(),
                                              availableTypesToString<T>());
    }
  }

  /*
  @brief Return string representation of the default value, if it was set. Otherwise, `None` will be
  returned.
  */
  std::string getDefaultValueAsString() const;

  /*
  @brief Return json representation of the default value. Can be empty.
  */
  nlohmann::json getDefaultValueAsJson() const;

  /*
  @brief Return the content of the value held by the configuration option. If
  there is no value, or `T` is the wrong type, then it will throw an exception.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes> T getValue() const {
    if (hasValue() && std::holds_alternative<Data<T>>(data_)) {
      return std::get<Data<T>>(data_).value_.value();
    } else if (!hasValue()) {
      // The value was never set.
      throw ConfigOptionValueNotSetException(identifier_, "held value");
    } else {
      // They used the wrong type.
      throw ConfigOptionGetWrongTypeException(identifier_, getActualValueTypeAsString(),
                                              availableTypesToString<T>());
    }
  }

  /*
  @brief Return string representation of the held value, if it was set. Otherwise, `None` will be
  returned.
  */
  std::string getValueAsString() const;

  /*
  @brief Return json representation of a dummy value, that is of the same type, as the type, this
  configuration option can hold.
  */
  nlohmann::json getDummyValueAsJson() const;

  // For printing.
  explicit operator std::string() const;

  // Get the identifier for this option.
  std::string_view getIdentifier() const;

  /*
  @brief Returns the index of the variant in `ValueType`, that this configuration option holds.
  */
  size_t getActualValueType() const;

  /*
  @brief Returns the string representation of the current value type.
  */
  std::string getActualValueTypeAsString() const;

  // Default move and copy constructor.
  ConfigOption(ConfigOption&&) noexcept = default;
  ConfigOption(const ConfigOption&) = default;

 private:
  /*
  @brief Create a configuration option, whose internal value can only be set to
  values of a specific type in a set of types.

  @tparam T The value, this configuration holds.

  @param identifier The name of the configuration option, with which it can be
  identified later.
  @param description Describes, what the configuration option stands for. For
  example: "The amount of rows in the table. Has a default value of 3."
  @param type The index number for the type of value, that you want to save
  here. See `ValueType`.
  @param variablePointer The variable, this pointer points to, will always be overwritten with the
  value in this configuration option, when the values changes.
  @param defaultValue The default value, if the option isn't set at runtime. An
  empty `std::optional` of the right type signifies, that there is no default
  value.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, ConfigOption::AvailableTypes>
  ConfigOption(std::string_view identifier, std::string_view description, const size_t& type,
               T* variablePointer, const std::optional<T>& defaultValue)
      : data_{Data<T>{defaultValue, defaultValue, variablePointer}},
        identifier_{identifier},
        description_{description} {
    // The `identifier` must be a valid `NAME` in the short hand for
    // configurations.
    if (!isNameInShortHand(identifier)) {
      throw NotValidShortHandNameException(identifier,
                                           "identifier parameter of ConfigOption constructor");
    }

    // Is `type` the right number for `T`?
    if (type != getIndexOfTypeInAvailableTypes<T>()) {
      // Finding out, what type, the number of `type` stands for.
      std::string typeString;
      ad_utility::RuntimeValueToCompileTimeValue<std::variant_size_v<AvailableTypes> - 1>(
          type, [&typeString]<size_t index,
                              typename Type = std::variant_alternative_t<index, AvailableTypes>>() {
            typeString = availableTypesToString<Type>();
          });

      throw ConfigOptionSetWrongTypeException(identifier_, typeString,
                                              getActualValueTypeAsString());
    }

    // If `defaultValue` has a default value, than `value_` has changed.
    if (defaultValue.has_value()) {
      updateVariablePointer();
    }
  }

  /*
  @brief Writes the currently held value to the variable, that `variablePointer_` points to. Doesn't
  do anything, if the configuration option doesn't hold a value.
  */
  void updateVariablePointer() const;
  /*
  @brief Return the string representation/name of the type, of the currently held alternative.
  */
  static std::string availableTypesToString(const AvailableTypes& value);

  /*
  @brief Return the string representation/name of the type.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes>
  static std::string availableTypesToString() {
    return availableTypesToString(T{});
  }

  /*
  @brief Return string representation of values, whose type is in `AvailableTypes`. If no value is
  given, returns `None`.
  */
  static std::string contentOfAvailableTypesToString(const std::optional<AvailableTypes>& v);

  /*
  @brief Returns the index position of a type in `AvailableTypes`.
  Example: A call with `T = int` would return `2`.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes>
  static constexpr size_t getIndexOfTypeInAvailableTypes() {
    // In order to get the types inside a `std::variant`, we have to cheat a bit.
    auto getIndex = []<typename... Ts>(const std::variant<Ts...>&) {
      return ad_utility::getIndexOfFirstTypeToPassCheck<
          []<typename D>() { return std::is_same_v<D, T>; }, Ts...>();
    };
    return getIndex(AvailableTypes{});
  }

  /*
  @brief Create a configuration option.

  @tparam T The type of value, that the configuration option can hold.

  @param identifier The name of the configuration option, with which it can be
  identified later.
  @param description Describes, what the configuration option stands for. For
  example: "The amount of rows in the table. Has a default value of 3."
  @param variablePointer The variable, this pointer points to, will always be overwritten with the
  value in this configuration option, when the values changes.
  @param defaultValue The default value, if the option isn't set at runtime. An
  empty `std::optional<T>` signifies, that there is no default value.
  */
  template <typename T>
  friend ConfigOption makeConfigOption(std::string_view identifier, std::string_view description,
                                       T* variablePointer, const std::optional<T>& defaultValue) {
    return ConfigOption(identifier, description, ConfigOption::getIndexOfTypeInAvailableTypes<T>(),
                        variablePointer, defaultValue);
  }
};

}  // namespace ad_utility
