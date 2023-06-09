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

// Describes a configuration option.
class ConfigOption {
  // Simply transforms a type into a pointer.
  template <typename T>
  using ToPointer = T*;

 public:
  // All possible types, that an option can hold.
  using AvailableTypes =
      std::variant<bool, std::string, int, float, std::vector<bool>, std::vector<std::string>,
                   std::vector<int>, std::vector<float>>;

  // How the option actually saves the values internally. Is a variant of `std::optional`s.
  using ValueType = LiftedVariant<AvailableTypes, std::optional>;

 private:
  // The possible types of the pointer, that points to the variable, where the value held by this
  // configuration option will be written to.
  using VariablePointerType = LiftedVariant<AvailableTypes, ToPointer>;

  // The name of the configuration option.
  const std::string identifier_;

  // Describes what the option does. Would also be a good place th write out the
  // default value, if there is one.
  const std::string description_;

  // What this configuration option was set to. Can be empty.
  ValueType value_;

  /*
  The variable, this points to, will be overwritten with `value_`, everytime `value_` is set. This
  should allow for easier access of the configuration option value.
  */
  VariablePointerType variablePointer_;

  // Has this option been set at runtime? Any `set` function will set this to
  // true, if they are used.
  bool configurationOptionWasSet_ = false;

  // The default value of the configuration option.
  const ValueType defaultValue_;

  // Converts the index of `Valuetype` into their string representation.
  static std::string valueTypeToString(const ValueType& value);

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
    if (getActualValueType() == getIndexOfTypeInVariant<T>(value_)) {
      configurationOptionWasSet_ = true;
      value_ = std::optional<T>{value};
      updateVariablePointer();
    } else {
      throw ConfigOptionSetWrongTypeException(identifier_, getActualValueTypeAsString(),
                                              valueTypeToString(std::optional<T>{}));
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
    if (hasDefaultValue() && std::holds_alternative<std::optional<T>>(defaultValue_)) {
      return std::get<std::optional<T>>(defaultValue_).value();
    } else if (!hasDefaultValue()) {
      throw ConfigOptionValueNotSetException(identifier_, "default value");
    } else {
      // They used the wrong type.
      throw ConfigOptionGetWrongTypeException(identifier_, valueTypeToString(value_),
                                              valueTypeToString(std::optional<T>{}));
    }
  }

  /*
  @brief Return the content of the value held by the configuration option. If
  there is no value, or `T` is the wrong type, then it will throw an exception.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes> T getValue() const {
    if (hasValue() && std::holds_alternative<std::optional<T>>(value_)) {
      return std::get<std::optional<T>>(value_).value();
    } else if (!hasValue()) {
      // The value was never set.
      throw ConfigOptionValueNotSetException(identifier_, "held value");
    } else {
      // They used the wrong type.
      throw ConfigOptionGetWrongTypeException(identifier_, valueTypeToString(value_),
                                              valueTypeToString(std::optional<T>{}));
    }
  }

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

  /*
  @brief Calls `std::visit` on the contained value of type `ValueType`.
   */
  template <typename VisitorFunction>
  constexpr auto visitValue(VisitorFunction&& vis) const {
    return std::visit(AD_FWD(vis), value_);
  }

  /*
  @brief Calls `std::visit` on the contained default value of type `ValueType`.
   */
  template <typename VisitorFunction>
  constexpr auto visitDefaultValue(VisitorFunction&& vis) const {
    return std::visit(AD_FWD(vis), defaultValue_);
  }

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
  requires ad_utility::isTypeContainedIn<std::optional<T>, ConfigOption::ValueType>
  ConfigOption(std::string_view identifier, std::string_view description, const size_t& type,
               T* variablePointer, const std::optional<T>& defaultValue)
      : identifier_{identifier},
        description_{description},
        value_(defaultValue),
        variablePointer_(variablePointer),
        defaultValue_(defaultValue) {
    // The `identifier` must be a valid `NAME` in the short hand for
    // configurations.
    if (!isNameInShortHand(identifier)) {
      throw NotValidShortHandNameException(identifier,
                                           "identifier parameter of ConfigOption constructor");
    }

    // Is `type` the right number for `T`?
    if (type != getIndexOfTypeInVariant<T>(ValueType{})) {
      // Finding out, what type number of `type` stands for.
      std::string typeString;
      ad_utility::RuntimeValueToCompileTimeValue<std::variant_size_v<ValueType> - 1>(
          type, [&typeString]<size_t index, typename TypeOptional =
                                                std::variant_alternative_t<index, ValueType>>() {
            typeString = valueTypeToString(TypeOptional{});
          });

      throw ConfigOptionSetWrongTypeException(identifier_, typeString, valueTypeToString(value_));
    }

    // If `defaultValue` has a default value, than `value_` has changed.
    if (defaultValue.has_value()) {
      updateVariablePointer();
    }
  }

  /*
  @brief Writes the value of `value_` to the variable, that `variablePointer_` points to. Doesn't do
  anything, if `value_` is empty.
  */
  void updateVariablePointer() const;

  /*
  @brief Returns the index position of a type in `std::variant<std::optional>`
  type.
  Example: For the type `std::variant<std::optional<int>, std::optional<bool>>`
  a call with `T = int` would return `1`.
  */
  template <typename T, typename... Ts>
  requires(std::same_as<std::optional<T>, Ts> || ...)
  static constexpr size_t getIndexOfTypeInVariant(const std::variant<Ts...>&) {
    return ad_utility::getIndexOfFirstTypeToPassCheck<
        []<typename D>() { return std::is_same_v<D, std::optional<T>>; }, Ts...>();
  }

  /*
  @brief Create an instance of `ValueType`, set to an empty `std::optional`, of
  the alternative type specified by `typeIndex`.
  */
  static ValueType createValueTypeWithEmptyOptional(const size_t& typeIndex);

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
    /*
    The part for determining the type index is a bit ugly, but there isn't
    really a better way, that is also typesafe and doesn't need any adjusting,
    when changing `ConfigOption::ValueType`.
    */
    return ConfigOption(identifier, description,
                        ConfigOption::getIndexOfTypeInVariant<T>(ConfigOption::ValueType{}),
                        variablePointer, defaultValue);
  }
};

}  // namespace ad_utility
