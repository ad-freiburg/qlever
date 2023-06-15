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
/*
Describes a configuration option. A configuration option can only hold/parse/set
values of a specific type, decided when creating the object.
*/
class ConfigOption {
 public:
  // All possible types, that an option can hold.
  using AvailableTypes =
      std::variant<bool, std::string, int, float, std::vector<bool>,
                   std::vector<std::string>, std::vector<int>,
                   std::vector<float>>;

 private:
  /*
  Holds the type dependant data of the class, because the class is not a
  templated class, but sometimes behaves like one.
  */
  template <typename T>
  struct Data {
    // The type, that `Data` is using.
    using Type = T;

    // The default value of the configuration option, if there is one.
    std::optional<T> defaultValue_;

    /*
    Whenever somebody sets the value of the configuration option, they, in
    actuallity, set the variable, this points to.
    */
    T* variablePointer_;
  };
  using Datatype = LiftedVariant<AvailableTypes, Data>;
  Datatype data_;

  // The name of the configuration option.
  const std::string identifier_;

  // Describes what the option does.
  const std::string description_;

  // Has this option been set at runtime? Any `set` function will set this to
  // true, if they are used.
  bool configurationOptionWasSet_ = false;

 public:
  // No default consturctor.
  ConfigOption() = delete;

  /*
  @brief Was the variable, that the internal pointer points to, ever set by this
  configuration option at run time?
  */
  bool wasSetAtRuntime() const;

  /*
  Does the configuration option hold a default value?
  */
  bool hasDefaultValue() const;

  /*
  @brief Was the variable, that the internal pointer points to, ever set by this
  configuration option? Note: The answer is only yes, if there was a default
  value given at construction, or any setter called.
  */
  bool hasSetDereferencedVariablePointer() const;

  /*
  @brief Sets the variable, that the internal pointer points to. Throws an
  exception, should the given value have a different type, than what the
  configuration option was set to.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes>
  void setValue(const T& value) {
    // Only set the variable, that our internal pointer points to, if the given
    // value is of the right type.
    if (getActualValueType() == getIndexOfTypeInAvailableTypes<T>()) {
      std::visit(
          [&value]<typename D>(Data<D>& d) {
            if constexpr (std::is_same_v<T, D>) {
              *d.variablePointer_ = value;
            }
          },
          data_);
      configurationOptionWasSet_ = true;
    } else {
      throw ConfigOptionSetWrongTypeException(identifier_,
                                              getActualValueTypeAsString(),
                                              availableTypesToString(value));
    }
  }

  /*
  @brief Interprets the value in the json as the type, that this configuration
  option is meant to have, and sets the variable, that our internal variable
  pointer points to, to it.
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
      throw ConfigOptionGetWrongTypeException(identifier_,
                                              getActualValueTypeAsString(),
                                              availableTypesToString<T>());
    }
  }

  /*
  @brief Return string representation of the default value, if it was set.
  Otherwise, `None` will be returned.
  */
  std::string getDefaultValueAsString() const;

  /*
  @brief Return json representation of the default value. Can be empty.
  */
  nlohmann::json getDefaultValueAsJson() const;

  /*
  @brief Return the content of the variable, that the internal pointer points
  to. If `T` is the wrong type, then it will throw an exception.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes> T getValue() const {
    if (std::holds_alternative<Data<T>>(data_)) {
      return *(std::get<Data<T>>(data_).variablePointer_);
    } else {
      // They used the wrong type.
      throw ConfigOptionGetWrongTypeException(identifier_,
                                              getActualValueTypeAsString(),
                                              availableTypesToString<T>());
    }
  }

  /*
  @brief Return string representation of the variable, that the internal pointer
  points to.
  */
  std::string getValueAsString() const;

  /*
  @brief Return json representation of the variable, that the internal pointer
  points to.
  */
  nlohmann::json getValueAsJson() const;

  /*
  @brief Return json representation of a dummy value, that is of the same type,
  as the type, this configuration option was set to.
  */
  nlohmann::json getDummyValueAsJson() const;

  /*
  @brief Return string representation of a dummy value, that is of the same
  type, as the type, this configuration option was set to.
  */
  std::string getDummyValueAsString() const;

  // For printing.
  explicit operator std::string() const;

  // Get the identifier for this option.
  std::string_view getIdentifier() const;

  /*
  @brief Returns the index of the variant in `ValueType`, that this
  configuration option was set to.
  */
  size_t getActualValueType() const;

  /*
  @brief Returns the string representation of the current type.
  */
  std::string getActualValueTypeAsString() const;

  // Default move and copy constructor.
  ConfigOption(ConfigOption&&) noexcept = default;
  ConfigOption(const ConfigOption&) = default;

  /*
  @brief Create a configuration option with a default value, who modifies the
  variable, that the given variable pointer points to, when it's set.

  @tparam T The value, this configuration holds.

  @param identifier The name of the configuration option, with which it can be
  identified later.
  @param description Describes, what the configuration option stands for. For
  example: "The amount of rows in the table. Has a default value of 3."
  @param variablePointer The variable, this pointer points to, will always be
  overwritten by any of the set functions of this class.
  @param defaultValue The default value.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, ConfigOption::AvailableTypes>
  ConfigOption(std::string_view identifier, std::string_view description,
               T* variablePointer, const T& defaultValue)
      : data_{Data<T>{std::optional<T>(defaultValue), variablePointer}},
        identifier_{identifier},
        description_{description} {
    verifyConstructorArguments(identifier, variablePointer);

    *variablePointer = defaultValue;
  }

  /*
  @brief Create a configuration option without a default value, who modifies the
  variable, that the given variable pointer points to, when it's set.

  @tparam T The value, this configuration holds.

  @param identifier The name of the configuration option, with which it can be
  identified later.
  @param description Describes, what the configuration option stands for. For
  example: "The amount of rows in the table. Has a default value of 3."
  @param variablePointer The variable, this pointer points to, will always be
  overwritten by any of the set functions of this class.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, ConfigOption::AvailableTypes>
  ConfigOption(std::string_view identifier, std::string_view description,
               T* variablePointer)
      : data_{Data<T>{std::optional<T>(std::nullopt), variablePointer}},
        identifier_{identifier},
        description_{description} {
    verifyConstructorArguments(identifier, variablePointer);
  }

 private:
  /*
  @brief Verifies, that the constructor arguments are correct. That is, follow
  all those little rules we made for them.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, ConfigOption::AvailableTypes>
  static void verifyConstructorArguments(std::string_view identifier,
                                         T* variablePointer) {
    // The `identifier` must be a valid `NAME` in the configuration short hand.
    if (!isNameInShortHand(identifier)) {
      throw NotValidShortHandNameException(
          identifier, "identifier parameter of ConfigOption constructor");
    }

    // `variablePointer` MUST point to an actual value. `nullptr` isn't allowed.
    if (variablePointer == nullptr) {
      throw ConfigOptionConstructorNullPointerException(identifier);
    }
  }

  /*
  @brief Return the string representation/name of the type, of the currently
  held alternative in the given `value`.
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
  @brief Return string representation of values, whose type is in
  `AvailableTypes`. If no value is given, returns `None`.
  */
  static std::string contentOfAvailableTypesToString(
      const std::optional<AvailableTypes>& v);

  /*
  @brief Returns the index position of a type in `AvailableTypes`.
  Example: A call with `T = int` would return `2`.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, AvailableTypes>
  static constexpr size_t getIndexOfTypeInAvailableTypes() {
    // In order to get the types inside a `std::variant`, we have to cheat a
    // bit.
    auto getIndex = []<typename... Ts>(const std::variant<Ts...>&) {
      return ad_utility::getIndexOfFirstTypeToPassCheck<
          []<typename D>() { return std::is_same_v<D, T>; }, Ts...>();
    };
    return getIndex(AvailableTypes{});
  }
};

}  // namespace ad_utility
