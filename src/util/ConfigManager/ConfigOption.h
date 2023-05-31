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

#include "util/ANTLRLexerHelper.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_benchmark {
// Forward declaration, so that `makeBenchmarkConfigurationOption` can be friend
// of the class.
class BenchmarkConfigurationOption;

template <typename T>
BenchmarkConfigurationOption makeBenchmarkConfigurationOption(
    std::string_view identifier, std::string_view description,
    const std::optional<T>& defaultValue = std::optional<T>(std::nullopt));

// Describes a configuration option.
class BenchmarkConfigurationOption {
  // An `std::variant`, where every entry is a `std::optional`.
  template <typename... Ts>
  using OptionalVariant = std::variant<std::optional<Ts>...>;

 public:
  // The possible types of the value, that can be held by this option.
  using ValueType = OptionalVariant<bool, std::string, int, float, std::vector<bool>,
                                    std::vector<std::string>, std::vector<int>, std::vector<float>>;

 private:
  // The name of the configuration option.
  const std::string identifier_;

  // Describes what the option does. Would also be a good place th write out the
  // default value, if there is one.
  const std::string description_;

  // What this configuration option was set to. Can be empty.
  ValueType value_;

  // Has this option been set at runtime? Any `set` function will set this to
  // true, if they are used.
  bool configurationOptionWasSet_ = false;

  // The default value of the configuration option.
  const ValueType defaultValue_;

  // Converts the index of `Valuetype` into their string representation.
  static std::string valueTypeToString(const ValueType& value);

 public:
  // No consturctor. Must be created using `makeConfigurationOption`.
  BenchmarkConfigurationOption() = delete;

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
  requires ad_utility::isTypeContainedIn<std::optional<T>, ValueType>
  void setValue(const T& value) {
    // Only set our value, if the given value is of the right type.
    if (getActualValueType() == getIndexOfTypeInVariant<T>(value_)) {
      configurationOptionWasSet_ = true;
      value_ = std::optional<T>{value};
    } else {
      throw ad_utility::Exception(absl::StrCat("The type of the value in configuration option '",
                                               identifier_, "' is '", valueTypeToString(value_),
                                               "'. It can't be set to a value of type '",
                                               valueTypeToString(std::optional<T>{}), "'."));
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
  requires ad_utility::isTypeContainedIn<std::optional<T>, ValueType>
  std::decay_t<T> getDefaultValue() const {
    if (hasDefaultValue() && std::holds_alternative<std::optional<T>>(defaultValue_)) {
      return std::get<std::optional<T>>(defaultValue_).value();
    } else if (!hasDefaultValue()) {
      throw ad_utility::Exception(absl::StrCat("Configuration option '", identifier_,
                                               "' was not created with a default value."));
    } else {
      // They used the wrong type.
      throw ad_utility::Exception(absl::StrCat("The type of the value in configuration option '",
                                               identifier_, "' is '", valueTypeToString(value_),
                                               "'. It can't be cast as '",
                                               valueTypeToString(std::optional<T>{}), "'."));
    }
  }

  /*
  @brief Return the content of the value held by the configuration option. If
  there is no value, or `T` is the wrong type, then it will throw an exception.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<std::optional<T>, ValueType> T getValue() const {
    if (hasValue() && std::holds_alternative<std::optional<T>>(value_)) {
      return std::get<std::optional<T>>(value_).value();
    } else if (!hasValue()) {
      // The value was never set.
      throw ad_utility::Exception(
          absl::StrCat("The value in configuration option '", identifier_, "' was never set."));
    } else {
      // They used the wrong type.
      throw ad_utility::Exception(absl::StrCat("The type of the value in configuration option '",
                                               identifier_, "' is '", valueTypeToString(value_),
                                               "'. It can't be cast as '",
                                               valueTypeToString(std::optional<T>{}), "'."));
    }
  }

  // For printing.
  explicit operator std::string() const;

  // Get the identifier for this option.
  std::string_view getIdentifier() const;

  /*
  @brief Returns the index of the variant in `ValueType`, that this configuration option holds..
  */
  size_t getActualValueType() const;

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
  BenchmarkConfigurationOption(BenchmarkConfigurationOption&&) noexcept = default;
  BenchmarkConfigurationOption(const BenchmarkConfigurationOption&) = default;

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
  @param defaultValue The default value, if the option isn't set at runtime. An
  empty `std::optional` of the right type signifies, that there is no default
  value.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<std::optional<T>, BenchmarkConfigurationOption::ValueType>
  BenchmarkConfigurationOption(std::string_view identifier, std::string_view description,
                               const size_t& type, const std::optional<T>& defaultValue)
      : identifier_{identifier},
        description_{description},
        value_(defaultValue),
        defaultValue_(defaultValue) {
    // The `identifier` must be a valid `NAME` in the short hand for
    // configurations.
    if (!stringOnlyContainsSpecifiedTokens<ConfigShorthandLexer>(
            identifier, static_cast<size_t>(ConfigShorthandLexer::NAME))) {
      throw ad_utility::Exception(
          absl::StrCat("Error while constructing configuraion option: The identifier must be "
                       "a 'NAME' in the configuration short hand grammar, which '",
                       identifier, "' doesn't fullfil."));
    }

    /*
    Is the default value of the right type? An empty `std::optional` signifies, that we don't have a
    default value, but it still must be of the right variant in `ValueType`.
    */
    if (type != getIndexOfTypeInVariant<T>(ValueType{})) {
      /*
      Finding out, what type the option should have. After all, our `value_` is set to the wrong
      variant, because it just contains `defaultValue`, so we can't use `valueTypeToString()`.
      */
      std::string valueType;
      ad_utility::RuntimeValueToCompileTimeValue<std::variant_size_v<ValueType> - 1>(
          type, [&valueType]<size_t index, typename TypeOptional =
                                               std::variant_alternative_t<index, ValueType>>() {
            valueType = valueTypeToString(TypeOptional{});
          });

      throw ad_utility::Exception(absl::StrCat(
          "Error while constructing configuraion option: Configuration option "
          "'",
          identifier, "' was given a default value of type '", valueTypeToString(defaultValue),
          "', but the configuration option was set to only ever hold values of "
          "type '",
          valueType,
          "'. This rule must also be followed, if there is no default value and an empty "
          "'std::optional' was given."));
    }
  }

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
  @brief Create a benchmark configuration option.

  @tparam T The type of value, that the configuration option can hold.

  @param identifier The name of the configuration option, with which it can be
  identified later.
  @param description Describes, what the configuration option stands for. For
  example: "The amount of rows in the table. Has a default value of 3."
  @param defaultValue The default value, if the option isn't set at runtime. An
  empty `std::optional<T>` signifies, that there is no default value.
  */
  template <typename T>
  friend BenchmarkConfigurationOption makeBenchmarkConfigurationOption(
      std::string_view identifier, std::string_view description,
      const std::optional<T>& defaultValue) {
    /*
    The part for determining the type index is a bit ugly, but there isn't
    really a better way, that is also typesafe and doesn't need any adjusting,
    when changing `BenchmarkConfigurationOption::ValueType`.
    */
    return BenchmarkConfigurationOption(identifier, description,
                                        BenchmarkConfigurationOption::getIndexOfTypeInVariant<T>(
                                            BenchmarkConfigurationOption::ValueType{}),
                                        defaultValue);
  }
};

}  // namespace ad_benchmark
