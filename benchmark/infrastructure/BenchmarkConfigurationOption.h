// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>

#include <any>
#include <concepts>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <variant>

#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_benchmark {

// Describes a configuration option.
class BenchmarkConfigurationOption {
 public:
  // The type of value, that can be held by this option.
  using ValueType = std::variant<std::monostate, bool, std::string, int, double,
                                 std::vector<bool>, std::vector<std::string>,
                                 std::vector<int>, std::vector<double>>;
  enum ValueTypeIndexes {
    boolean = 1,
    string,
    integer,
    floatingPoint,
    booleanList,
    stringList,
    integerList,
    floatingPointList
  };

 private:
  // The name of the configuration option.
  const std::string identifier_;

  // Describes what the option does. Would also be a good place th write out the
  // default value, if there is one.
  const std::string description_;

  // The type of value, that is held by this option.
  const ValueTypeIndexes type_;

  // What this configuration option was set to. Can be empty.
  ValueType value_;

  // Converts the index of `Valuetype` into their string representation.
  static std::string typesForValueToString(const size_t& value) {
    constexpr std::string_view indexToString[]{
        "std::monostate",  "boolean",          "string",
        "integer",         "double",           "list of booleans",
        "list of strings", "list of integers", "list of doubles"};

    return std::string{indexToString[value]};
  }

 public:
  /*
  @brief Create a configuration option, whose internal value can only be set to
  values of a specific type in a set of types.

  @param identifier The name of the configuration option, with which it can be
  identified later.
  @param description Describes, what the configuration option stands for. For
  example: "The amount of rows in the table. Has a default value of 3."
  @param type The index nummer for the type of value, that you want to save
  here. Managed per enum.
  @param defaultValue The default value, if the option isn't set at runtime.
  `std::monostate` counts as no default value.
  */
  BenchmarkConfigurationOption(std::string_view identifier,
                               std::string_view description,
                               const ValueTypeIndexes& type,
                               const ValueType& defaultValue = std::monostate{})
      : identifier_{identifier},
        description_{description},
        type_{type},
        value_{defaultValue} {
    // The `identifier` must be a string unlike `""`.
    AD_CONTRACT_CHECK(identifier != "");
  }

  /*
  Does the configuration option hold a value?
  */
  bool hasValue() const;

  /*
  @brief Sets the value held by the configuration option. Throws an exception,
  should the given value have a different type, than what the configuration
  option was set to.
  */
  void setValue(const ValueType& value) {
    // Only set our value, if the given value is of the right type.
    if (type_ == value.index()) {
      value_ = value;
    } else {
      throw ad_utility::Exception(
          absl::StrCat("The type of the value in configuration option '",
                       identifier_, "' is '", typesForValueToString(type_),
                       "'. It can't be set to a value of type '",
                       typesForValueToString(value.index()), "'."));
    }
  }

  /*
  @brief Interprets the value in the json as the type of the value, that this
  configuration option is meant to hold, and sets the internal value to it.
  */
  void setValueWithJson(const nlohmann::json& json);

  /*
  @brief Return the content of the value held by the configuration option. If
  there is no value, or `T` is the wrong type, then it will throw an exception.
  */
  template <typename T>
  requires ad_utility::isTypeContainedIn<T, ValueType>
  std::decay_t<T> getValue() const {
    if (std::holds_alternative<std::decay_t<T>>(value_)) {
      return std::get<std::decay_t<T>>(value_);
    } else if (!hasValue()) {
      // The value was never set.
      throw ad_utility::Exception(
          absl::StrCat("The value in configuration option '", identifier_,
                       "' was never set."));
    } else {
      // The index value of the type `T` in our variant.
      // TODO Take a look, if there is a better alternative, than just copying
      // the types held by std variant.
      constexpr size_t index = ad_utility::getIndexOfFirstTypeToPassCheck<
          []<typename D>() { return std::is_same_v<D, std::decay_t<T>>; },
          std::monostate, bool, std::string, int, double, std::vector<bool>,
          std::vector<std::string>, std::vector<int>, std::vector<double>>();

      // They used the wrong type.
      throw ad_utility::Exception(absl::StrCat(
          "The type of the value in configuration option '", identifier_,
          "' is '", typesForValueToString(type_), "'. It can't be cast as '",
          typesForValueToString(index), "'."));
    }
  }

  // For printing.
  explicit operator std::string() const;

  // Get the identifier for this option.
  std::string_view getIdentifier() const;
};
}  // namespace ad_benchmark
