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

#include "util/Exception.h"
#include "util/json.h"

namespace ad_benchmark {

/*
@brief Sets the given `std::any` to the interpreted value of the json.

@tparam T What to interpret the json as.
*/
template <typename T>
static void setAnyToInterpretedJsonObject(std::any& any,
                                          const nlohmann::json& json) {
  any = json.get<T>();
}

// Describes a configuration option.
class BenchmarkConfigurationOption {
  // The name of the configuration option.
  const std::string identifier_;

  // Describes what the option does. Would also be a good place th write out the
  // default value, if there is one.
  const std::string description_;

  // The type of value, that is held by this option.
  const std::type_info& valueType_;

  // What this configuration option was set to. Can be empty.
  std::any value_;

  // A pointer to a function, that will set `value` using `nlohmann::json`, by
  // interpreting the json object as `optionType`.
  void (*setWithJson_)(std::any&, const nlohmann::json&);

 public:
  /*
  @brief Create a configuration option, whose internal value can only be set to
  values of a specific type. Note: This type must be copy constructible.

  @param possibleDefaultValue Because constructors can't be called with explicit
  template arguments, we pass the type of the value, that you want the
  configuration option to hold, indirectly. If `mustBeSet` is `false`, the
  internal value of the configuration option will be set to
  `possibleDefaultValue`, as a kind of default value. Otherwise, it will be
  discarded.
  @param identifier The name of the configuration option, with which it can be
  identified later.
  @param description Describes, what the configuration option stands for. For
  example: "The amount of rows in the table. Has a default value of 3."
  @param mustBeSet Must this option be set per runtime arguments? If yes, trying
  to read the held value will cause an exception, if it was never set. If no,
  the default value will be used, should somebody try to read the value, when
  the option wasn't set at runtime.
  */
  template <std::copy_constructible T>
  BenchmarkConfigurationOption(const T& possibleDefaultValue,
                               std::string_view identifier,
                               std::string_view description,
                               const bool mustBeSet = false)
      : identifier_(identifier),
        description_(description),
        valueType_(typeid(possibleDefaultValue)),
        setWithJson_(&setAnyToInterpretedJsonObject<T>) {
    // The `identifier` must be a string unlike `""`.
    AD_CONTRACT_CHECK(identifier != "");
    // We only initialize `value_`, if it should have a default value.
    if (!mustBeSet) {
      value_ = possibleDefaultValue;
    }
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
  void setValue(auto value) {
    // Only set our value, if the given value is of the right type.
    if (typeid(value) == valueType_) {
      value_ = value;
    } else {
      throw ad_utility::Exception(
          absl::StrCat("The type of the value in configuration option '",
                       identifier_, "' is '", valueType_.name(),
                       "'. It can't be set to a value of type '",
                       typeid(value).name(), "'."));
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
  template <std::copy_constructible T>
  T getValue() const {
    if (typeid(T) == valueType_ && hasValue()) {
      return std::any_cast<T>(value_);
    } else if (!hasValue()) {
      // The value was never set.
      throw ad_utility::Exception(
          absl::StrCat("The value in configuration option '", identifier_,
                       "' was never set."));
    } else {
      // They used the wrong type.
      throw ad_utility::Exception(
          absl::StrCat("The type of the value in configuration option '",
                       identifier_, "' is '", valueType_.name(),
                       "'. It can't be cast as '", typeid(T).name(), "'."));
    }
  }

  // For printing.
  explicit operator std::string() const;

  // Get the identifier for this option.
  std::string_view getIdentifier() const;
};
}  // namespace ad_benchmark
