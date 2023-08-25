// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <gtest/gtest.h>

#include <any>
#include <concepts>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <variant>
#include <vector>

#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigOptionProxy.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/HashMap.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_utility {

/*
A validator function is any invocable object, who takes the given parameter
types, in the same order and transformed to `const type&`, and returns a
bool.

This concept is used for `ConfigManager::addValidator`.
*/
template <typename Func, typename... ParameterTypes>
concept Validator =
    std::regular_invocable<Func, const ParameterTypes&...> &&
    std::same_as<std::invoke_result_t<Func, const ParameterTypes&...>, bool>;

/*
Manages a bunch of `ConfigOption`s.
*/
class ConfigManager {
  /*
  The added configuration options.

  A configuration option tends to be placed like a key value pair in a json
  object. For example: `{"object 1" : [{"object 2" : { "The configuration option
  identifier" : "Configuration information"} }]}`

  The string key describes their location in the json object literal, by
  representing a json pointer in string form.
  */
  ad_utility::HashMap<std::string, std::unique_ptr<ConfigOption>>
      configurationOptions_;

  /*
  List of the added validators. Whenever the values of the options are set,
  they are called afterwards with `verifyWithValidators` to make sure, that
  the new values are valid.
  If the new value isn't valid, it throws an exception.
  */
  std::vector<std::function<void(void)>> validators_;

 public:
  /*
  @brief Creates and adds a new configuration option without a default value.
  This configuration option must always be set at runtime.

  @tparam OptionType The type of value, the configuration option can hold.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option. The last key in the vector is the name of the
  configuration option.
  @param optionDescription A description for the configuration option.
  @param variableToPutValueOfTheOptionIn The value held by the configuration
  option will be copied into this variable, whenever the value in the
  configuration option changes.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <typename OptionType>
  requires ad_utility::isTypeContainedIn<OptionType,
                                         ConfigOption::AvailableTypes>
  ConstConfigOptionProxy<OptionType> addOption(
      const std::vector<std::string>& pathToOption,
      std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn) {
    return addOptionImpl(pathToOption, optionDescription,
                         variableToPutValueOfTheOptionIn,
                         std::optional<OptionType>(std::nullopt));
  }

  /*
  @brief Creates and adds a new configuration option with a default value.
  Setting this option at runtime is optional and not required.

  @tparam OptionType The type of value, the configuration option can hold.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option. The last key in the vector is the name of the
  configuration option.
  @param optionDescription A description for the configuration option.
  @param variableToPutValueOfTheOptionIn The value held by the configuration
  option will be copied into this variable, whenever the value in the
  configuration option changes.
  @param defaultValue A default value for the configuration option.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <typename OptionType,
            std::same_as<OptionType> DefaultValueType = OptionType>
  requires ad_utility::isTypeContainedIn<OptionType,
                                         ConfigOption::AvailableTypes>
  ConstConfigOptionProxy<OptionType> addOption(
      const std::vector<std::string>& pathToOption,
      std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn,
      DefaultValueType defaultValue) {
    return addOptionImpl(pathToOption, optionDescription,
                         variableToPutValueOfTheOptionIn,
                         std::optional<OptionType>(std::move(defaultValue)));
  }

  /*
  @brief Creates and adds a new configuration option, just like in the other
  `addOption`. But instead of a `pathToOption`, there is only an
  `optionName`, which describes a path only made out of this single string.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <typename OptionType>
  requires ad_utility::isTypeContainedIn<OptionType,
                                         ConfigOption::AvailableTypes>
  ConstConfigOptionProxy<OptionType> addOption(
      std::string optionName, std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn) {
    return addOption<OptionType>(
        std::vector<std::string>{std::move(optionName)}, optionDescription,
        variableToPutValueOfTheOptionIn);
  }

  /*
  @brief Creates and adds a new configuration option, just like in the other
  `addOption`. But instead of a `pathToOption`, there is only an
  `optionName`, which describes a path only made out of this single string.

  @return A reference to the newly created configuration option. This reference
  will stay valid, even after adding more options.
  */
  template <typename OptionType,
            std::same_as<OptionType> DefaultValueType = OptionType>
  requires ad_utility::isTypeContainedIn<OptionType,
                                         ConfigOption::AvailableTypes>
  ConstConfigOptionProxy<OptionType> addOption(
      std::string optionName, std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn,
      DefaultValueType defaultValue) {
    return addOption<OptionType>(
        std::vector<std::string>{std::move(optionName)}, optionDescription,
        variableToPutValueOfTheOptionIn, std::move(defaultValue));
  }

  /*
  @brief Sets the configuration options based on the given json.

  @param j There will be an exception thrown, if:
  - `j` doesn't contain values for all configuration options, that must be set
  at runtime.
  - Same, if there are values for configuration options, that do not exist.
  - `j` is anything but a json object literal.
  - Any of the added validators return false.
  */
  void parseConfig(const nlohmann::json& j);

  /*
  @brief Parses the given short hand and returns it as a json object,
  that contains all the described configuration data.

  @param shortHandString The language of the short hand is defined in
  `generated/ConfigShorthand.g4`. The short hand is a format similar to JSON
  that is more suitable to be set directly from the command line
  */
  static nlohmann::json parseShortHand(const std::string& shortHandString);

  /*
  @brief Returns a string containing a json configuration and the
  string representations of all added configuration options. Followed by a list
  of all the configuration options, that kept their default values.

  @param printCurrentJsonConfiguration If true, the current values of the
  configuration options will be used for printing the json configuration. If
  false, an example json configuration will be printed, that uses the default
  values of the configuration options, and, if an option doesn't have a default
  value, a dummy value.
  */
  std::string printConfigurationDoc(bool printCurrentJsonConfiguration) const;

  /*
  @brief Add a function to the configuration manager for verifying, that the
  values of the given configuration options are valid. Will always be called
  after parsing.

  @tparam ValidatorParameterTypes The types of the values in the
  `validatorParameter`. Can be left up to type deduction.

  @param validatorFunction Checks, if the values of the given configuration
  options are valid. Should return true, if they are valid.
  @param errorMessage A `std::runtime_error` with this as an error message will
  get thrown, if the `validatorFunction` returns false.
  @param configOptionsToBeChecked Proxies for the configuration options, whos
  values will be passed to the validator function as function arguments. Will
  keep the same order.
  */
  template <typename... ValidatorParameterTypes>
  void addValidator(
      Validator<ValidatorParameterTypes...> auto validatorFunction,
      std::string_view errorMessage,
      ConstConfigOptionProxy<
          ValidatorParameterTypes>... configOptionsToBeChecked)
      requires(sizeof...(configOptionsToBeChecked) > 0) {
    addValidatorImpl(
        "addValidator",
        []<typename T>(ConstConfigOptionProxy<T> opt) {
          return opt.getConfigOption().template getValue<std::decay_t<T>>();
        },
        validatorFunction, errorMessage, configOptionsToBeChecked...);
  }

  /*
  @brief Add a function to the configuration manager for verifying, that the
  given configuration options are valid. Will always be called after parsing.

  @param validatorFunction Checks, if the given configuration options are valid.
  Should return true, if they are valid.
  @param errorMessage A `std::runtime_error` with this as an error message will
  get thrown, if the `validatorFunction` returns false.
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the validator function as function arguments. Will keep the
  same order.
  */
  template <typename ValidatorT>
  void addOptionValidator(
      ValidatorT validatorFunction, std::string_view errorMessage,
      isInstantiation<ConstConfigOptionProxy> auto... configOptionsToBeChecked)
      requires(
          sizeof...(configOptionsToBeChecked) > 0 &&
          Validator<ValidatorT,
                    decltype(configOptionsToBeChecked.getConfigOption())...>) {
    addValidatorImpl(
        "addOptionValidator",
        []<typename T>(ConstConfigOptionProxy<T> opt) {
          return opt.getConfigOption();
        },
        validatorFunction, errorMessage, configOptionsToBeChecked...);
  }

 private:
  FRIEND_TEST(ConfigManagerTest, ParseConfig);
  FRIEND_TEST(ConfigManagerTest, ParseConfigExceptionTest);
  FRIEND_TEST(ConfigManagerTest, ParseShortHandTest);
  FRIEND_TEST(ConfigManagerTest, ContainsOption);

  /*
  @brief Creates the string representation of a valid `nlohmann::json` pointer
  based on the given keys.
  */
  static std::string createJsonPointerString(
      const std::vector<std::string>& keys);

  /*
  @brief Verifies, that the given path is a valid path for an option. If not,
  throws exceptions.

  @param pathToOption Describes a path in json.
  */
  void verifyPath(const std::vector<std::string>& path) const;

  /*
  @brief Adds a configuration option, that can be accessed with the given path.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option.

  @return The added config option.
  */
  ConfigOption& addConfigOption(const std::vector<std::string>& pathToOption,
                                ConfigOption&& option);

  /*
  @brief Return string representation of a `std::vector<std::string>`.
  */
  static std::string vectorOfKeysForJsonToString(
      const std::vector<std::string>& keys);

  /*
  @brief Return a string, containing a list of all entries from
  `getListOfNotChangedConfigOptionsWithDefaultValues`, in the form of
  "Configuration option 'x' was not set at runtime, using default value 'y'.".
  */
  std::string getListOfNotChangedConfigOptionsWithDefaultValuesAsString() const;

  /*
  @brief Creates and adds a new configuration option.

  @tparam OptionType The type of value, the configuration option can hold.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option. The last key in the vector is the name of the
  configuration option.
  @param optionDescription A description for the configuration option.
  @param variableToPutValueOfTheOptionIn The value held by the configuration
  option will be copied into this variable, whenever the value in the
  configuration option changes.
  @param defaultValue A default value for the configuration option. If none is
  given, signified by an empty optional, then a value for the configuration
  option MUST be given at runtime.

  @return A reference to the newly created configuration option. Will stay
  valid, even after more options.
  */
  template <typename OptionType>
  requires ad_utility::isTypeContainedIn<OptionType,
                                         ConfigOption::AvailableTypes>
  ConstConfigOptionProxy<OptionType> addOptionImpl(
      const std::vector<std::string>& pathToOption,
      std::string_view optionDescription,
      OptionType* variableToPutValueOfTheOptionIn,
      std::optional<OptionType> defaultValue =
          std::optional<OptionType>(std::nullopt)) {
    verifyPath(pathToOption);

    /*
    The `unqiue_ptr` was created, by creating a new `ConfigOption` via it's
    move constructor. Which is why, we can't just return the `ConfigOption`
    we created here.
    */
    return ConstConfigOptionProxy<OptionType>(addConfigOption(
        pathToOption,
        ConfigOption(pathToOption.back(), optionDescription,
                     variableToPutValueOfTheOptionIn, defaultValue)));
  }

  /*
  @brief Provide a range of tuples, that hold references to the key value pairs
  in `configurationOptions_`, but with the pointer dereferenced.
  */
  auto configurationOptions();
  auto configurationOptions() const;

  /*
  @brief Call all the validators to check, if the current value is valid.
  */
  void verifyWithValidators() const;

  /*
  Checks, if the given configuration option is contained in the manager.
  Note: This checks for identity (pointer equality), not semantic equality.
  */
  bool containsOption(const ConfigOption& opt) const;

  /*
  @brief The implementation of `addValidator` and `addOptionValidator`.

  @param addValidatorFunctionName The name of the function, that is calling this
  implementation. So that people can easier identify them.
  @param translationFunction Transforms the given `ConstConfigOptionProxy` in
  the value form for the validator function.
  @param validatorFunction Should return true, if the transformed
  `ConstConfigOptionProxy` represent valid values. For example: There
  configuration options were all set at run time, or contain numbers bigger
  than 10.
  @param errorMessage A `std::runtime_error` with this as an error message will
  get thrown, if the `validatorFunction` returns false.
  @param configOptionsToBeChecked Proxies for the configuration options, who
  will be passed to the validator function as function arguments, after being
  transformed. Will keep the same order.
  */
  template <typename TranslationFunction, typename ValidatorFunction,
            isInstantiation<ConstConfigOptionProxy>... ValidatorParameter>
  requires(std::invocable<TranslationFunction, const ValidatorParameter> &&
           ...) &&
          Validator<ValidatorFunction,
                    std::invoke_result_t<TranslationFunction,
                                         ValidatorParameter>...> &&
          (sizeof...(ValidatorParameter) > 0)
  void addValidatorImpl(std::string_view addValidatorFunctionName,
                        TranslationFunction translationFunction,
                        ValidatorFunction validatorFunction,
                        std::string_view errorMessage,
                        const ValidatorParameter... configOptionsToBeChecked) {
    // Check, if we contain all the configuration options, that were given us.
    auto checkIfContainOption = [this, &addValidatorFunctionName]<typename T>(
                                    ConstConfigOptionProxy<T> opt) {
      if (!containsOption(opt.getConfigOption())) {
        throw std::runtime_error(absl::StrCat(
            "Error while adding validator with ", addValidatorFunctionName,
            ": The given configuration "
            "option '",
            opt.getConfigOption().getIdentifier(),
            "' is not contained in the configuration manager."));
      }
    };
    (checkIfContainOption(configOptionsToBeChecked), ...);

    // Create the expanded error message.
    const std::array optionNames{absl::StrCat(
        "'", configOptionsToBeChecked.getConfigOption().getIdentifier(),
        "'")...};
    std::string expandedErrorMessage =
        absl::StrCat("Validity check of configuration options ",
                     lazyStrJoin(optionNames, ", "), " failed: ", errorMessage);

    /*
    Add a function wrapper to our list of validators, that calls the
    `validatorFunction` with the config options and throws an exception, if the
    `validatorFunction` returns false.
    */
    validators_.emplace_back([translationFunction, validatorFunction,
                              errorMessage = std::move(expandedErrorMessage),
                              configOptionsToBeChecked...]() {
      if (!std::invoke(
              validatorFunction,
              std::invoke(translationFunction, configOptionsToBeChecked)...)) {
        throw std::runtime_error(errorMessage);
      }
    });
  }
};
}  // namespace ad_utility
