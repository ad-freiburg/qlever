// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <gtest/gtest.h>

#include <any>
#include <concepts>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>

#include "util/ConfigManager/ConfigExceptions.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/ValidatorConcept.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/HashMap.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_utility {

/*
Manages a bunch of `ConfigOption`s.
*/
class ConfigManager {
  /*
  The added configuration options. Configuration managers are used by the user
  to describe a json object literal more explicitly.

  A configuration option tends to be placed like a key value pair in a json
  object. For example: `{"object 1" : [{"object 2" : { "The configuration option
  identifier" : "Configuration information"} }]}`

  The string key describes their location in the json object literal, by
  representing a json pointer in string form.
  */
  using HashMapEntry =
      std::unique_ptr<std::variant<ConfigOption, ConfigManager>>;
  ad_utility::HashMap<std::string, HashMapEntry> configurationOptions_;

  /*
  List of the added validators. Whenever the values of the options are set,
  they are called afterwards with `verifyWithValidators` to make sure, that the
  new values are valid.
  */
  std::vector<std::function<bool(void)>> validators_;

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
  ConfigOption& addOption(const std::vector<std::string>& pathToOption,
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
  ConfigOption& addOption(const std::vector<std::string>& pathToOption,
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
  ConfigOption& addOption(std::string optionName,
                          std::string_view optionDescription,
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
  ConfigOption& addOption(std::string optionName,
                          std::string_view optionDescription,
                          OptionType* variableToPutValueOfTheOptionIn,
                          DefaultValueType defaultValue) {
    return addOption<OptionType>(
        std::vector<std::string>{std::move(optionName)}, optionDescription,
        variableToPutValueOfTheOptionIn, std::move(defaultValue));
  }

  /*
  @brief Creates and adds a new configuration manager with a prefix path for
  it's internally held configuration options and managers.

  @param pathToOption Describes a path in json, which will be a prefix to all
  the other paths held in the newly created ConfigManager.

  @return A reference to the newly created configuration manager. This reference
  will stay valid, even after adding more options.
  */
  ConfigManager& addSubManager(const std::vector<std::string>& path);

  /*
  @brief Sets the configuration options based on the given json.

  @param j There will be an exception thrown, if:
  - `j` doesn't contain values for all configuration options, that must be set
  at runtime.
  - If there are values for configuration options, that do not exist.
  - `j` is anything but a json object literal.
  - Any of the validators returns false.
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
  `validatorParameter`. Needed to interpreting them.

  @param validatorFunction Checks, if the values of the given configuration
  options are valid. Should return true, if they are valid.
  @param errorMessage A `std::runtime_error` with this as an error message will
  get thrown, if the `validatorFunction` returns false.
  @param validatorParameter The configuration option, whos values will be passed
  to the validator function as function arguments. Will keep the same order.
  */
  template <typename... ValidatorParameterTypes>
  void addValidator(
      Validator<ValidatorParameterTypes...> auto validatorFunction,
      const std::string& errorMessage,
      const std::same_as<ConfigOption> auto&... validatorParameter)
      requires(sizeof...(validatorParameter) > 0 &&
               sizeof...(ValidatorParameterTypes) ==
                   sizeof...(validatorParameter)) {
    /*
    Add a function wrapper to our list of validators, that calls the
    `validatorFunction` with the needed values and throws an exception, if the
    `validatorFunction` returns false.
    */
    validators_.emplace_back(
        [validatorFunction, errorMessage, &validatorParameter...]() {
          if (!std::invoke(validatorFunction,
                           validatorParameter.template getValue<
                               std::decay_t<ValidatorParameterTypes>>()...)) {
            throw std::runtime_error(errorMessage);
          } else {
            return true;
          }
        });
  }

 private:
  FRIEND_TEST(ConfigManagerTest, ParseConfig);
  FRIEND_TEST(ConfigManagerTest, ParseConfigExceptionTest);
  FRIEND_TEST(ConfigManagerTest, ParseShortHandTest);
  FRIEND_TEST(ConfigManagerTest, CheckForBrokenPaths);

  /*
  @brief Creates the string representation of a valid `nlohmann::json` pointer
  based on the given keys.
  */
  static std::string createJsonPointerString(
      const std::vector<std::string>& keys);

  /*
  @brief Verifies, that the given path is a valid path for an option/manager. If
  not, throws exceptions.

  @param pathToOption Describes a path in json.
  */
  void verifyPath(const std::vector<std::string>& path) const;

  /*
  @brief Adds a configuration option, that can be accessed with the given path.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option.
  */
  void addConfigOption(const std::vector<std::string>& pathToOption,
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
  ConfigOption& addOptionImpl(const std::vector<std::string>& pathToOption,
                              std::string_view optionDescription,
                              OptionType* variableToPutValueOfTheOptionIn,
                              std::optional<OptionType> defaultValue =
                                  std::optional<OptionType>(std::nullopt)) {
    verifyPath(pathToOption);
    addConfigOption(
        pathToOption,
        ConfigOption(pathToOption.back(), optionDescription,
                     variableToPutValueOfTheOptionIn, defaultValue));

    /*
    The `unqiue_ptr` was created, by creating a new `ConfigOption` via it's
    move constructor. Which is why, we can't just return the `ConfigOption`
    we created here.
    */
    return std::get<ConfigOption>(
        *configurationOptions_.at(createJsonPointerString(pathToOption)));
  }

  /*
  @brief A vector to all the configuratio options, held by this manager,
  represented with their json paths and reference to them. Options held by a sub
  manager, are also included with the path to the sub manager as prefix.

  @param pathPrefix This prefix will be added to all configuration option json
  paths, that will be returned.
  */
  std::vector<std::pair<std::string, ConfigOption*>> configurationOptions(
      std::string_view pathPrefix = "");
  std::vector<std::pair<std::string, const ConfigOption*>> configurationOptions(
      std::string_view pathPrefix = "") const;

  /*
  @brief The implementation for `configurationOptions`.

  @tparam ReturnPointe Should be either `ConfigOption*`, or `const
  ConfigOption*`.

  @param pathPrefix This prefix will be added to all configuration option json
  paths, that will be returned.
  */
  template <typename ReturnPointer>
  static std::vector<std::pair<std::string, ReturnPointer>>
  configurationOptionsImpl(auto& configurationOptions,
                           std::string_view pathPrefix = "");

  /*
  @brief Call all the validators to check, if the current value is valid.
  */
  void verifyWithValidators() const;
};
}  // namespace ad_utility
