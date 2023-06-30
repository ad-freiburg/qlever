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
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_utility {

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
  absl::flat_hash_map<std::string, ConfigOption> configurationOptions_;

 public:
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
  */
  template <typename OptionType>
  requires ad_utility::isTypeContainedIn<OptionType,
                                         ConfigOption::AvailableTypes>
  void createConfigOption(const std::vector<std::string>& pathToOption,
                          std::string_view optionDescription,
                          OptionType* variableToPutValueOfTheOptionIn,
                          std::optional<OptionType> defaultValue =
                              std::optional<OptionType>(std::nullopt)) {
    /*
    We need a non-empty path to construct a ConfigOption object, the `verify...`
    function always throws an exception for this case. No need to duplicate the
    error code.
    */
    if (pathToOption.empty()) {
      verifyPathToConfigOption(pathToOption, "");
    }

    addConfigOption(
        pathToOption,
        ConfigOption(pathToOption.back(), optionDescription,
                     variableToPutValueOfTheOptionIn, defaultValue));
  }

  /*
  @brief Creates and adds a new configuration option, just like in the other
  `createConfigOption`. But instead of a `pathToOption`, there is only an
  `optionName`, which describes a path only made out of this single string.
  */
  template <typename OptionType>
  requires ad_utility::isTypeContainedIn<OptionType,
                                         ConfigOption::AvailableTypes>
  void createConfigOption(std::string optionName,
                          std::string_view optionDescription,
                          OptionType* variableToPutValueOfTheOptionIn,
                          std::optional<OptionType> defaultValue =
                              std::optional<OptionType>(std::nullopt)) {
    createConfigOption<OptionType>(
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

 private:
  // For testing.
  FRIEND_TEST(ConfigManagerTest, GetConfigurationOptionByNestedKeysTest);
  FRIEND_TEST(ConfigManagerTest, ParseConfig);
  FRIEND_TEST(ConfigManagerTest, ParseConfigExceptionTest);
  FRIEND_TEST(ConfigManagerTest, ParseShortHandTest);

  /*
  @brief Creates the string representation of a valid `nlohmann::json` pointer
  based on the given keys.
  */
  static std::string createJsonPointerString(
      const std::vector<std::string>& keys);

  /*
  @brief Verifies, that the given path is a valid path for an option, with this
  name. If not, throws exceptions.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option.
  @param optionName The identifier of the `ConfigOption`.
  */
  void verifyPathToConfigOption(const std::vector<std::string>& pathToOption,
                                std::string_view optionName) const;

  /*
  @brief Adds a configuration option, that can be accessed with the given path.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option.
  */
  void addConfigOption(const std::vector<std::string>& pathToOption,
                       ConfigOption&& option);

  /*
  @brief Return the underlying configuration option, if it's at the position
  described by the `keys`. If there is no configuration option at that
  place, an exception will be thrown.

  @param keys The keys for looking up the configuration option.
  */
  const ConfigOption& getConfigurationOptionByNestedKeys(
      const std::vector<std::string>& keys) const;

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
};
}  // namespace ad_utility
