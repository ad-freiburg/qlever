// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <bits/ranges_algo.h>
#include <bits/ranges_util.h>

#include <any>
#include <concepts>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeinfo>
#include <variant>

#include "util/ANTLRLexerHelper.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_utility {

// Only returns true, if the given type can be seen as a string.
template <typename T>
inline constexpr bool isString =
    std::is_constructible_v<std::string, std::decay_t<T>>;

/*
Manages a bunch of `ConfigOption`s.
*/
class ConfigManager {
 public:
  // A list of keys, that are valid keys in json.
  using KeyForJson = std::variant<nlohmann::json::size_type, std::string>;
  using VectorOfKeysForJson = std::vector<KeyForJson>;

 private:
  // The added configuration options.
  std::vector<ConfigOption> configurationOptions_;

  /*
  A configuration option tends to be placed like a key value pair in a json
  object. For example: `{"object 1" : [{"object 2" : { "The configuration option
  identifier" : "Configuration information"} }]}`

  That is, only accesable through the usage of multiple keys in the form of
  strings and numbers. Like in a container modeling a tree, with the
  configuration option in the leaves.

  This `nlohmann::json` object describes their `position` inside those trees, by
  having their `path` end in a key value pair, with the key being the
  configuration options identifier and the value their index inside
  `configurationOptions_`.
  */
  nlohmann::json keyToConfigurationOptionIndex_ =
      nlohmann::json(nlohmann::json::value_t::object);

  /*
  @brief Creates a valid `nlohmann::json` pointer based on  the given keys.
  */
  static nlohmann::json::json_pointer createJsonPointer(
      const VectorOfKeysForJson& keys);

 public:
  /*
  @brief Creates and adds a new configuration option.

  @tparam OptionType The type of value, the configuration option can hold.

  @param pathToOption Describes a path in json, that points to the value held by
  the configuration option. The last key in the vector is the name of the
  configuration option. NOTE: Both the first, and the last key have to be
  strings.
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
  void createConfigOption(const VectorOfKeysForJson& pathToOption,
                          std::string_view optionDescription,
                          OptionType* variableToPutValueOfTheOptionIn,
                          std::optional<OptionType> defaultValue =
                              std::optional<OptionType>(std::nullopt)) {
    // We need at least a name in the path.
    AD_CONTRACT_CHECK(pathToOption.size() > 0);

    // The position in the json object literal, our `pathToOption` points to.
    const nlohmann::json::json_pointer ptr{createJsonPointer(pathToOption)};

    /*
    The first key must be a string, not a number. Having an array at the
    highest level, would be bad practice, because setting and reading options,
    that are just identified with numbers, is rather difficult.
    */
    if (!std::holds_alternative<std::string>(pathToOption.front())) {
      // TODO Add custom exception. This kind of exceptionn is for runtime
      // qlever.
      throw ad_utility::Exception(
          absl::StrCat("Key error, while trying to add a configuration "
                       "option: The first key in '",
                       ptr.to_string(),
                       "' isn't a string. It needs to be a string, because "
                       "internally we save locations in a json format, more "
                       "specificly in a json object literal."));
    }

    // The last entry in the path is the name for the configuration option. It
    // must be a string.
    // TODO Custom exception.
    AD_CONTRACT_CHECK(std::holds_alternative<std::string>(pathToOption.back()));

    /*
    The string keys must be a valid `NAME` in the short hand. Otherwise, the
    option can't get accessed with the short hand.
    */
    auto checkIfValidNameVisitor = []<typename T>(const T& key) {
      // Only actually check, if we have a string.
      if constexpr (isString<std::decay_t<T>>) {
        return stringOnlyContainsSpecifiedTokens<ConfigShorthandLexer>(
            AD_FWD(key), static_cast<size_t>(ConfigShorthandLexer::NAME));
      } else {
        return true;
      }
    };

    if (auto failedKey = std::ranges::find_if_not(
            pathToOption,
            [&checkIfValidNameVisitor](const KeyForJson& key) {
              return std::visit(checkIfValidNameVisitor, key);
            });
        failedKey != pathToOption.end()) {
      /*
      One of the keys failed. `failedKey` is an iterator pointing to the key.
      */
      // TODO Add custom exception. This kind of exceptionn is for runtime
      // qlever.
      throw ad_utility::Exception(absl::StrCat(
          "Key error: The key '", std::get<std::string>(*failedKey), "' in '",
          ptr.to_string(),
          "' doesn't describe a valid name, according to the short hand "
          "grammar."));
    }

    // Is there already a configuration option with the same identifier at the
    // same location?
    if (keyToConfigurationOptionIndex_.contains(ptr)) {
      // TODO Add custom exception. This kind of exceptionn is for runtime
      // qlever.
      throw ad_utility::Exception(absl::StrCat(
          "Key error: There was already a configuration option found at '",
          ptr.to_string(), "'\n", printConfigurationDoc(), "\n"));
    }

    // Add the location of the new configuration option to the json.
    keyToConfigurationOptionIndex_[ptr] = configurationOptions_.size();
    // Add the new configuration option.
    configurationOptions_.push_back(makeConfigOption<OptionType>(
        std::get<std::string>(pathToOption.back()), optionDescription,
        variableToPutValueOfTheOptionIn, defaultValue));
  }

  /*
  @brief Creates and adds a new configuration option.

  @tparam OptionType The type of value, the configuration option can hold.

  @param optionName Name of the option. Additionally, this the one name is the
  entire path in json, needed to address the value held by this option.
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
  void createConfigOption(std::string_view optionName,
                          std::string_view optionDescription,
                          OptionType* variableToPutValueOfTheOptionIn,
                          std::optional<OptionType> defaultValue =
                              std::optional<OptionType>(std::nullopt)) {
    createConfigOption<OptionType>(
        VectorOfKeysForJson{std::string{optionName}}, optionDescription,
        variableToPutValueOfTheOptionIn, defaultValue);
  }

  /*
  Get all the added configuration options.
  */
  const std::vector<ConfigOption>& getConfigurationOptions() const;

  /*
  @brief Return the underlying configuration option, if it's at the position
  described by the `keys`. If there is no configuration option at that
  place, an exception will be thrown.

  @param keys The keys for looking up the configuration option.
  */
  const ConfigOption& getConfigurationOptionByNestedKeys(
      const VectorOfKeysForJson& keys) const;

  /*
  @brief Sets the configuration options based on the given json. Note: This will
  overwrite values held by the configuration data, if there were values for them
  given inside the string.

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
  `generated/ConfigShorthand.g4`.
  */
  static nlohmann::json parseShortHand(const std::string& shortHandString);

  /*
  @brief Returns a string containing an example json configuration and the
  string representations of all added configuration options.
  */
  std::string printConfigurationDoc() const;
};
}  // namespace ad_utility
