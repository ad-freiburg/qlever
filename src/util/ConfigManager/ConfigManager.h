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
  @brief Add the given configuration option in such a way, that it can be
  accessed by calling `getConfigurationOptionByNestedKeys` with the here given
  keys, follwed by the name of the configuration option.
  Example: Given a configuration option `numberOfRows` and the keys
  `{"generalOptions", 1, "Table"}`, then it can be accessed with
  `{"generalOptions", 1, "Table", "numberOfRows"}`.
  */
  void addConfigurationOption(const ConfigOption& option,
                              const VectorOfKeysForJson& keys = {});

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
