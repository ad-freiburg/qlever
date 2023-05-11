// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <any>
#include <concepts>
#include <optional>
#include <sstream>
#include <type_traits>
#include <typeinfo>

#include "../benchmark/infrastructure/BenchmarkConfigurationOption.h"
#include "nlohmann/json.hpp"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/json.h"

namespace ad_benchmark {

/*
Manages a bunch of `BenchmarkConfigurationOption`s.
*/
class BenchmarkConfiguration {
  // The added configuration options.
  std::vector<BenchmarkConfigurationOption> configurationOptions_;

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

  @tparam Keys Positive whole numbers, or strings.
  */
  template <std::convertible_to<std::string>... Keys>
  static nlohmann::json::json_pointer createJsonPointer(const Keys&... keys) {
    // A json pointer needs special characters, if a `/`, or `~`, is used in a
    // key. So here a special conversion function for our keys, that adds those,
    // if needed.
    auto toString = []<typename T>(const T& key) -> std::string {
      std::string transformedKey = static_cast<std::string>(key);

      // Replace special character `~` with `~0`.
      transformedKey = absl::StrReplaceAll(transformedKey, {{"~", "~0"}});

      // Replace special character `/` with `~1`.
      return absl::StrReplaceAll(transformedKey, {{"/", "~1"}});
    };

    // Creating the string for the pointer.
    std::ostringstream pointerString;
    pointerString << "/";
    ((pointerString << toString(keys) << "/"), ...);

    return nlohmann::json::json_pointer(pointerString.str());
  }

  /*
   * @brief Parses the given short hand and returns it as a json object,
   *  that contains all the described configuration data.
   *
   * @param shortHandString The language of the short hand is a number of
   *  assigments `variableName = variableContent;`.
   *  `variableName` is the name of the configuration option. As long as it's
   *  a valid variable name in JSON everything should be good.
   *  `variableContent` can a boolean literal, an integer literal,  a string
   *  literal, or a list of those literals in the form of
   *  `[value1, value2, ...]`.
   *  An example for a short hand string:
   *  `"isSorted=false;numberOfLoops=2;numberOfItems={4,5,6,7];myName =
   * "Bernd";`
   */
  static nlohmann::json parseShortHand(const std::string& shortHandString);

 public:
  void addConfigurationOption(BenchmarkConfigurationOption&& option,
                              const auto&... keys) requires(sizeof...(keys) > 0)
  {
    // The position in the json object literal, our keys point to.
    const nlohmann::json::json_pointer ptr{createJsonPointer(AD_FWD(keys)...)};

    // Is there already a configuration option with the same identifier at the
    // same location?
    if (keyToConfigurationOptionIndex_.contains(ptr)) {
      throw ad_utility::Exception(absl::StrCat(
          "Key error: There was already a configuration option found at '",
          ptr.to_string(), "'"));
    }

    // Add the location of the new configuration option to the json.
    keyToConfigurationOptionIndex_[ptr] = configurationOptions_.size();
    configurationOptions_.push_back(std::move(option));
  }

  /*
  Get all the added configuration options.
  */
  std::vector<BenchmarkConfigurationOption> getConfigurationOptions() const;

  /*
  @brief Return the underlying configuration option, if it's at the position
  described by the `keys`. If there is no configuration option at that
  place, an exception will be thrown.

  @param keys The keys for looking up the configuration option.
   Look at the documentation of `nlohmann::basic_json::at`, if you want
   to see, what's possible.
  */
  const BenchmarkConfigurationOption& getConfigurationOptionByNestedKeys(
      const auto&... keys) const requires(sizeof...(keys) > 0) {
    // If there is an entry at the described location, this should point to the
    // index number of the configuration option in `configurationOptions_`.
    const nlohmann::json::json_pointer ptr{createJsonPointer(AD_FWD(keys)...)};

    if (keyToConfigurationOptionIndex_.contains(ptr)) {
      return configurationOptions_.at(
          keyToConfigurationOptionIndex_.at(ptr).get<size_t>());
    } else {
      throw ad_utility::Exception(absl::StrCat(
          "Key error: There was no configuration option found at '",
          ptr.to_string(), "'"));
    }
  }

  /*
   * @brief Sets the configuration options based on the given json object
   * literal, represented by the string. Note: This will overwrite values held
   * by the configuration data, if there were values for them given inside the
   * string.
   *
   * @param jsonString A string representing a json object literal. MUST be an
   * object, otherwise will cause an exception.
   */
  void setJsonString(const std::string& jsonString);

  /*
   * @brief Parses the given short hand and sets all configuration options, that
   *  where described with a valid syntax. Note: This will overwrite values held
   * by the configuration data, if there were values for them given inside the
   * string.
   *
   * @param shortHandString For a description of the short hand syntax, see
   *  `BenchmarkConfiguration::parseShortHand`
   */
  void setShortHand(const std::string& shortHandString);

  // JSON serialization.
  friend void to_json(nlohmann::json& j,
                      const BenchmarkConfiguration& configuration);
};
}  // namespace ad_benchmark
