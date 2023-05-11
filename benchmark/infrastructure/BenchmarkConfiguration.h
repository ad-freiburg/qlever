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

// The types, that can be used for access keys in `nlohmann::json` objects.
template <typename T>
concept KeyForJson = (std::convertible_to<T, std::string> || std::integral<T>);

// Only returns true, if all the given keys, that are numbers, are bigger/equal
// than 0.
static bool allArgumentsBiggerOrEqualToZero(const auto&... keys) {
  auto biggerOrEqual = []<typename T>(const T& key) {
    if constexpr (std::is_arithmetic_v<T>) {
      return key >= 0;
    } else {
      return true;
    }
  };
  return (biggerOrEqual(keys) && ...);
}

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
  static nlohmann::json::json_pointer createJsonPointer(
      const KeyForJson auto&... keys) {
    // A numeric key, must be `>=0`.
    AD_CONTRACT_CHECK(allArgumentsBiggerOrEqualToZero(keys...));

    // Creating the string for the pointer.
    std::ostringstream pointerString;
    pointerString << "/";
    ((pointerString << keys << "/"), ...);

    // A json pointer needs special characters, if a `/`, or `~`, is used in it.
    // Unfortunaly, I don't know the behavior of `StrReplaceAll` good enoguh, to
    // be able to tell, if it would lead to no complications, if we replace both
    // symbols at once.
    return nlohmann::json::json_pointer(absl::StrReplaceAll(
        absl::StrReplaceAll(pointerString.str(), {{"~", "~0"}}),
        {{"/", "~1"}}));
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
  /*
  @brief Add the given configuration option in such a way, that it can be
  accessed by calling `getConfigurationOptionByNestedKeys` with the here given
  keys, follwed by the name of the configuration option.
  Example: Given a configuration option `numberOfRows` and the keys `"general
  Options", 1, "Table"`, then it can be accessed with `"general Options", 1,
  "Table", "numberOfRows"`.
  */
  void addConfigurationOption(const BenchmarkConfigurationOption& option,
                              const KeyForJson auto&... keys) {
    // A numeric key, must be `>=0`.
    AD_CONTRACT_CHECK(allArgumentsBiggerOrEqualToZero(keys...));

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
  const std::vector<BenchmarkConfigurationOption>& getConfigurationOptions()
      const;

  /*
  @brief Return the underlying configuration option, if it's at the position
  described by the `keys`. If there is no configuration option at that
  place, an exception will be thrown.

  @param keys The keys for looking up the configuration option.
   Look at the documentation of `nlohmann::basic_json::at`, if you want
   to see, what's possible.
  */
  const BenchmarkConfigurationOption& getConfigurationOptionByNestedKeys(
      const KeyForJson auto&... keys) const requires(sizeof...(keys) > 0) {
    // A numeric key, must be `>=0`.
    AD_CONTRACT_CHECK(allArgumentsBiggerOrEqualToZero(keys...));

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
