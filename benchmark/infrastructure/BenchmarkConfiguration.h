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
#include <string>
#include <type_traits>
#include <typeinfo>

#include "../benchmark/infrastructure/BenchmarkConfigurationOption.h"
#include "generated/BenchmarkConfigurationShorthandLexer.h"
#include "nlohmann/json.hpp"
#include "util/ANTLRLexerHelper.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_benchmark {

// Only returns true, if the given type can be seen as a string.
template <typename T>
inline constexpr bool isString =
    std::is_constructible_v<std::string, std::decay_t<T>>;

// The types, that can be used as access keys in `nlohmann::json` objects. In
// short: Only whole numbers and everything, that could be converted into a
// string.
template <typename T>
concept KeyForJson = isString<T> || std::integral<std::decay_t<T>>;

// Only returns true, if all the given keys, that are numbers, are bigger/equal
// than 0.
static bool allArgumentsBiggerOrEqualToZero(const KeyForJson auto&... keys) {
  [[maybe_unused]] auto biggerOrEqual = []<typename T>(const T& key) {
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
      const KeyForJson auto&... keys) requires(sizeof...(keys) > 0) {
    // A numeric key, must be `>=0`.
    AD_CONTRACT_CHECK(allArgumentsBiggerOrEqualToZero(keys...));

    /*
    A json pointer needs special characters, if a `/`, or `~`, is used in a
    key. So here a special conversion function for our keys, that adds those,
    if needed.
    */
    auto toString = []<typename T>(const T& key) {
      // Our transformed key.
      std::string transformedKey;

      /*
      Transforming the key. We simply check through the way, we can convert
      them into a string and do the one, that works first.
      */
      if constexpr (std::is_constructible_v<std::string, T>) {
        transformedKey = std::string(key);
      } else {
        /*
        Must have been a number. I mean, `KeyForJson` doesn't allow anything
        else, than those 2 possibilities and this the the last one.
        */
        static_assert(std::integral<std::decay_t<T>>);
        transformedKey = std::to_string(key);
      }

      // Replace special character `~` with `~0` and `/` with `~1`.
      return absl::StrReplaceAll(transformedKey, {{"~", "~0"}, {"/", "~1"}});
    };

    // Creating the string for the pointer.
    std::ostringstream pointerString;
    ((pointerString << "/" << toString(keys)), ...);

    return nlohmann::json::json_pointer(pointerString.str());
  }

  /*
  @brief Parses the given short hand and returns it as a json object,
   that contains all the described configuration data.

  @param shortHandString The language of the short hand is defined in
  `generated/BenchmarkConfigurationShorthandAutomatic.g4`.
  */
  static nlohmann::json parseShortHand(const std::string& shortHandString);

 public:
  /*
  @brief Add the given configuration option in such a way, that it can be
  accessed by calling `getConfigurationOptionByNestedKeys` with the here given
  keys, follwed by the name of the configuration option.
  Example: Given a configuration option `numberOfRows` and the keys
  `"generalOptions", 1, "Table"`, then it can be accessed with
  `"generalOptions", 1, "Table", "numberOfRows"`.
  */
  template <KeyForJson... Keys>
  void addConfigurationOption(const BenchmarkConfigurationOption& option,
                              const Keys&... keys) {
    // All numeric key, must be `>=0`.
    AD_CONTRACT_CHECK(allArgumentsBiggerOrEqualToZero(keys...));

    // The position in the json object literal, our keys point to.
    const nlohmann::json::json_pointer ptr{
        createJsonPointer(AD_FWD(keys)..., option.getIdentifier())};

    /*
    The first key must be a string, not a number. Having an array at the
    highest level, would be bad practice, because setting and reading options,
    that are just identified with numbers, is rather difficult.
    */
    if constexpr (sizeof...(keys) > 0) {
      if constexpr (!isString<ad_utility::First<Keys...>>) {
        throw ad_utility::Exception(
            absl::StrCat("Key error: The first key in '", ptr.to_string(),
                         "' isn't a string."));
      }
    }

    /*
    The string keys must be a valid `NAME` in the short hand. Otherwise, the
    option can't get accessed with the short hand.
    */
    if constexpr (sizeof...(keys) > 0) {
      // For saving, which key failed the test.
      std::string_view lastCheckedKey;

      auto checkIfValidName = [&lastCheckedKey]<typename T>(const T& key) {
        // Only actually check, if we have a string, or string like, type `T`.
        if constexpr (isString<T>) {
          lastCheckedKey = key;
          return stringOnlyContainsSpecifiedTokens<
              BenchmarkConfigurationShorthandLexer>(
              AD_FWD(key),
              static_cast<size_t>(BenchmarkConfigurationShorthandLexer::NAME));
        } else {
          return true;
        }
      };

      if ((!checkIfValidName(keys) || ...)) {
        /*
        One of the keys failed. Because how the logical `or` is evaluated, left
        to right, until a `true` is found, the `string_view` must hold the
        failed key.
        */
        throw ad_utility::Exception(absl::StrCat(
            "Key error: The key '", lastCheckedKey, "' in '", ptr.to_string(),
            "' doesn't describe a valid name, according to the short hand "
            "grammar."));
      }
    }

    // Is there already a configuration option with the same identifier at the
    // same location?
    if (keyToConfigurationOptionIndex_.contains(ptr)) {
      throw ad_utility::Exception(absl::StrCat(
          "Key error: There was already a configuration option found at '",
          ptr.to_string(), "'\n", static_cast<std::string>(*this), "\n"));
    }

    // Add the location of the new configuration option to the json.
    keyToConfigurationOptionIndex_[ptr] = configurationOptions_.size();
    configurationOptions_.push_back(option);
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
          ptr.to_string(), "'\n", static_cast<std::string>(*this), "\n"));
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

  // For printing.
  explicit operator std::string() const;
};
}  // namespace ad_benchmark
