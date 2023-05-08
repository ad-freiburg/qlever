// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>

#include <optional>
#include <sstream>
#include <type_traits>

#include "util/Exception.h"
#include "util/json.h"

namespace ad_benchmark {
/*
 * A rather basic wrapper for nlohmann::json, which only allows reading of
 * information and setting the configuration by parsing strings.
 */
class BenchmarkConfiguration {
  // No real reason, to really build everything ourselves, when the
  // nlohmann::json object already containes everything, that we could need.
  // P.S.: It initializes to an empty json object.
  nlohmann::json data_ = nlohmann::json(nlohmann::json::value_t::object);

  /*
  @brief Parses the given short hand and returns it as a json object,
   that contains all the described configuration data..

  @param shortHandString The language of the short hand is defined in
  `generated/BenchmarkConfigurationShorthandAutomatic.g4`.
  */
  static nlohmann::json parseShortHand(const std::string& shortHandString);

 public:
  /*
  @brief If the underlying JSON can be accessed with
  `data_[firstKey][secondKey][....]` then return the resulting value of this
  recursive access, interpreted as a given type. Else return an empty
  `std::optional`.

  @tparam ReturnType The type the resulting value should be interpreted as.

  @param keys The keys for looking up the configuration option.
   Look at the documentation of `nlohmann::basic_json::at`, if you want
   to see, what's possible.
  */
  template <typename ReturnType>
  std::optional<ReturnType> getValueByNestedKeys(const auto&... keys) const
      requires(sizeof...(keys) > 0) {
    // Easier usage.
    using ConstJsonPointer = const nlohmann::json::value_type*;

    // For recursively walking over the json objects in `data_`.
    ConstJsonPointer currentJsonObject{&data_};

    // To reduce duplication. Is a key held by a given json object?
    auto isKeyValid = []<typename Key>(ConstJsonPointer jsonObject,
                                       const Key& keyToCheck) {
      /*
      If `Key` is integral, it can only be a valid key for the `jsonObject`, if
     the `jsonObject` is an array, according to the documentation. A more
     detailed explanation: In json an object element always has a string as a
     key. No exception. However, array elements have positive, whole numbers as
     their keys. Follwing that logic backwards, a number can only ever be a
     valid key in json, if we are looking at a json array.
      */
      if constexpr (std::is_integral<Key>::value) {
        return jsonObject->is_array() && (0 <= keyToCheck) &&
               (static_cast<size_t>(keyToCheck) < jsonObject->size());
      } else {
        return jsonObject->contains(keyToCheck);
      }
    };

    // Check if the key is valid and assign the json object, it is the key of.
    auto checkAndAssign = [&currentJsonObject,
                           &isKeyValid](auto const& currentKey) {
      if (isKeyValid(currentJsonObject, currentKey)) {
        // The side effect, for which this whole things exists for.
        currentJsonObject = &(currentJsonObject->at(currentKey));
        return true;
      } else {
        return false;
      }
    };

    if ((checkAndAssign(keys) && ...)) {
      try {
        return {currentJsonObject->get<ReturnType>()};
      } catch (...) {
        // Trying to interpret the value must have failed.
        std::ostringstream errorMessage;
        errorMessage << "Interpretation error: While there was a"
                        " value found at ";
        ((errorMessage << "[" << keys << "]"), ...);
        errorMessage << ", it couldn't be interpreted as the wanted type";
        throw ad_utility::Exception(errorMessage.str());
      }
    } else {
      return std::nullopt;
    }
  }

  /*
   * @brief Sets the configuration based on the given json object literal,
   * represented by the string. This overwrites all previous held configuration
   * data.
   *
   * @param jsonString A string representing a json object literal. MUST be an
   * object, otherwise will cause an exception.
   */
  void setJsonString(const std::string& jsonString);

  /*
   * @brief Add key value pairs to the held json object literal, by adding the
   * the key value pairs of the json object literal represented by the given
   * string. This overwrites previous key value pairs in the json object
   * literal, if the keys collide.
   *
   * @param jsonString A string describing a json object literal. MUST be an
   * object, otherwise will cause an exception.
   */
  void addJsonString(const std::string& jsonString);

  /*
   * @brief Parses the given short hand and adds all configuration data, that
   *  was described with a valid syntax. This overwrites all previous held
   *  configuration data.
   *
   * @param shortHandString For a description of the short hand syntax, see
   *  `BenchmarkConfiguration::parseShortHand`
   */
  void setShortHand(const std::string& shortHandString);

  /*
   * @brief Parses the given short hand and adds all the key value pairs, that
   *  was described with a valid syntax, as key value pairs to the held
   *  json object literal. This overrides key value pairs, if the keys collide.
   *
   * @param shortHandString For a description of the short hand syntax, see
   *  `BenchmarkConfiguration::parseShortHand`
   */
  void addShortHand(const std::string& shortHandString);

  // JSON serialization.
  friend void to_json(nlohmann::json& j,
                      const BenchmarkConfiguration& configuration);
};
}  // namespace ad_benchmark
