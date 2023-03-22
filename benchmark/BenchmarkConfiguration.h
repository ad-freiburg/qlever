// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include "util/json.h"

/*
 * A rather basic wrapper for nlohmann::json, which only allows reading of
 * information and setting the configuration by parsing strings.
 */
class BenchmarkConfiguration{
 // No real reason, to really build everything ourselves, when the
 // nlohmann::json object already containes everything, that we could need.
 nlohmann::json data_;

public:

 /*
  * @brief Returns, if an configuration option was set.
  *
  * @tparam Index,Indexes The types of the keys, that you gave. Deduction
  *  should be able to handle it.
  * 
  * @param index,indexes The keys for looking up the configuration option.
  *  Look at the documentation of `nlohmann::basic_json::at`, if you want
  *  to see, what's possible.
  */
 template<typename Index, typename... Indexes>
 bool isOptionSet(const Index& index, const Indexes&... indexes) const{
  // To reduce duplication. Is a key held by a given json object?
  auto isKeyValid = [](const auto& jsonObject, const auto& key){
   // If the json object is an array, the key has to be a number, otherwise
   // the request doesn't make much sense.
   return (jsonObject.is_array() && (key < jsonObject.size())) ||
   jsonObject.contains(key);
  };
  
  // Is there json object with index as it's valid key? If yes, we have a
  // starting point for our recursive check.
  if (!isKeyValid(data_, index)){return false;}
  nlohmann::json::reference currentJsonObject = data_.at(index);

  // Check if the key is valid and assign the json object, it is the key of.
  auto checkAndAssign = [&currentJsonObject, &isKeyValid](auto const& key){
   if (isKeyValid(currentJsonObject, key)){
    // The side effect, for which this whole things exists for.
    currentJsonObject = currentJsonObject.at(key);
    return true;
   } else {
    return false;
   }
  };

  // Checking, if all the given indexes are valid in their contexts.
  // The initial `true` is for the initial test of `data_`. Because, if we
  // reached this point, it must have been true, otherwise we already would
  // have returned `false`.
  return (true && ... && checkAndAssign(indexes));
 }

 /*
  * @brief Returns a value held by the configuration.
  *
  * @tparam ReturnType The type of the value, that you want to have later.
  * @tparam Index,Indexes The types of the keys, that you gave. Deduction
  *  should be able to handle it.
  * 
  * @param index,indexes The keys for getting the value out of the
  *  configuration. Look at the documentation of `nlohmann::basic_json::at`
  *  , if you want to see, what's possible.
  */
 template<typename ReturnType, typename Index, typename... Indexes>
 ReturnType getValue(const Index& index, const Indexes&... indexes) const{
   auto toReturn = data_.at(index);
   ((toReturn = toReturn.at(indexes)), ...);
   return toReturn.template get<ReturnType>();
 }

 /*
  * @brief Returns a value held by the configuration. Returns the given
  *  default value, if it doesn't hold a value at the place described by
  *  the indeces.
  *
  * @tparam ReturnType The type of the value, that you want to have later.
  * @tparam Index,Indexes The types of the keys, that you gave. Deduction
  *  should be able to handle it.
  * 
  * @param defaultValue The value to return, if the searched for value
  *  wasn't set in the configuration.
  * @param index,indexes The keys for getting the value out of the
  *  configuration. Look at the documentation of `nlohmann::basic_json::at`
  *  , if you want to see, what's possible.
  */
 template<typename ReturnType, typename Index, typename... Indexes>
 ReturnType getValueOrDefault(const ReturnType& defaultValue,
  const Index& index, const Indexes&... indexes) const{
  if (!isOptionSet(index, indexes...)){
   return defaultValue;
  } else {
   return getValue<ReturnType>(index, indexes...);
  }
 }

 /*
  * @brief Sets the configuration based on the given json string. This
  *  overwrites all previous held configuration data.
  */
 void parseJsonString(const std::string& jsonString);

 /*
  * @brief Parses the given short hand and adds all configuration data, that
  *  was described with a valid syntax.
  *
  * @param shortHandString The language of the short hand is a number of
  *  assigments `variableName = variableContent;` with no seperator.
  *  `variableName` is the name of the configuration option. As long as it's
  *  a valid variable name in `C++` everything should be good.
  *  `variableContent` can a boolean literal, an integer literal, or a list
  *  of those literals in the form of `{value1, value2, ...}`.
  *  An example for a short hand string:
  *  `"isSorted=false;numberOfLoops=2;numberOfItems={4,5,6,7};"`
  */
 void parseShortHand(const std::string& shortHandString);

 // JSON (d)serialization.
 // TODO This works, but `"data_":null` looks a bit ugly. Is there a way,
 // to skip the `data_` and just have the content of `data_` as the
 // serialied form of this class?
 NLOHMANN_DEFINE_TYPE_INTRUSIVE(BenchmarkConfiguration, data_)
};
