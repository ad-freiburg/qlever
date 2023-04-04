// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <optional>

#include "util/json.h"
#include <type_traits>

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
 @brief If the underlying JSON can be accessed with
 `data_[firstKey][secondKey][....]` the return the resulting value of this
 recursive access, interpreted as a given type. Else return an empty
 `std::optional`.

 @tparam ReturnType The type the resulting value should be interpreted as.
 
 @param key, keys The keys for looking up the configuration option.
  Look at the documentation of `nlohmann::basic_json::at`, if you want
  to see, what's possible.
 */
 template<typename ReturnType>
 std::optional<ReturnType> getValueByNestedKeys(const auto& key,
  const auto&... keys)const{
  // Easier usage. 
  using ConstJsonPointer = const nlohmann::json::value_type*;

  // For recursively walking over the json objects in `data_`.
  ConstJsonPointer currentJsonObject{&data_};
  
  // To reduce duplication. Is a key held by a given json object?
  auto isKeyValid = []<typename Key>(ConstJsonPointer jsonObject,
   const Key& keyToCheck){
    /*
    If `Key` is integral, it can only be a valid key for the `jsonObject`, if the
   `jsonObject` is an array, according to the documentation.
    A more detailed explanation: In json an object element always has a string
    as a key. No exception. However, array elements have positive, whole
    numbers as their keys. Follwing that logic backwards, a number can only
    ever be a valid key in json, if we are looking at a json array.
    */
    if constexpr (std::is_integral<Key>::value){
     return jsonObject->is_array() && (0 <= keyToCheck) &&
     (keyToCheck < jsonObject->size());
    }else{
     return jsonObject->contains(keyToCheck);
    }
   };
  
  // Check if the key is valid and assign the json object, it is the key of.
  auto checkAndAssign = [&currentJsonObject, &isKeyValid](
   auto const& currentKey){
   if (isKeyValid(currentJsonObject, currentKey)){
    // The side effect, for which this whole things exists for.
    currentJsonObject = &(currentJsonObject->at(currentKey));
    return true;
   } else {
    return false;
   }
  };

  if ((checkAndAssign(key) && ... && checkAndAssign(keys))){
   return {currentJsonObject->get<ReturnType>()};
  }else{
   return {std::nullopt};
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
