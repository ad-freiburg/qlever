// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <string>

#include "util/HashMap.h"
#include "util/Exception.h"
#include "util/json.h"

/*
 * @brief A custom exception for HashMapWithInsertionOrder, for when a key
 *  doesn't exist in a hash map.
 */
class KeyIsntRegisteredException : public std::exception {
  private:
    // The error message.
    std::string message_;

  public:
    /*
     * @param keyName Name of the key. Must be given.
     * @param hashMapName Name of the hash map, where you are looking.
     */
    KeyIsntRegisteredException(const std::string& keyName,
        const std::string& hashMapName = ""){
      // This part always exists.
      message_ = "No (key, value)-pair with the key '" + keyName + "' found";
      // Add the hashMapName, if we have one.
      if (hashMapName != "") {
        message_ += " in the hash map '" + hashMapName + "'";
      }
      message_ += ".";
    }

    const char * what () const throw () {
      return message_.c_str();
    }
};

/*
 * @brief A basic hash map, that saves the order of inserted elements.
 *
 * @tparam Key, Value The types for the hash map.
 */
template<typename Key, typename Value>
class HashMapWithInsertionOrder{

  // Holds all the values.
  std::vector<Value> values_;

  // Translates the key to the index of the value in `values_`.
  ad_utility::HashMap<Key, size_t> keyToValueIndex_;

  public:

  /*
   * @brief Add a new key, value pair to the hash map.
   */
  void addEntry(const Key& key, const Value& value){
    // It is not allowed to have two entires with the same key.
    AD_CONTRACT_CHECK(!keyToValueIndex_.contains(key));

    // Note the values index and add it.
    keyToValueIndex_.insert(std::make_pair(key, values_.size()));
    values_.push_back(value);
  }

  /*
   * @brief Return a reference to the value of the key, value pair. Creates an
   *  exception, if there is no entry with key.
   */
  Value& getReferenceToValue(const Key& key){
    try {
      return values_.at(keyToValueIndex_.at(key));
    } catch (std::out_of_range const&) {
      // Instead of the the default error, when a key doesn't exist in a hash
      // map, we use our own custom one. Makes things easier to understand.
      if constexpr (std::is_same<Key, std::string>::value) {
        throw KeyIsntRegisteredException(key);
      } else {
        throw KeyIsntRegisteredException(std::to_string(key));
      }
    }
  }

  /*
   * @brief Returns all values in the order, that they were inserted into the
   *  hash map.
   */
  const std::vector<Value> getAllValues() const{
    return values_;
  }
  
  // Functions for json serialization.
  void to_json(nlohmann::json& j) const{
    // Making sure, that j is an array.
    j = nlohmann::json::array();

    // Adding key value pairs to the json object in the form of arrays
    // `[key, value]`.
    std::ranges::for_each(keyToValueIndex_,
        [&j, this](const auto& keyValuePair){
          j.push_back({keyValuePair.first, values_.at(keyValuePair.second)});
        }, {});
  }

  // Functions for json deserialization.
  void from_json(const nlohmann::json& j){
    // Nothing to do, if there are no values.
    if (!j.empty()){
      // Adding all the key value pairs. Every entry in the json array should be
      // an array of the form `[key, value]`.
      std::ranges::for_each(j, [this](const auto& arrayEntry){
            addEntry(arrayEntry.at(0).template get<Key>(),
              arrayEntry.at(1).template get<Value>());
          }, {});
    }
  }
};


// Functions for json (d)serialization. These just call the required member
// functions of the HashMapWithInsertionOrder. I would remove them, but they
// have to be in the same namespace, so...

template<typename Key, typename Value>
void to_json(nlohmann::json& j, const HashMapWithInsertionOrder<Key, Value>& HMap){
  HMap.to_json(j);
}

template<typename Key, typename Value>
void from_json(const nlohmann::json& j,
    HashMapWithInsertionOrder<Key, Value>& HMap){
  HMap.from_json(j);
}

