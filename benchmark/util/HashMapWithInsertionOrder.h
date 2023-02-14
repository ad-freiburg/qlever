// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <string>

#include <util/HashMap.h>
#include <util/Exception.h>

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

  // The hash map.
  ad_utility::HashMap<Key, Value> hashMap_;

  // The order of insertion.
  std::vector<Key> insertionOrder_;

  public:

  /*
   * @brief Add a new key, value pair to the hash map. Warning:
   *  The value and key do NOT get copied.
   */
  void addEntry(const Key& key, const Value& value){
    // It is not allowed to have two entires with the same key.
    AD_CHECK(!hashMap_.contains(key));

    // Directly insert the given value into the table and note,
    // when it was inserted.
    hashMap_.insert(std::make_pair(key, value));
    insertionOrder_.push_back(key);
  }

  /*
   * @brief Return a reference to the value of the key, value pair. Creates an
   *  exception, if there is no entry with key.
   */
  Value& getReferenceToValue(const Key& key){
    try {
      return hashMap_.at(key);
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
    // The new vector containing the values for the keys in hashMapKeys in the
    // same order.
    std::vector<Value> hashMapValues;

    // The end size of hashMapValues is exactly the size of insertionOrder_. So
    // we can already allocate all memory, that it will use, making all the
    // following calls of push_back cheap.
    hashMapValues.reserve(insertionOrder_.size());

    // Copying the values into hashMapValues.
    std::ranges::for_each(insertionOrder_,
        [&hashMapValues, this](const Key& key)
        mutable{
          // I can't use getReferenceToValue, because that function is not
          // const.
          hashMapValues.push_back(hashMap_.at(key));
        },
        {});

    return hashMapValues;
  }
};
