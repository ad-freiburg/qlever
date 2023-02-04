// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (February of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <util/HashMap.h>
#include <util/Exception.h>

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
    return hashMap_.at(key);
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
