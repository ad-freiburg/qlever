//
// Created by johannes on 01.05.21.
//

#ifndef QLEVER_SERIALIZEHASHMAP_H
#define QLEVER_SERIALIZEHASHMAP_H

#include "../../util/HashMap.h"

template <typename Serializer, class K, class V, class HashFcn, class EqualKey, class Alloc>
void serialize(Serializer& serializer,
               ad_utility::HashMap<K, V, HashFcn, EqualKey, Alloc>& hashMap) {
  if constexpr (Serializer::IsWriteSerializer) {
    serializer& hashMap.size();
    for (auto& [key, value] : hashMap) {
      serializer& key;
      serializer& value;
    }
  } else {
    hashMap.clear();
    auto size = hashMap.size();
    serializer& size;

    hashMap.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      std::pair<K, V> pair;
      serializer& pair.first;
      serializer& pair.second;
      hashMap.insert(std::move(pair));
    }
  }
}

#endif  // QLEVER_SERIALIZEHASHMAP_H
