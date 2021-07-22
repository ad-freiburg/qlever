//
// Created by johannes on 01.05.21.
//

#ifndef QLEVER_SERIALIZEHASHMAP_H
#define QLEVER_SERIALIZEHASHMAP_H

#include "../../util/HashMap.h"
#include "./SerializePair.h"

namespace ad_utility::serialization {
template <typename Serializer, class K, class V, class HashFcn, class EqualKey,
          class Alloc>
void serialize(Serializer& serializer,
               ad_utility::HashMap<K, V, HashFcn, EqualKey, Alloc>& hashMap) {
  if constexpr (Serializer::IsWriteSerializer) {
    serializer << hashMap.size();
    for (const auto& pair : hashMap) {
      serializer << pair;
    }
  } else {
    hashMap.clear();
    auto size = hashMap.size();
    serializer >> size;

    hashMap.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      std::pair<K, V> pair;
      serializer >> pair;
      hashMap.insert(std::move(pair));
    }
  }
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEHASHMAP_H
