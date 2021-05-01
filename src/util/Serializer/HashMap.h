//
// Created by johannes on 01.05.21.
//

#ifndef QLEVER_HASHMAP_H
#define QLEVER_HASHMAP_H

#include "../../util/HashMap.h"

namespace ad_utility {
template <typename Serializer, class K, class V, class HashFcn, class EqualKey,
          class Alloc>
void serialize(Serializer& serializer,
               HashMap<K, V, HashFcn, EqualKey, Alloc>& hashMap,
               [[maybe_unused]] unsigned int version) {
  if constexpr (Serializer::isWriteSerializer) {
    serializer | hashMap.size();
    for (auto& [key, value] : hashMap) {
      serializer | key;
      serializer | value;
    }
  } else {
    hashMap.clear();
    auto size = hashMap.size();
    serializer | size;

    hashMap.reserve(size);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_HASHMAP_H
