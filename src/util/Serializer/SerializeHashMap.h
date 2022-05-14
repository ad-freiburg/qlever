//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEHASHMAP_H
#define QLEVER_SERIALIZEHASHMAP_H

#include "../../util/HashMap.h"
#include "../TypeTraits.h"
#include "./SerializePair.h"

namespace ad_utility::serialization {
template <Serializer S, typename HashMap>
requires ad_utility::isInstantiation<ad_utility::HashMap,
                                     std::decay_t<HashMap>> void
serialize(S& serializer, HashMap&& hashMap) {
  using K = typename std::decay_t<HashMap>::key_type;
  using M = typename std::decay_t<HashMap>::mapped_type;
  if constexpr (WriteSerializer<S>) {
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
      // We can't use `value_type` as it is a pair of a *const* key and a value,
      // which would not work here because we have to write to it.
      std::pair<K, M> pair;
      serializer >> pair;
      hashMap.insert(std::move(pair));
    }
  }
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEHASHMAP_H
