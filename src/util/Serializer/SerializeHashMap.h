//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SERIALIZEHASHMAP_H
#define QLEVER_SERIALIZEHASHMAP_H

#include "util/HashMap.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

namespace ad_utility::serialization {

AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, ad_utility::HashMap>)) {
  using K = typename std::decay_t<T>::key_type;
  using M = typename std::decay_t<T>::mapped_type;
  if constexpr (WriteSerializer<S>) {
    serializer << arg.size();
    for (const auto& pair : arg) {
      serializer << pair;
    }
  } else {
    arg.clear();
    auto size = arg.size();
    serializer >> size;

    arg.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      // We have to use `pair<T::key_type, T::mapped_type>` instead of
      // `T::value_type` because the latter is a `pair<const key_type,
      // mapped_type>` and we have to write to the key.
      std::pair<K, M> pair;
      serializer >> pair;
      arg.insert(std::move(pair));
    }
  }
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SERIALIZEHASHMAP_H
