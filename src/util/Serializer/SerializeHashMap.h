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

namespace detail {
// The class template that `ad_utility::HashMap` is an alias for. We cannot
// directly use `ad_utility::HashMap` in `similarToInstantiation`, because it is
// not a variadic alias when the Boost hash maps are used.
#ifdef QLEVER_HAS_BOOST_UNORDERED
template <typename... Ts>
using HashMapTemplate = boost::unordered_flat_map<Ts...>;
#else
template <typename... Ts>
using HashMapTemplate = absl::flat_hash_map<Ts...>;
#endif
}  // namespace detail

AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, detail::HashMapTemplate>)) {
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

    // NOTE: The actual memory usage of the hash map is larger than this, but
    // this check is enough to reject invalid huge sizes before reserving.
    checkAllocation(serializer, size, sizeof(std::pair<K, M>));
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
