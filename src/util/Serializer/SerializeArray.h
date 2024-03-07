// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <tuple>

#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"
#include "util/ConstexprUtils.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT((ad_utility::isArray<std::decay_t<T>> || ad_utility::similarToInstantiation<T, std::tuple>)) {
  using Arr = std::decay_t<T>;
  // TODDO<joka921> We want "each of the contained types is trivially serializable"
  if constexpr (TriviallySerializable<Arr>) {
    using CharPtr = std::conditional_t<ReadSerializer<S>, char*, const char*>;
    serializer.serializeBytes(reinterpret_cast<CharPtr>(&arg),
                              sizeof(arg));
  } else {
    ad_utility::ConstexprForLoop(
        std::make_index_sequence<std::tuple_size_v<std::decay_t<T>>>(),
        [&]<size_t I> { serializer | std::get<I>(arg); });
  }
}
}  // namespace ad_utility::serialization
