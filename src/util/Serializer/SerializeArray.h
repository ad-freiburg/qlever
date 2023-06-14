// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>

#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT((ad_utility::isArray<std::decay_t<T>>)) {
  using V = typename std::decay_t<T>::value_type;
  if constexpr (TriviallySerializable<V>) {
    using CharPtr = std::conditional_t<ReadSerializer<S>, char*, const char*>;
    serializer.serializeBytes(reinterpret_cast<CharPtr>(arg.data()),
                              arg.size() * sizeof(V));
  } else {
    for (size_t i = 0; i < arg.size(); ++i) {
      serializer | arg[i];
    }
  }
}
}  // namespace ad_utility::serialization
