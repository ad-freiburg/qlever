// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZE_SERIALIZEATOMIC_H
#define QLEVER_SRC_UTIL_SERIALIZE_SERIALIZEATOMIC_H

#include "util/Serializer/Serializer.h"
#include "util/Synchronized.h"
#include "util/TypeTraits.h"

// Serialization for `ad_utility::CopyableAtomic<T>` if `T` is default
// constructible.
namespace ad_utility::serialization {
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, ad_utility::CopyableAtomic>)
        CPP_and CPP_NOT(std::is_trivially_copyable_v<std::decay_t<T>>)
            CPP_and std::is_default_constructible_v<
                typename std::decay_t<T>::value_type>) {
  using V = typename std::decay_t<T>::value_type;
  if constexpr (ReadSerializer<S>) {
    V target;
    serializer >> target;
    arg = target;
  } else {
    serializer << arg.load();
  }
}
}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZE_SERIALIZEATOMIC_H
