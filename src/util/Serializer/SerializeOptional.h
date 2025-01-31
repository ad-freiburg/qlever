//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include <optional>

#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

// Serialization for `std::optional<T>`
namespace ad_utility::serialization {
template <typename T>
CPP_concept similarToOptional =
    ad_utility::similarToInstantiation<T, std::optional>;
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(similarToOptional<T> CPP_and CPP_NOT(
    std::is_trivially_copyable_v<std::decay_t<T>>)) {
  if constexpr (ReadSerializer<S>) {
    bool hasValue;
    serializer >> hasValue;
    if (!hasValue) {
      arg = std::nullopt;
    } else {
      arg.emplace();
      serializer >> arg.value();
    }
  } else {
    bool hasValue = arg.has_value();
    serializer << hasValue;
    if (hasValue) {
      serializer << arg.value();
    }
  }
}
}  // namespace ad_utility::serialization
