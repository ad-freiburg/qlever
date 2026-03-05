// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_SERIALIZER_ZEROCOPYSERIALIZER_H
#define QLEVER_SRC_UTIL_SERIALIZER_ZEROCOPYSERIALIZER_H

#include <array>

#include "util/Exception.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility::serialization {

// Align the current write position to the alignment requirement of type T.
// This adds padding bytes (zeros) if necessary.
CPP_template(typename T, typename S)(
    requires WriteSerializer<S>) void alignForType(S& serializer) {
  size_t currentPos = serializer.getCurrentPosition();
  size_t alignment = alignof(T);
  size_t padding = (alignment - (currentPos % alignment)) % alignment;
  if (padding > 0) {
    // Use a static array to avoid dynamic allocation for padding.
    // 64 bytes should cover all practical alignment requirements.
    constexpr size_t maxAlignment = 64;
    std::array<char, maxAlignment> zeros = {0};
    AD_CONTRACT_CHECK(padding < zeros.size(),
                      "Alignment padding exceeds maximum supported alignment");
    serializer.serializeBytes(zeros.data(), padding);
  }
}

// Skip bytes to align the current read position to the alignment requirement
// of type T.
CPP_template(typename T, typename S)(
    requires ReadSerializer<S>) void alignForType(S& serializer) {
  size_t currentPos = serializer.getCurrentPosition();
  size_t alignment = alignof(T);
  size_t padding = (alignment - (currentPos % alignment)) % alignment;
  if (padding > 0) {
    serializer.skip(padding);
  }
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_ZEROCOPYSERIALIZER_H
