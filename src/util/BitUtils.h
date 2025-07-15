//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BITUTILS_H
#define QLEVER_BITUTILS_H

#include <cmath>
#include <exception>

#include "./Exception.h"
#include "./TypeTraits.h"

namespace ad_utility {
// The return value has 1s for the lowest `numBits` bits, and 0 in all the
// higher bits.
constexpr inline uint64_t bitMaskForLowerBits(uint64_t numBits) {
  if (numBits > 64) {
    throw std::out_of_range{"mask for more than 64 bits required"};
  }
  if (numBits == 64) {
    return std::numeric_limits<uint64_t>::max();
  }
  return (uint64_t(1) << numBits) - 1;
}

// The return value has 1s for the highest `numBits` bits, and 0 in all the
// lower bits.
constexpr inline uint64_t bitMaskForHigherBits(uint64_t numBits) {
  return ~bitMaskForLowerBits(64 - numBits);
}

// A constexpr function to determine how many bits are required in order to
// represent an unsigned integer with a given maximum value (inclusive).
constexpr int bitMaskSizeForValue(uint64_t maxValue) {
  if (maxValue == 0) {
    return 0;
  }
  return bitMaskSizeForValue(maxValue >> 1) + 1;
}

namespace detail {

// Return any value, the type of which is the smallest unsigned integer type
// that contains at least `numBits` many bits. For example, if `numBits` <= 8,
// then an `uint8_t` will be returned.
template <uint8_t numBits>
constexpr auto unsignedTypeForNumberOfBitsImpl() {
  static_assert(numBits <= 64);
  if constexpr (numBits <= 8) {
    return uint8_t{};
  } else if constexpr (numBits <= 16) {
    return uint16_t{};
  } else if constexpr (numBits <= 32) {
    return uint32_t{};
  } else {
    return uint64_t{};
  }
}
}  // namespace detail

// The smallest unsigned integer type that contains at least `numBits` many
// bits. For example, if `numBits` <= 8, then an `uint8_t` will be returned.
template <uint8_t numBits>
using unsignedTypeForNumberOfBits =
    decltype(detail::unsignedTypeForNumberOfBitsImpl<numBits>());
}  // namespace ad_utility

#endif  // QLEVER_BITUTILS_H
