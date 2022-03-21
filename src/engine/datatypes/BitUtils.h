//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BITUTILS_H
#define QLEVER_BITUTILS_H

#include <cmath>
#include <exception>

#include "../../util/Exception.h"
#include "../../util/TypeTraits.h"

namespace ad_utility {

// Compute the number of bits that is required to encode the range from 0 to
// `numDistinctValues - 1` as an unsigned integer.
constexpr inline uint8_t numBitsRequired(size_t numDistinctValues) {
  if (numDistinctValues == 0) {
    return 1;
  }
  uint8_t result = 1;
  numDistinctValues -= 1;
  while (numDistinctValues >>= 1) {
    result++;
  }
  return result;
}

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

namespace detail {

// A constexpr implementation of `ceil` (round up) on 32-bit floats.
// TODO<C++23> Use `std::ceil` which will then become constexpr.
constexpr float ceil(float input) {
  if (input >= static_cast<float>(std::numeric_limits<int64_t>::max())) {
    return input;
  }
  auto asInt = static_cast<int64_t>(input);
  if (asInt < input) {
    return asInt + 1;
  } else {
    return asInt;
  }
}
// Return any value, the type of which is the smallest unsigned integer type
// that contains at least `numBits` many bits. For example, if `numBits` <= 8,
// then an `uint8_t` will be returned.
template <uint8_t numBits>
constexpr auto unsignedTypeForNumberOfBitsImpl() {
  constexpr uint8_t numBytes = ceil(static_cast<float>(numBits) / 8);
  static_assert(numBytes <= 8);
  if constexpr (numBytes == 0 || numBytes == 1) {
    return uint8_t{};
  } else if constexpr (numBytes == 2) {
    return uint16_t{};
  } else if constexpr (numBytes <= 4) {
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
