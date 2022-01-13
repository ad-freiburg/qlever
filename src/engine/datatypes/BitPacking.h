//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BITPACKING_H
#define QLEVER_BITPACKING_H

#include <exception>
#include "../../util/ConstexprSmallString.h"
#include "../../util/Exception.h"

constexpr inline uint8_t numBitsRequired(size_t numDistinctValues) {
  uint8_t result = 1;
  numDistinctValues -= 1;
  while (numDistinctValues >>= 1) {
    result++;
  }
  return result;
}

// The return value has 1s for the lowest `numBits` bits, and 0 elsewhere.
constexpr inline uint64_t bitMaskForLowerBits(uint8_t numBits) {
  if (numBits > 64) {
    throw std::out_of_range{"mask for more than 64 bits required"};
  }
  if (numBits == 64) {
    return std::numeric_limits<uint64_t>::max();
  }
  return (uint64_t(1) << numBits) - 1;
}

template<uint8_t numBits>
constexpr auto unsignedTypeForNumberOfBitsImpl() {
  constexpr uint8_t numBytes = std::ceil(static_cast<float>(numBits) / 8);
  if constexpr (numBytes == 1) {
    return uint8_t{};
  } else if constexpr (numBytes == 2) {
    return uint16_t{};
  } else if constexpr (numBytes <= 4) {
    return uint32_t {};
  } else {
    return uint64_t {};
  }
}




template<uint8_t numBits>
using unsignedTypeForNumberOfBits = decltype(unsignedTypeForNumberOfBitsImpl<numBits>());

struct BoundedIntegerOutOfRangeError : public std::runtime_error{
  using std::runtime_error::runtime_error;
};

template <int64_t Min, int64_t Max> requires(Max > Min)
struct BoundedInteger {
  constexpr static auto numBits = numBitsRequired(Max - Min + 1);
  using T = unsignedTypeForNumberOfBits<numBits>;
 private:
  T _data;
 public:
  constexpr static auto min = Min;
  constexpr static auto max = Max;

  explicit constexpr BoundedInteger(int64_t value) : _data{value - Min} {
    if (value < Min || value > Max) {
      // TODO<joka921> exception message
      throw BoundedIntegerOutOfRangeError{"value out of range"};
    }
  }

  [[nodiscard]] constexpr int64_t get() const noexcept {
    return static_cast<int64_t>(_data) + Min;
  }

  [[nodiscard]] constexpr uint64_t toBits() const noexcept {return _data;}

  static constexpr BoundedInteger fromUncheckedBits(uint64_t bits) {
    return {UncheckedBitsTag{}, bits};
  }

  private:
   struct UncheckedBitsTag{};
   BoundedInteger(UncheckedBitsTag, uint64_t bits) : _data{static_cast<T>(bits)} {}
};

// TODO<joka921> requires clause
template<typename... Bounded>
struct CombinedBoundedIntegers {
  std::tuple<Bounded...> _integers;

  // TODO<joka921> requires clause
  constexpr CombinedBoundedIntegers(auto... values) {
    ...
  }
};

#endif  // QLEVER_BITPACKING_H
