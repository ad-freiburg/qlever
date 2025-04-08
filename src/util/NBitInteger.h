//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_NBITINTEGER_H
#define QLEVER_NBITINTEGER_H

#include "./BitUtils.h"

namespace ad_utility {

// A type for signed integers that only need `N` bits to represent.
CPP_template(uint8_t N)(requires(N <= 64) CPP_and(N >= 1))  //
    class NBitInteger {
  using T = uint64_t;
  static constexpr uint8_t UNUSED_BITS = 64 - N;

 public:
  static constexpr T toNBit(int64_t i) {
    constexpr T mask = bitMaskForLowerBits(N);
    return static_cast<T>(i & mask);
  }

  static constexpr int64_t fromNBit(T t) {
    // The right shift is arithmetic, so a sign bit will be propagated.
    return (static_cast<int64_t>(t) << UNUSED_BITS) >> UNUSED_BITS;
  }

  static constexpr int64_t max() {
    // propagate the sign.
    return std::numeric_limits<int64_t>::max() >> UNUSED_BITS;
  }
  static constexpr int64_t min() {
    // propagate the sign.
    return std::numeric_limits<int64_t>::min() >> UNUSED_BITS;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_NBITINTEGER_H
