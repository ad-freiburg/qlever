// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPARISONOFINTANDDOUBLE_H
#define QLEVER_SRC_UTIL_COMPARISONOFINTANDDOUBLE_H

#include <cmath>
#include <cstdint>

namespace ad_utility {

// Compare `i` and `d` exactly: return a negative number, zero, or a positive
// number if `i` is less than, equal to, or greater than `d`. `d` must not be
// NaN. The built-in comparison converts `i` to a double, so e.g. both `2^53`
// and `2^53 + 1` would equal `2^53`, which is not a strict weak ordering.
inline int compareIntAndDouble(int64_t i, double d) {
  // `2^63` is exactly representable as a double, but not as an `int64_t`.
  constexpr double twoToThe63 = 9223372036854775808.0;
  if (d >= twoToThe63) {
    return -1;
  }
  if (d < -twoToThe63) {
    return 1;
  }
  // Now the integral part of `d` fits into an `int64_t`, and `d - integralPart`
  // is exact.
  double integralPart = std::trunc(d);
  auto integralPartAsInt = static_cast<int64_t>(integralPart);
  if (i != integralPartAsInt) {
    return i < integralPartAsInt ? -1 : 1;
  }
  double fractionalPart = d - integralPart;
  return fractionalPart > 0 ? -1 : (fractionalPart < 0 ? 1 : 0);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COMPARISONOFINTANDDOUBLE_H
