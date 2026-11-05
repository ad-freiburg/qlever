// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_MATH_H
#define QLEVER_SRC_UTIL_MATH_H

#include <cstddef>

#include "util/Exception.h"

namespace ad_utility {

// Return `numerator / denominator`, rounded up. The `denominator` must be
// positive. Unlike the common `(numerator + denominator - 1) / denominator`,
// this cannot overflow.
constexpr size_t ceilDiv(size_t numerator, size_t denominator) {
  AD_CONTRACT_CHECK(denominator > 0);
  return numerator / denominator + (numerator % denominator != 0);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MATH_H
