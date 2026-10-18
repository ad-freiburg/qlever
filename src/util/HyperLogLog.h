// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_HYPERLOGLOG_H
#define QLEVER_SRC_UTIL_HYPERLOGLOG_H

#include <absl/numeric/bits.h>

#include <cmath>
#include <cstdint>
#include <vector>

namespace ad_utility {

// A HyperLogLog sketch (Flajolet et al., 2007) for estimating the number of
// distinct values in a stream using constant memory (`2^14` bytes) and constant
// time per value. The relative standard error is about `1.04 / sqrt(2^14)`,
// which is less than 1%, but this is a standard deviation and not a bound.
// Small cardinalities (up to tens of thousands) are estimated almost exactly
// via linear counting. Just above the switch from linear counting to the raw
// estimate (around `2.5 * 2^14` distinct values), the raw estimate is biased,
// such that errors of a few percent can occur (HyperLogLog++ corrects this with
// empirical bias tables, which are not needed for our purposes).
class HyperLogLog {
  static constexpr size_t numBits_ = 14;
  static constexpr size_t numRegisters_ = size_t{1} << numBits_;
  std::vector<uint8_t> registers_ = std::vector<uint8_t>(numRegisters_, 0);

  // The finalizer of SplitMix64, which is a cheap and good 64-bit mixer.
  static constexpr uint64_t hash(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
  }

 public:
  // Add a value to the sketch. Adding the same value repeatedly has no effect.
  void add(uint64_t value) {
    uint64_t h = hash(value);
    // The first `numBits_` bits select the register, the position of the first
    // one-bit in the remaining bits is the rank.
    size_t index = h >> (64 - numBits_);
    uint64_t rest = h << numBits_;
    auto rank = static_cast<uint8_t>(rest == 0 ? 64 - numBits_ + 1
                                               : absl::countl_zero(rest) + 1);
    if (rank > registers_[index]) {
      registers_[index] = rank;
    }
  }

  // Return the estimated number of distinct values added so far.
  double estimate() const {
    constexpr double m = numRegisters_;
    double sum = 0;
    size_t numZeroRegisters = 0;
    for (uint8_t rank : registers_) {
      sum += std::ldexp(1.0, -rank);
      numZeroRegisters += rank == 0;
    }
    double rawEstimate = 0.7213 / (1 + 1.079 / m) * m * m / sum;
    // Small range correction (linear counting). No large range correction is
    // needed because of the 64-bit hash.
    if (rawEstimate <= 2.5 * m && numZeroRegisters != 0) {
      return m * std::log(m / static_cast<double>(numZeroRegisters));
    }
    return rawEstimate;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HYPERLOGLOG_H
