// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Simple interfaces for the random facilities from the STL

#pragma once

#include <algorithm>
#include <array>
#include <cstring>
#include <random>
#include <type_traits>
#include <vector>

/**
 * A simple and fast Pseudo-Random-Number-Generator called Xoroshiro128+, for
 * details see
 * https://thompsonsed.co.uk/random-number-generators-for-c-performance-tested
 * Limiting the range of the generated numbers is currently not supported.
 * Requires that `Int` is an integral type that fits into 64 bits. If used with
 * a type that does not fulfill this property, the template will not match
 * (because of the std::enable_if) and there will be a compile-time error.
 */
template <typename Int>
requires(std::is_integral_v<Int> &&
         sizeof(Int) <= sizeof(uint64_t)) class FastRandomIntGenerator {
 public:
  FastRandomIntGenerator() {
    // Randomly initialize the shuffleTable
    std::random_device seeder{};
    // `std::random_device` only yields 32 bit values, so we need two of them
    // for each entry of `_shuffleTable`
    static_assert(sizeof(decltype(seeder())) == 4);
    for (auto& el : _shuffleTable) {
      el = seeder();
      el <<= 32;
      el |= seeder();
    }
  }

  // Generate a random number
  Int operator()() {
    uint64_t s1 = _shuffleTable[0];
    uint64_t s0 = _shuffleTable[1];
    uint64_t result = s0 + s1;
    _shuffleTable[0] = s0;
    s1 ^= s1 << 23;
    _shuffleTable[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);

    // keep the bits.
    // TODO<C++20>: std::bit_cast
    Int convertedResult;
    std::memcpy(&convertedResult, &result, sizeof(Int));
    return convertedResult;
  }

 private:
  std::array<uint64_t, 4> _shuffleTable;
};

/**
 * @brief Create random integers from a given range.
 * @tparam Int an integral type like int, unsigned int, size_t, etc...
 *
 * This generator is much slower than the FastRandomIntGenerator, but has
 * more flexibility and stronger probabilistic guarantees
 */
template <typename Int, typename = std::enable_if_t<std::is_integral_v<Int>>>
class SlowRandomIntGenerator {
 public:
  explicit SlowRandomIntGenerator(Int min = std::numeric_limits<Int>::min(),
                                  Int max = std::numeric_limits<Int>::max())
      : _randomEngine{std::random_device{}()}, _distribution{min, max} {}

  Int operator()() { return _distribution(_randomEngine); }

 private:
  std::default_random_engine _randomEngine;
  std::uniform_int_distribution<Int> _distribution;
};

class RandomDoubleGenerator {
 public:
  explicit RandomDoubleGenerator(
      double min = std::numeric_limits<double>::min(),
      double max = std::numeric_limits<double>::max())
      : _randomEngine{std::random_device{}()}, _distribution{min, max} {}

  double operator()() { return _distribution(_randomEngine); }

 private:
  std::default_random_engine _randomEngine;
  std::uniform_real_distribution<double> _distribution;
};

/// Randomly shuffle range denoted by `[begin, end)`
template <typename RandomIt>
void randomShuffle(RandomIt begin, RandomIt end) {
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(begin, end, g);
}
