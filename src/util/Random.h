// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Simple interfaces for the random facilities from the STL

#ifndef QLEVER_RANDOM_H
#define QLEVER_RANDOM_H

#include <random>
#include <type_traits>
#include <vector>

/**
 * @brief Create random integers from a given range
 * @tparam Int an integral type like int, unsigned int, size_t, etc...
 */
template <typename Int, typename = std::enable_if_t<std::is_integral_v<Int>>>
class RandomIntGenerator {
 public:
  explicit RandomIntGenerator(Int min = std::numeric_limits<Int>::min(),
                              Int max = std::numeric_limits<Int>::max())
      : _randomEngine(std::random_device{}()), _distribution(min, max) {}

  Int operator()() { return _distribution(_randomEngine); }

 private:
  std::default_random_engine _randomEngine;
  std::uniform_int_distribution<Int> _distribution;
};

#endif  // QLEVER_RANDOM_H
