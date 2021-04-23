// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Simple interfaces for the random facilities from the STL

#ifndef QLEVER_RANDOM_H
#define QLEVER_RANDOM_H

#include <future>
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
                              Int max = std::numeric_limits<Int>::max()) {
    for (size_t i = 0; i < NUM_FUTURES; ++i) {
      _randomEngines[i] = std::default_random_engine{std::random_device{}()};
    }
    _distributions.fill(std::uniform_int_distribution<Int>{min, max});
    for (size_t i = 0; i < NUM_FUTURES; ++i) {
      startFuture(i);
    }
  }

  Int operator()() {
    if (_elementIndex < _buffer.size()) {
      return _buffer[_elementIndex++];
    }
    // we are out of buffered random numbers
    _buffer = _futures[_futureIndex].get();
    startFuture(_futureIndex);
    _futureIndex = (_futureIndex + 1) % NUM_FUTURES;
    _elementIndex = 0;
    // we now have elements in the buffer;
    return operator()();
  }

 private:
  void startFuture(size_t i) {
    _futures[i] = std::async([i, this]() {
      std::vector<Int> result;
      result.reserve(NUM_ELEMENTS_PER_FUTURE);
      for (size_t k = 0; k < NUM_ELEMENTS_PER_FUTURE; ++k) {
        result.push_back(_distributions[i](_randomEngines[i]));
      }
      return result;
    });
  }
  std::array<std::default_random_engine, 3> _randomEngines;
  static constexpr size_t NUM_FUTURES = 3;
  static constexpr size_t NUM_ELEMENTS_PER_FUTURE = 1000000;
  std::array<std::uniform_int_distribution<Int>, NUM_FUTURES> _distributions;
  std::array<std::future<std::vector<Int>>, NUM_FUTURES> _futures;
  std::vector<Int> _buffer;
  size_t _futureIndex = 0;
  size_t _elementIndex = 0;
};

#endif  // QLEVER_RANDOM_H
