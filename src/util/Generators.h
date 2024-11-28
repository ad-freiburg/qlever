//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/cleanup/cleanup.h>

#include <optional>

#include "util/Generator.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

namespace ad_utility {

// Wrap the given `generator` inside another generator that aggregates a cache
// by calling `aggregator` on every iteration of the inner `generator` until it
// returns false. If the `aggregator` returns false, the cached value is
// discarded. If the cached value is still present once the generator is fully
// consumed, `onFullyCached` is called with the cached value.
template <typename T>
cppcoro::generator<T> wrapGeneratorWithCache(
    cppcoro::generator<T> generator,
    InvocableWithExactReturnType<bool, std::optional<T>&, const T&> auto
        aggregator,
    InvocableWithExactReturnType<void, T> auto onFullyCached) {
  std::optional<T> aggregatedData{};
  bool shouldBeAggregated = true;
  for (T& element : generator) {
    if (shouldBeAggregated) {
      shouldBeAggregated = aggregator(aggregatedData, element);
      if (!shouldBeAggregated) {
        aggregatedData.reset();
      }
    }
    co_yield element;
  }
  if (aggregatedData.has_value()) {
    onFullyCached(std::move(aggregatedData).value());
  }
}

// Convert a callback-based action into a generator. The action is expected to
// call a callback being passed to it with the next value to yield.
template <typename T>
cppcoro::generator<T> generatorFromActionWithCallback(
    std::invocable<std::function<void(T)>> auto action) {
  std::mutex mutex;
  std::condition_variable cv;
  enum struct State { Inner, Outer, Finished };
  State state = State::Inner;
  struct CancelException : public std::exception {};
  std::variant<std::monostate, T, std::exception_ptr> storage;
  ad_utility::JThread thread{[&mutex, &cv, &state, &storage, &action]() {
    std::unique_lock lock(mutex);
    auto wait = [&]() {
      cv.wait(lock, [&]() { return state != State::Outer; });
      if (state == State::Finished) {
        throw CancelException{};
      }
    };

    auto writeValue = [&cv, &state, &storage](auto value) noexcept {
      storage = std::move(value);
      state = State::Outer;
      cv.notify_one();
    };
    auto writeValueAndWait = [&state, &writeValue, &wait](T value) {
      AD_CORRECTNESS_CHECK(state == State::Inner);
      writeValue(std::move(value));
      wait();
    };
    try {
      action(writeValueAndWait);
      writeValue(std::monostate{});
    } catch (...) {
      writeValue(std::current_exception());
    }
  }};
  std::unique_lock lock{mutex};
  cv.wait(lock, [&state]() { return state == State::Outer; });
  auto cleanup = absl::Cleanup{[&cv, &state, &lock]() {
    state = State::Finished;
    lock.unlock();
    cv.notify_one();
  }};
  while (true) {
    // Wait for read phase.
    cv.wait(lock, [&state] { return state == State::Outer; });
    if (std::holds_alternative<std::monostate>(storage)) {
      break;
    }
    if (std::holds_alternative<std::exception_ptr>(storage)) {
      std::rethrow_exception(std::get<std::exception_ptr>(storage));
    }
    co_yield std::get<T>(storage);
    state = State::Inner;
    lock.unlock();
    cv.notify_one();
    lock.lock();
  }
}
};  // namespace ad_utility
