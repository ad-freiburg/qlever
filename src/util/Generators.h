//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GENERATORS_H
#define QLEVER_SRC_UTIL_GENERATORS_H

#include <absl/cleanup/cleanup.h>

#include <condition_variable>
#include <mutex>
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
// NOTE: The `int` is just a dummy value.
CPP_template(typename InputRange, typename AggregatorT,
             typename T = ql::ranges::range_value_t<InputRange>,
             typename FullyCachedFuncT = int)(
    requires InvocableWithExactReturnType<AggregatorT, bool, std::optional<T>&,
                                          const T&>
        CPP_and InvocableWithExactReturnType<FullyCachedFuncT, void, T>)
    cppcoro::generator<T> wrapGeneratorWithCache(
        InputRange generator, AggregatorT aggregator,
        FullyCachedFuncT onFullyCached) {
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

// Convert a callback-based `functionWithCallback` into a generator by spawning
// a thread that runs the functionWithCallback. The `functionWithCallback` is a
// callable that takes a `callback` with signature `void(T)`. The arguments with
// which this callback is called when running the `functionWithCallback` become
// the elements that are yielded by the created `generator`.
template <typename T, typename F>
cppcoro::generator<T> generatorFromActionWithCallback(F functionWithCallback)
    requires ql::concepts::invocable<F, std::function<void(T)>> {
  std::mutex mutex;
  std::condition_variable cv;

  // Only one of the threads can run at the same time. The semantics are
  // `Inner` -> The inner thread that runs the callback has to produce the next
  // value. `Outer` -> The outer thread that is the generator has to yield the
  // next value. value. `OuterIsFinished` -> The generator is finished and will
  // read no further values from the inner thread, the inner thread has to stop.
  enum struct State { Inner, Outer, OuterIsFinished };
  State state = State::Inner;

  // Used to pass the produced values from the inner thread to the generator.
  // `monostate` means that the inner thread is finished and there will be no
  // further values. `exception_ptr` means, that the inner thread has
  // encountered an exception, which then rethrown by the outer generator.
  std::variant<std::monostate, T, std::exception_ptr> storage;

  // Set up the inner thread.
  ad_utility::JThread thread{
      [&mutex, &cv, &state, &storage, &functionWithCallback]() {
        struct FinishedBecauseOuterIsFinishedException : public std::exception {
        };
        // The inner thread starts (it has to produce the first value), so we
        // can unconditionally take the lock.
        std::unique_lock lock(mutex);
        AD_CORRECTNESS_CHECK(state == State::Inner);

        // Wait for the outer thread to consume the last value and return
        // control to the inner thread.
        auto wait = [&]() {
          cv.wait(lock, [&]() { return state != State::Outer; });
          if (state == State::OuterIsFinished) {
            throw FinishedBecauseOuterIsFinishedException{};
          }
        };

        // Write the `value` to the `storage` and notify the outer thread that
        // it has to consume it.
        auto writeValue = [&cv, &state, &storage, &lock](auto value) noexcept {
          storage = std::move(value);
          state = State::Outer;
          lock.unlock();
          cv.notify_one();
          lock.lock();
        };

        // Write the `value`, pass control to the outer thread and wait till it
        // is our turn again.
        auto writeValueAndWait = [&state, &writeValue, &wait](T value) {
          AD_CORRECTNESS_CHECK(state == State::Inner);
          writeValue(std::move(value));
          wait();
        };

        try {
          // `writeValueAndWait` is the callback that is repeatedly called by
          // the `functionWithCallback`.
          functionWithCallback(writeValueAndWait);

          // The function has completed, notify the outer thread and finish.
          writeValue(std::monostate{});
        } catch (const FinishedBecauseOuterIsFinishedException&) {
          // This means that the outer thread is completely finished and only
          // waits for the inner thread to join.
          return;
        } catch (...) {
          // The function has created an exception, pass it to the outer thread.
          writeValue(std::current_exception());
        }
        // The destructor of `unique_lock` automatically calls unlock, so we
        // unconditionally pass control to the outer thread.
      }};

  // Now the logic for the outer generator thread:
  // The outer thread initially has to wait for the inner thread to produce
  // the first value.
  std::unique_lock lock{mutex};
  cv.wait(lock, [&state]() { return state == State::Outer; });

  // At the end we have to notify the inner thread that it has to stop producing
  // values, s.t. we can clean up. This happens in particular if the outer
  // generator is prematurely destroyed. NOTE: This can only happen once we hold
  // the lock, which means that the inner thread is waiting for the outer
  // thread.
  auto cleanup = absl::Cleanup{[&cv, &state, &lock]() {
    state = State::OuterIsFinished;
    lock.unlock();
    cv.notify_one();
  }};

  while (true) {
    // Wait for the inner thread to produce a value.
    cv.wait(lock, [&state] { return state == State::Outer; });
    if (std::holds_alternative<std::monostate>(storage)) {
      break;
    }
    if (std::holds_alternative<std::exception_ptr>(storage)) {
      std::rethrow_exception(std::get<std::exception_ptr>(storage));
    }
    co_yield std::get<T>(storage);

    // Pass control to the inner thread and repeat.
    state = State::Inner;
    lock.unlock();
    cv.notify_one();
    lock.lock();
  }
}
};  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GENERATORS_H
