// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <absl/cleanup/cleanup.h>

#include <exception>
#include <thread>

#include "./Generator.h"
#include "./ThreadSafeQueue.h"

namespace ad_utility::streams {

using ad_utility::data_structures::ThreadSafeQueue;

/**
 * Yield all the elements of the range. A background thread iterates over the
 * range and adds the element to a queue with size `bufferLimit`, the elements
 * are the yielded from this queue. This is faster if retrieving a single
 * element from the range is expensive, but very inefficient if retrieving
 * elements is cheap because of the synchronization overhead.
 */
template <typename Range>
cppcoro::generator<typename Range::value_type> runStreamAsync(
    Range range, size_t bufferLimit) {
  using value_type = typename Range::value_type;
  ThreadSafeQueue<value_type> queue{bufferLimit};
  std::exception_ptr exception = nullptr;
  std::thread thread{[&] {
    try {
      for (auto& value : range) {
        if (!queue.push(std::move(value))) {
          return;
        }
      }
    } catch (...) {
      exception = std::current_exception();
    }
    queue.signalLastElementWasPushed();
  }};

  absl::Cleanup cleanup{[&] {
    queue.disablePush();
    thread.join();
    if (exception) {
      // This exception will only be thrown once all the values that were
      // pushed to the queue before the exception occurred.
      std::rethrow_exception(exception);
    }
  }};

  while (std::optional<value_type> value = queue.pop()) {
    co_yield value.value();
  }
}
}  // namespace ad_utility::streams
