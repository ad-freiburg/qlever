// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <absl/cleanup/cleanup.h>

#include <exception>
#include <thread>

#include "util/Generator.h"
#include "util/Log.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"

namespace ad_utility::streams {

using ad_utility::data_structures::ThreadSafeQueue;

/**
 * Yield all the elements of the range. A background thread iterates over the
 * range and adds the element to a queue with size `bufferLimit`, the elements
 * are the yielded from this queue. This is faster if retrieving a single
 * element from the range is expensive, but very inefficient if retrieving
 * elements is cheap because of the synchronization overhead.
 */
template <typename Range, bool logTime = (LOGLEVEL >= TIMING)>
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
    queue.finish();
  }};

  // Only rethrow an exception from the `thread` if no exception occured in this
  // thread to avoid crashes because of multiple active exceptions.
  auto cleanup = ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
      [&queue, &thread, &exception] {
        queue.finish();
        thread.join();
        // This exception will only be thrown once all the values that were
        // pushed to the queue before the exception occurred.
        if (exception) {
          std::rethrow_exception(exception);
        }
      });

  auto ifTiming = [](std::invocable auto function) {
    if constexpr (logTime) {
      std::invoke(function);
    }
  };

  std::optional<ad_utility::Timer> t{ad_utility::Timer::Started};
  ifTiming([&t] { t.emplace(ad_utility::Timer::Started); });
  while (std::optional<value_type> value = queue.pop()) {
    ifTiming([&t] { t->stop(); });
    co_yield value.value();
    ifTiming([&t] { t->cont(); });
  }
  ifTiming([&t] {
    t->stop();
    LOG(INFO) << "Waiting time for async stream was " << t->msecs().count()
              << "ms" << std::endl;
  });
}
}  // namespace ad_utility::streams
