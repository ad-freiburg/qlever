// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_ASYNCSTREAM_H
#define QLEVER_SRC_UTIL_ASYNCSTREAM_H

#include <absl/cleanup/cleanup.h>

#include <exception>
#include <memory>
#include <optional>
#include <thread>

#include "Iterators.h"
#include "jthread.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"

namespace {
using ad_utility::data_structures::ThreadSafeQueue;

template <typename Range, bool logTime>
struct AsyncStreamGenerator
    : public ad_utility::InputRangeFromGet<typename Range::value_type> {
  using value_type = typename Range::value_type;
  AsyncStreamGenerator(Range range, const size_t bufferLimit)
      : queue{bufferLimit},
        exception{nullptr},
        thread{&AsyncStreamGenerator::thread_function, std::move(range),
               std::ref(queue), std::ref(exception)} {
    ifTiming([this] { t.emplace(ad_utility::Timer::Started); });
  }

  std::optional<value_type> get() override {
    ifTiming([this] { t->cont(); });
    auto value{queue.pop()};
    ifTiming([this] { t->stop(); });

    if (!value) {
      auto cleanup =
          ad_utility::makeOnDestructionDontThrowDuringStackUnwinding([this] {
            queue.finish();
            thread.join();
            // This exception will only be thrown once all the values that were
            // pushed to the queue before the exception occurred.
            if (exception) {
              std::rethrow_exception(exception);
            }
          });
      ifTiming([this] {
        t->stop();
        LOG(TRACE) << "Waiting time for async stream was " << t->msecs().count()
                   << "ms" << std::endl;
      });
    }

    return value;
  }

  static void thread_function(Range range, ThreadSafeQueue<value_type>& queue,
                              std::exception_ptr& exception_ptr) {
    try {
      for (auto& value : range) {
        if (!queue.push(std::move(value))) {
          return;
        }
      }
    } catch (...) {
      exception_ptr = std::current_exception();
    }
    queue.finish();
  }

  void ifTiming(std::function<void()> function) {
    if constexpr (logTime) {
      std::invoke(function);
    }
  }

  ThreadSafeQueue<value_type> queue;
  std::exception_ptr exception;
  ad_utility::JThread thread;
  std::optional<ad_utility::Timer> t;
};

}  // namespace

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
ad_utility::InputRangeTypeErased<typename Range::value_type> runStreamAsync(
    Range range, size_t bufferLimit) {
  using value_type = typename Range::value_type;

  auto generator{std::make_unique<AsyncStreamGenerator<Range, logTime>>(
      std::move(range), bufferLimit)};
  return ad_utility::InputRangeTypeErased<value_type>{std::move(generator)};
}

}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_ASYNCSTREAM_H
