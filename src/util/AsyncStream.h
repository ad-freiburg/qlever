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

#include "Generator.h"
#include "Iterators.h"
#include "jthread.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"

namespace ad_utility::streams {
namespace detail {
using ad_utility::data_structures::ThreadSafeQueue;

// The actual implementation of  `runStreamAsync` below.
template <typename Range, bool logTime>
struct AsyncStreamGenerator
    : public ad_utility::InputRangeFromGet<ql::ranges::range_value_t<Range>> {
  using value_type = typename Range::value_type;

  ThreadSafeQueue<value_type> queue;
  ad_utility::JThread thread;
  std::optional<ad_utility::Timer> t;

  AsyncStreamGenerator(Range range, const size_t bufferLimit)
      : queue{bufferLimit} {
    ifTiming([this] { t.emplace(ad_utility::Timer::Started); });

    thread = ad_utility::JThread{[this, range = std::move(range)]() mutable {
      try {
        for (auto& value : range) {
          if (!queue.push(std::move(value))) {
            return;
          }
        }
      } catch (...) {
        queue.pushException(std::current_exception());
      }
      queue.finish();
    }};
  }

  // Inform the thread that the queue has finished s.t. it can join.
  ~AsyncStreamGenerator() { queue.finish(); }

  std::optional<value_type> get() override {
    ifTiming([this] { t->cont(); });
    auto value{queue.pop()};
    ifTiming([this] { t->stop(); });

    if (!value) {
      ifTiming([this] {
        t->stop();
        LOG(TRACE) << "Waiting time for async stream was " << t->msecs().count()
                   << "ms" << std::endl;
      });
    }
    return value;
  }

  template <typename F>
  void ifTiming(F function) {
    if constexpr (logTime) {
      std::invoke(function);
    }
  };
};

}  // namespace detail

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
  return ad_utility::InputRangeTypeErased{
      std::make_unique<detail::AsyncStreamGenerator<Range, logTime>>(
          std::move(range), bufferLimit)};
}

}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_ASYNCSTREAM_H
