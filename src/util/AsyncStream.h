// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_ASYNCSTREAM_H
#define QLEVER_SRC_UTIL_ASYNCSTREAM_H

#include <absl/cleanup/cleanup.h>

#include <exception>
#include <memory>
#include <optional>

#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/jthread.h"

namespace ad_utility::streams {
namespace detail {

// The actual implementation of  `runStreamAsync` below.
template <typename Range, bool logTime>
struct AsyncStreamGenerator
    : public ad_utility::InputRangeFromGet<ql::ranges::range_value_t<Range>> {
  using value_type = typename Range::value_type;

  ad_utility::data_structures::ThreadSafeQueue<value_type> queue_;
  ad_utility::JThread thread_;
  std::optional<ad_utility::Timer> t_;

  AsyncStreamGenerator(Range range, const size_t bufferLimit)
      : queue_{bufferLimit} {
    ifTiming([this] { t_.emplace(ad_utility::Timer::Started); });

    thread_ = ad_utility::JThread{[this, range = std::move(range)]() mutable {
      try {
        for (auto& value : range) {
          if (!queue_.push(std::move(value))) {
            return;
          }
        }
      } catch (...) {
        queue_.pushException(std::current_exception());
      }
      queue_.finish();
    }};
  }

  // Inform the thread that the queue has finished s.t. it can join.
  ~AsyncStreamGenerator() { queue_.finish(); }

  std::optional<value_type> get() override {
    ifTiming([this] { t_->cont(); });
    auto value{queue_.pop()};
    ifTiming([this] { t_->stop(); });

    if (!value) {
      ifTiming([this] {
        t_->stop();
        LOG(TRACE) << "Waiting time for async stream was "
                   << t_->msecs().count() << "ms" << std::endl;
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

/**
 * Yield all the elements of the range. A background thread iterates over the
 * range and adds the element to a queue with size `bufferLimit`, the elements
 * are the yielded from this queue. This is faster if retrieving a single
 * element from the range is expensive, but very inefficient if retrieving
 * elements is cheap because of the synchronization overhead.
 */
template <typename Range, bool logTime = (LOGLEVEL >= TIMING)>
ad_utility::InputRangeTypeErased<ql::ranges::range_value_t<Range>>
runStreamAsync(Range range, size_t bufferLimit) {
  return ad_utility::InputRangeTypeErased{
      std::make_unique<detail::AsyncStreamGenerator<Range, logTime>>(
          std::move(range), bufferLimit)};
}

}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_ASYNCSTREAM_H
