// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_ASYNCSTREAM_H
#define QLEVER_SRC_UTIL_ASYNCSTREAM_H

#include <absl/cleanup/cleanup.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <exception>
#include <future>
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
  using value_type = ql::ranges::range_value_t<Range>;

  ad_utility::data_structures::ThreadSafeQueue<value_type> queue_;
  // Exactly one of the following two members is used, depending on whether an
  // `executor` was passed to the constructor: without an executor the range is
  // iterated over by the `thread_`, with an executor by a task that was posted
  // to that executor, the completion of which the `future_` tracks.
  ad_utility::JThread thread_;
  std::future<void> future_;
  std::optional<ad_utility::Timer> t_;

  AsyncStreamGenerator(Range range, const size_t bufferLimit,
                       boost::asio::any_io_executor executor)
      : queue_{bufferLimit} {
    ifTiming([this] { t_.emplace(ad_utility::Timer::Started); });

    // Push all the values of the `range` into the `queue_`. Takes the range by
    // reference, because the two ways of launching this below own it
    // differently.
    auto produceValues = [this](Range& rangeToConsume) {
      try {
        for (auto& value : rangeToConsume) {
          if (!queue_.push(std::move(value))) {
            // The consumer has already finished the queue, so we must not
            // finish it again.
            return;
          }
        }
      } catch (...) {
        queue_.pushException(std::current_exception());
      }
      queue_.finish();
    };

    if (!executor) {
      thread_ = ad_utility::JThread{
          [produceValues, range = std::move(range)]() mutable {
            produceValues(range);
          }};
      return;
    }

    // The task on the `executor` owns the `range` in an `optional`, such that
    // it can destroy it explicitly before the `promise` is fulfilled. This is
    // essential, because the destructor of this class waits for that promise,
    // and callers rely on everything that the `range` owns being released once
    // that destructor has returned. A `JThread` (see above) guarantees this via
    // its `join()`, but a task that runs on an executor is destroyed only
    // *after* it has returned, so that without the explicit `reset()` below the
    // `range` may still be alive for a short while afterwards.
    //
    // A concrete example is the sorted output of a
    // `CompressedExternalIdTableSorter`: the `range` holds a registration as an
    // active reader of that sorter, and the sorter may legitimately be
    // `clear()`ed as soon as its output has been destroyed (see
    // `CompressedRelationPermutationWriterImpl.h`, which does exactly that for
    // every large relation of a permutation pair).
    std::promise<void> promise;
    future_ = promise.get_future();
    boost::asio::post(
        executor, [produceValues, promise = std::move(promise),
                   range = std::optional<Range>{std::move(range)}]() mutable {
          absl::Cleanup fulfillPromise{[&promise]() { promise.set_value(); }};
          produceValues(range.value());
          range.reset();
        });
  }

  // Inform the producer that the queue has finished s.t. it can terminate, and
  // then wait for it: the `thread_` is joined by its own destructor, the task
  // on the `executor` is waited for explicitly. When this destructor has
  // returned, the producer has stopped touching the `queue_` and the range has
  // been destroyed (see the constructor for why the latter matters).
  ~AsyncStreamGenerator() {
    queue_.finish();
    if (future_.valid()) {
      future_.wait();
    }
  }

  std::optional<value_type> get() override {
    ifTiming([this] { t_->cont(); });
    auto value{queue_.pop()};
    ifTiming([this] { t_->stop(); });

    if (!value) {
      ifTiming([this] {
        t_->stop();
        AD_LOG_TRACE << "Waiting time for async stream was "
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
  }
};

}  // namespace detail

/**
 * Yield all the elements of the range. A background task iterates over the
 * range and adds the element to a queue with size `bufferLimit`, the elements
 * are the yielded from this queue. This is faster if retrieving a single
 * element from the range is expensive, but very inefficient if retrieving
 * elements is cheap because of the synchronization overhead.
 *
 * If an `executor` is given (typically the executor of a
 * `boost::asio::thread_pool`), then the background task runs on that executor,
 * else it runs on a thread that is created for it and joined again when the
 * returned range is destroyed. Note that the executor has to be able to run the
 * task to completion concurrently with the consumer of the returned range, so
 * the thread pool behind it must have at least one thread that is not otherwise
 * occupied, and the consumer must not be one of its threads. The execution
 * context behind the `executor` has to outlive the returned range.
 */
template <typename Range, bool logTime = (ad_utility::compileTimeLogLevel >=
                                          ad_utility::LogLevel::Enum::TIMING)>
ad_utility::InputRangeTypeErased<ql::ranges::range_value_t<Range>>
runStreamAsync(Range range, size_t bufferLimit,
               boost::asio::any_io_executor executor = {}) {
  return ad_utility::InputRangeTypeErased{
      std::make_unique<detail::AsyncStreamGenerator<Range, logTime>>(
          std::move(range), bufferLimit, std::move(executor))};
}

}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_ASYNCSTREAM_H
