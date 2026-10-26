// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_ASYNCSTREAM_H
#define QLEVER_SRC_UTIL_ASYNCSTREAM_H

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <exception>
#include <future>
#include <memory>
#include <optional>

#include "backports/asio.h"
#include "util/ExceptionHandling.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"

namespace ad_utility::streams {
namespace detail {

// The actual implementation of  `runStreamAsync` below.
template <typename Range, bool logTime>
struct AsyncStreamGenerator
    : public ad_utility::InputRangeFromGet<ql::ranges::range_value_t<Range>> {
  using value_type = ql::ranges::range_value_t<Range>;

  ad_utility::data_structures::ThreadSafeQueue<value_type> queue_;
  // If no `executor` was passed to the constructor, then the range is iterated
  // over on this private thread pool with a single thread.
  std::optional<boost::asio::thread_pool> ownThreadPool_;
  // Track the completion of the task that iterates over the range.
  std::future<void> future_;
  std::optional<ad_utility::Timer> t_;

  AsyncStreamGenerator(Range range, const size_t bufferLimit,
                       ql::any_io_executor executor)
      : queue_{bufferLimit} {
    ifTiming([this] { t_.emplace(ad_utility::Timer::Started); });

    if (!executor) {
      ownThreadPool_.emplace(1);
      executor = ownThreadPool_->get_executor();
    }

    // Push all the values of the `range` into the `queue_`.
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

    // The task on the `executor` owns the `range` in an `optional`, such that
    // it can destroy it explicitly before the `future_` becomes ready. This is
    // essential, because the destructor of this class waits for that future,
    // and callers rely on everything that the `range` owns being released once
    // that destructor has returned. But a task that runs on an executor (and
    // in particular the state that it captures) is destroyed only *after* it
    // has returned, so that without the explicit `reset()` below the `range`
    // may still be alive for a short while afterwards.
    //
    // A concrete example is the sorted output of a
    // `CompressedExternalIdTableSorter`: the `range` holds a registration as an
    // active reader of that sorter, and the sorter may legitimately be
    // `clear()`ed as soon as its output has been destroyed (see
    // `CompressedRelationPermutationWriterImpl.h`, which does exactly that for
    // every large relation of a permutation pair).
    //
    // NOTE: We deliberately use `boost::asio::use_future` instead of passing a
    // `std::packaged_task<void()>` to `boost::asio::post`. The latter triggers
    // a bug in Clang (fixed in Clang 23) with libc++, which breaks the
    // compilation of unrelated code that uses `std::packaged_task<void()>`
    // later in the same translation unit.
    future_ = boost::asio::post(
        executor, boost::asio::use_future([produceValues,
                                           range = std::optional<Range>{
                                               std::move(range)}]() mutable {
          // `produceValues` passes all exceptions from the `range` on to
          // the consumer, so an exception here is a bug that would leave
          // the `range` alive and the consumer possibly waiting forever.
          ad_utility::terminateIfThrows(
              [&produceValues, &range]() { produceValues(range.value()); },
              "Producing the values of `runStreamAsync`");
          range.reset();
        }));
  }

  // Inform the producer that the queue has finished s.t. it can terminate, and
  // then wait for it. When this destructor has returned, the producer has
  // stopped touching the `queue_` and the range has been destroyed (see the
  // constructor for why the latter matters). The thread of the
  // `ownThreadPool_` (if any) is then joined by the destructor of that member.
  ~AsyncStreamGenerator() {
    ad_utility::terminateIfThrows(
        [this]() {
          queue_.finish();
          if (future_.valid()) {
            future_.wait();
          }
        },
        "The destructor of the generator of `runStreamAsync`");
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
 * else it runs on a private thread pool with a single thread, which is joined
 * when the returned range is destroyed. Note that the executor has to be able
 * to run the task to completion concurrently with the consumer of the returned
 * range, so the thread pool behind it must have at least one thread that is not
 * otherwise occupied, and the consumer must not be one of its threads. The
 * execution context behind the `executor` has to outlive the returned range.
 */
template <typename Range, bool logTime = (ad_utility::compileTimeLogLevel >=
                                          ad_utility::LogLevel::Enum::TIMING)>
ad_utility::InputRangeTypeErased<ql::ranges::range_value_t<Range>>
runStreamAsync(Range range, size_t bufferLimit,
               ql::any_io_executor executor = {}) {
  return ad_utility::InputRangeTypeErased{
      std::make_unique<detail::AsyncStreamGenerator<Range, logTime>>(
          std::move(range), bufferLimit, std::move(executor))};
}

}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_ASYNCSTREAM_H
