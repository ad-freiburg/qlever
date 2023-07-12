// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//          Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <ranges>

#include "absl/cleanup/cleanup.h"
#include "util/Exception.h"
#include "util/Generator.h"
#include "util/jthread.h"

namespace ad_utility::data_structures {

/// A thread safe, multi-consumer, multi-producer queue.
template <typename T>
class ThreadSafeQueue {
  std::exception_ptr pushedException_;
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable pushNotification_;
  std::condition_variable popNotification_;
  // Note: Although this class is generally synchronized via `std::mutex`, we
  // still use `std::atomic` for the information whether it has finished. This
  // allows the `finish()` function to be noexcept which allows a safe way to
  // prevent deadlocks.
  std::atomic_flag finish_ = ATOMIC_FLAG_INIT;
  size_t maxSize_;

 public:
  using value_type = T;
  explicit ThreadSafeQueue(size_t maxSize) : maxSize_{maxSize} {}

  // We can neither copy nor move this class
  ThreadSafeQueue(const ThreadSafeQueue&) = delete;
  const ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;
  ThreadSafeQueue(ThreadSafeQueue&&) = delete;
  const ThreadSafeQueue& operator=(ThreadSafeQueue&&) = delete;

  /// Push an element into the queue. Block until there is free space in the
  /// queue or until finish() was called. Return false if finish()
  /// was called. In this case the current element element and all future
  /// elements are not added to the queue.
  bool push(T value) {
    std::unique_lock lock{mutex_};
    popNotification_.wait(
        lock, [this] { return queue_.size() < maxSize_ || finish_.test(); });
    if (finish_.test()) {
      return false;
    }
    queue_.push(std::move(value));
    lock.unlock();
    pushNotification_.notify_one();
    return true;
  }

  // The semantics of pushing an exception are as follows: All subsequent calls
  // to `pop` will throw the `exception`, and all subsequent calls to `push`
  // will return `false`.
  void pushException(std::exception_ptr exception) {
    std::unique_lock lock{mutex_};
    // It is important that we only push the first exception we encounter,
    // otherwise there might be race conditions between rethrowing and resetting
    // the exception.
    if (pushedException_ != nullptr) {
      return;
    }
    pushedException_ = std::move(exception);
    finish_.test_and_set();
    lock.unlock();
    pushNotification_.notify_all();
    popNotification_.notify_all();
  }

  // After calling this function, all calls to `push` will return `false` and no
  // further elements will be added to the queue. Calls to `pop` will yield the
  // elements that were already stored in the queue before the call to
  // `finish`, after those were drained, `pop` will return `nullopt`. This
  // function can be called from the producing/pushing threads to signal that
  // all elements have been pushed, or from the consumers to signal that they
  // will not pop further elements from the queue.
  void finish() noexcept {
    // It is crucial that this function never throws, so that we can safely call
    // it unconditionally in destructors to prevent deadlocks. Should the
    // implementation ever change, make sure that it is still `noexcept`.
    finish_.test_and_set();
    pushNotification_.notify_all();
    popNotification_.notify_all();
  }

  /// Always call `finish` on destruction. This makes sure that worker
  /// threads that pop from the queue always see std::nullopt, even if the
  /// threads that push to the queue exit via an exception or if the explicit
  /// call to `finish` is missing.
  ~ThreadSafeQueue() { finish(); }

  /// Blocks until another thread pushes an element via push() which is
  /// hen returned or signalLastElementWasPushed() is called resulting in an
  /// empty optional, whatever happens first
  std::optional<T> pop() {
    std::unique_lock lock{mutex_};
    pushNotification_.wait(lock, [this] {
      return !queue_.empty() || finish_.test() || pushedException_;
    });
    if (pushedException_) {
      std::rethrow_exception(pushedException_);
    }
    if (finish_.test() && queue_.empty()) {
      return {};
    }
    std::optional<T> value = std::move(queue_.front());
    queue_.pop();
    lock.unlock();
    popNotification_.notify_one();
    return value;
  }
};

// A thread safe queue that is similar (wrt the interface and the behavior) to
// the `ThreadSafeQueue` above, with the following difference: Each element that
// is pushed is associated with a unique index `n`. A call to `push(n,
// someValue)` will block until other threads have pushed all indices in the
// range [0, ..., n - 1]. This can be used to enforce the ordering of values
// that are asynchronously created by multiple threads. Note that great care has
// to be taken that all the indices will be pushed eventually by some thread,
// and that for each thread individually the indices are increasing, otherwise
// the queue will lead to a deadlock.
template <typename T>
class OrderedThreadSafeQueue {
 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  ThreadSafeQueue<T> queue_;
  size_t nextIndex_ = 0;
  // For the reason why this is `atomic_flag`, see the same member in
  // `ThreadSafeQueue`.
  std::atomic_flag finish_ = ATOMIC_FLAG_INIT;

 public:
  using value_type = T;
  // Construct from the maximal queue size (see `ThreadSafeQueue` for details).
  explicit OrderedThreadSafeQueue(size_t maxSize) : queue_{maxSize} {}

  // We can neither copy nor move this class
  OrderedThreadSafeQueue(const OrderedThreadSafeQueue&) = delete;
  const OrderedThreadSafeQueue& operator=(const OrderedThreadSafeQueue&) =
      delete;
  OrderedThreadSafeQueue(OrderedThreadSafeQueue&&) = delete;
  const OrderedThreadSafeQueue& operator=(OrderedThreadSafeQueue&&) = delete;

  // Push the `value` to the queue that is associated with the `index`. This
  // call blocks, until `push` has been called for all indices in `[0, ...,
  // index - 1]` or until `finish` was called. The remaining behavior is
  // equal to `ThreadSafeQueue::push`.
  bool push(size_t index, T value) {
    std::unique_lock lock{mutex_};
    cv_.wait(lock,
             [this, index]() { return index == nextIndex_ || finish_.test(); });
    if (finish_.test()) {
      return false;
    }
    ++nextIndex_;
    bool result = queue_.push(std::move(value));
    lock.unlock();
    cv_.notify_all();
    return result;
  }

  // Same as the function above, but the two arguments are passed in as a
  // `std::pair`.
  bool push(std::pair<size_t, T> indexAndValue) {
    return push(indexAndValue.first, std::move(indexAndValue.second));
  }

  // See `ThreadSafeQueue` for details.
  void pushException(std::exception_ptr exception) {
    queue_.pushException(std::move(exception));
    finish_.test_and_set();
    cv_.notify_all();
  }

  // See `ThreadSafeQueue` for details.
  void finish() noexcept {
    // It is crucial that this function never throws, so that we can safely call
    // it unconditionally in destructors to prevent deadlocks. Should the
    // implementation ever change, make sure that it is still `noexcept`.
    queue_.finish();
    finish_.test_and_set();
    cv_.notify_all();
  }

  // See `ThreadSafeQueue` for details.
  ~OrderedThreadSafeQueue() { finish(); }

  // See `ThreadSafeQueue` for details. All the returned values will be in
  // ascending consecutive order wrt the index with which they were pushed.
  std::optional<T> pop() { return queue_.pop(); }
};

// A concept for one of the thread-safe queue types above
template <typename T>
concept IsThreadsafeQueue =
    ad_utility::similarToInstantiation<T, ThreadSafeQueue> ||
    ad_utility::similarToInstantiation<T, OrderedThreadSafeQueue>;

namespace detail {
// A helper function for setting up a producer for one of the threadsafe
// queues above. Takes a reference to a queue and a `producer`. The producer
// must return `std::optional<somethingThatCanBePushedToTheQueue>`. The producer
// is called repeatedly, and the resulting values are pushed to the queue. If
// the producer returns `nullopt`, `numThreads` is decremented, and the queue is
// finished if `numThreads <= 0`. All exceptions that happen during the
// execution of `producer` are propagated to the queue.
template <IsThreadsafeQueue Queue, std::invocable Producer>
auto makeQueueTask(Queue& queue, Producer producer,
                   std::atomic<int64_t>& numThreads) {
  return [&queue, producer = std::move(producer), &numThreads] {
    try {
      while (auto opt = producer()) {
        if (!queue.push(std::move(opt.value()))) {
          break;
        }
      }
    } catch (...) {
      try {
        queue.pushException(std::current_exception());
      } catch (...) {
        queue.finish();
      }
    }
    --numThreads;
    if (numThreads <= 0) {
      queue.finish();
    }
  };
}
}  // namespace detail

// This helper function makes the usage of the (Ordered)ThreadSafeQueue above
// much easier. It takes the size of the queue, the number of producer threads,
// and a `producer` (a callable that produces values). The `producer` is called
// repeatedly in `numThreads` many concurrent threads. It needs to return
// `std::optional<SomethingThatCanBePushedToTheQueue>` and has the following
// semantics: If `nullopt` is returned, then the thread is finished. The queue
// is finished, when all the producer threads have finished by yielding
// `nullopt`, or if any call to `producer` in any thread throws an
// exception. In that case the exception is propagated to the resulting
// generator. The resulting generator yields all the values that have been
// pushed to the queue.
template <typename Queue>
cppcoro::generator<typename Queue::value_type> queueManager(size_t queueSize,
                                                            size_t numThreads,
                                                            auto producer) {
  Queue queue{queueSize};
  AD_CONTRACT_CHECK(numThreads > 0u);
  std::vector<ad_utility::JThread> threads;
  std::atomic<int64_t> numUnfinishedThreads{static_cast<int64_t>(numThreads)};
  absl::Cleanup queueFinisher{[&queue] { queue.finish(); }};
  for ([[maybe_unused]] auto i : std::views::iota(0u, numThreads)) {
    threads.emplace_back(
        detail::makeQueueTask(queue, producer, numUnfinishedThreads));
  }

  while (auto opt = queue.pop()) {
    co_yield (opt.value());
  }
}

}  // namespace ad_utility::data_structures
