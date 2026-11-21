// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_THREADSAFEREORDERBUFFER_H
#define QLEVER_SRC_UTIL_THREADSAFEREORDERBUFFER_H

#include <absl/functional/function_ref.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility {

// The exception that `ThreadSafeReorderBuffer::takeNext` throws if a stop was
// requested (without an exception being stored) before all the slots were
// taken.
class ThreadSafeReorderBufferStopped : public std::runtime_error {
 public:
  explicit ThreadSafeReorderBufferStopped(size_t nextIndex)
      : std::runtime_error{"The tasks were stopped before slot " +
                           std::to_string(nextIndex) +
                           " of the `ThreadSafeReorderBuffer` was taken"} {}
};

// A reorder buffer: the hand-off of the results of a fixed number of
// concurrent tasks (the "slots"), which complete in an arbitrary order, to a
// single consumer that takes them in the order of the slots, together with the
// error and stop state that the tasks and the consumer share. The tasks
// `deliver` their results in an arbitrary order, from arbitrary threads, and
// the consumer `takeNext`s them one by one, waiting for each of them. Any party
// may `storeException` (only the first exception is kept) or `requestStop`,
// after which the consumer stops waiting: `takeNext` then rethrows the stored
// exception (if any) or throws a `ThreadSafeReorderBufferStopped`.
//
// MEMORY: `deliver` never blocks, so there is no backpressure, and a result
// stays in the buffer until the consumer has taken all the slots before it.
// In the worst case (the slots are delivered in reverse order), all the
// results are held in memory at the same time.
//
// BLOCKING: `takeNext` blocks its thread (on a `std::condition_variable`)
// until the next slot has been delivered. It must therefore not be called on
// a thread of a pool that also has to run the tasks that deliver the results,
// otherwise the pool may starve or even deadlock.
//
// NOTE 1: `ad_utility::data_structures::OrderedThreadSafeQueue` (see
// `ThreadSafeQueue.h`) also hands values over in the order of their indices,
// but with the opposite trade-off: there, `push(n, value)` blocks the producer
// until the values for all the indices before `n` have been pushed, which
// bounds the memory. This class is meant for producers that must never block,
// typically coroutines on the same thread pool. If all the threads of the pool
// were blocked in `push` for later indices, the task for the next index could
// never run. Here, the producers deliver immediately, and only the consumer
// (on a dedicated thread) waits.
//
// NOTE 2: In addition, `takeNext` wakes up regularly to call `onTick`, a stop
// can be told apart from the regular end, and the producers can query
// `stopRequested`.
template <typename T>
class ThreadSafeReorderBuffer : public NoCopyNoMove {
 private:
  mutable std::mutex mutex_;
  std::condition_variable conditionVariable_;
  // The results that the consumer has not taken yet. A slot holds a value iff
  // it has been delivered and not taken yet.
  std::vector<std::optional<T>> results_;
  // The index of the slot that `takeNext` returns next.
  size_t nextIndex_ = 0;
  // The index of the slot that the consumer is currently waiting for (if
  // any), so that `deliver` only wakes it up for that slot.
  std::optional<size_t> waitingForIndex_;
  std::exception_ptr exception_;
  std::atomic<bool> wasStopRequested_{false};

 public:
  // Create a buffer without any slots, see `setNumSlots`.
  ThreadSafeReorderBuffer() = default;

  // Set the number of slots. Has to be called before any of the other
  // functions that refer to a slot.
  void setNumSlots(size_t numSlots) {
    std::lock_guard lock{mutex_};
    results_.clear();
    results_.resize(numSlots);
    nextIndex_ = 0;
  }

  // Deliver the `result` of the slot with the given `index`. Each slot has to
  // be delivered exactly once. A task that has failed or was stopped doesn't
  // deliver anything, it calls `storeException` or `requestStop` instead,
  // which also wakes up the consumer.
  void deliver(size_t index, T result) {
    bool consumerIsWaitingForIndex = false;
    {
      std::lock_guard lock{mutex_};
      AD_CONTRACT_CHECK(index < results_.size(), "Slot ", index,
                        " is out of range");
      AD_CONTRACT_CHECK(index >= nextIndex_ && !results_[index].has_value(),
                        "A result was delivered twice to slot ", index);
      results_[index] = std::move(result);
      consumerIsWaitingForIndex = waitingForIndex_ == index;
    }
    if (consumerIsWaitingForIndex) {
      conditionVariable_.notify_one();
    }
  }

  // Wait until the next slot has been delivered, then return its result and
  // remove it from the buffer. Return `std::nullopt` once all the slots have
  // been taken. While waiting, call `onTick` (without holding the lock) after
  // every wakeup (e.g. to report the progress of the tasks). The consumer
  // wakes up at the latest every `tickInterval`, but possibly more often (on
  // the delivery of the next slot, on a stop, or spuriously). If an exception
  // has been stored before or while waiting, rethrow it. Otherwise, if a stop
  // has been requested, throw a `ThreadSafeReorderBufferStopped`. Both take
  // precedence over a result that is already there and over the end.
  std::optional<T> takeNext(std::chrono::milliseconds tickInterval,
                            absl::FunctionRef<void()> onTick) {
    std::unique_lock lock{mutex_};
    // The consumer can stop waiting if the next slot has been delivered, if
    // all the slots have been taken, if an exception has been stored, or if a
    // stop has been requested.
    auto canStopWaiting = [this]() {
      return nextIndex_ == results_.size() ||
             results_[nextIndex_].has_value() || exception_ != nullptr ||
             wasStopRequested_.load();
    };
    while (!canStopWaiting()) {
      waitingForIndex_ = nextIndex_;
      conditionVariable_.wait_for(lock, tickInterval);
      waitingForIndex_.reset();
      lock.unlock();
      onTick();
      lock.lock();
    }
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    if (wasStopRequested_.load()) {
      throw ThreadSafeReorderBufferStopped{nextIndex_};
    }
    if (nextIndex_ == results_.size()) {
      return std::nullopt;
    }
    auto& slot = results_[nextIndex_];
    std::optional<T> result = std::move(slot);
    slot.reset();
    ++nextIndex_;
    return result;
  }

  // Store the `exception` (if it is not `nullptr` and is the first one), and
  // request a stop. Return `true` iff the `exception` is not `nullptr`.
  bool storeException(std::exception_ptr exception) {
    if (exception == nullptr) {
      return false;
    }
    {
      std::lock_guard lock{mutex_};
      if (exception_ == nullptr) {
        exception_ = std::move(exception);
      }
    }
    requestStop();
    return true;
  }

  // Request a stop, and wake up the consumer.
  void requestStop() {
    {
      // NOTE: The lock makes sure that a consumer that has just checked the
      // predicate in `takeNext` doesn't miss the notification below.
      std::lock_guard lock{mutex_};
      wasStopRequested_.store(true);
    }
    conditionVariable_.notify_all();
  }

  // Return `true` iff a stop has been requested (or an exception stored).
  bool stopRequested() const noexcept { return wasStopRequested_.load(); }

  // Rethrow the stored exception, if there is one.
  void rethrowIfException() const {
    std::lock_guard lock{mutex_};
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_THREADSAFEREORDERBUFFER_H
