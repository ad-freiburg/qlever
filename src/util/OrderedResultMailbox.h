// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ORDEREDRESULTMAILBOX_H
#define QLEVER_SRC_UTIL_ORDEREDRESULTMAILBOX_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility {

// The hand-off of the results of a fixed number of concurrent tasks (the
// "slots") to a single consumer that takes them in the order of the slots,
// together with the error and stop state that the tasks and the consumer
// share. The tasks `deliver` their results in an arbitrary order, from
// arbitrary threads, and the consumer `take`s them one by one, waiting for
// each of them. Any party may `storeException` (only the first exception is
// kept) or `requestStop`, after which the consumer stops waiting: `take` then
// rethrows the stored exception (if any) or fails.
template <typename T>
class OrderedResultMailbox : public NoCopyNoMove {
 private:
  mutable std::mutex mutex_;
  std::condition_variable conditionVariable_;
  // The results that the consumer has not taken yet. A slot that is done may
  // still hold no result, see `deliver`.
  std::vector<std::optional<T>> results_;
  std::vector<bool> isDone_;
  std::exception_ptr exception_;
  std::atomic<bool> wasStopRequested_{false};

 public:
  // Create a mailbox without any slots, see `setNumSlots`.
  OrderedResultMailbox() = default;

  // Set the number of slots. Has to be called before any of the other
  // functions that refer to a slot.
  void setNumSlots(size_t numSlots) {
    std::lock_guard lock{mutex_};
    results_.clear();
    results_.resize(numSlots);
    isDone_.assign(numSlots, false);
  }

  // Mark the slot with the given `index` as done, with the given `result`. A
  // task that has failed or was stopped delivers `std::nullopt`, so that the
  // consumer doesn't wait for it forever (it then sees the exception or the
  // stop, see `take`).
  void deliver(size_t index, std::optional<T> result) {
    {
      std::lock_guard lock{mutex_};
      AD_CONTRACT_CHECK(!isDone_.at(index),
                        "A result was delivered twice to the same slot");
      results_.at(index) = std::move(result);
      isDone_.at(index) = true;
    }
    conditionVariable_.notify_all();
  }

  // Wait until the slot with the given `index` is done, then return its result
  // and remove it from the mailbox. While waiting, call `onTick` (without
  // holding the lock) every `tickInterval` (e.g. to report the progress of
  // the tasks). If an exception has been stored before or while waiting,
  // rethrow it. Throw if a stop has been requested before the slot was done.
  template <typename OnTick>
  T take(size_t index, std::chrono::milliseconds tickInterval,
         OnTick&& onTick) {
    std::unique_lock lock{mutex_};
    auto isDoneOrStopped = [this, index]() {
      return isDone_.at(index) || exception_ != nullptr ||
             wasStopRequested_.load();
    };
    while (!isDoneOrStopped()) {
      conditionVariable_.wait_for(lock, tickInterval);
      lock.unlock();
      onTick();
      lock.lock();
    }
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    AD_CORRECTNESS_CHECK(isDone_.at(index) && !wasStopRequested_.load(),
                         "The tasks were stopped before slot ", index,
                         " was done");
    auto result = std::move(results_.at(index));
    results_.at(index).reset();
    AD_CORRECTNESS_CHECK(result.has_value());
    return std::move(result).value();
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
      // predicate in `take` doesn't miss the notification below.
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

#endif  // QLEVER_SRC_UTIL_ORDEREDRESULTMAILBOX_H
