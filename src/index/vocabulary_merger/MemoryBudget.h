// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_MEMORYBUDGET_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_MEMORYBUDGET_H

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <utility>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/NoCopyNoMove.h"

// The memory budget of the stages behind the merge of the partial vocabularies
// (see `index/vocabulary_merger/SegmentPipeline.h`). This is not part of the
// public interface of `index/VocabularyMerger.h`.
namespace ad_utility::vocabulary_merger::detail {

// A thread-safe account of the memory that the data of the stages behind the
// merge occupies, against a fixed `limit`. Memory is accounted for by
// `Reservation`s, which return their bytes when they are destroyed, typically
// together with the data they account for (for example in the deleter of a
// `std::shared_ptr`, so that the bytes are returned by whichever stage drops
// the data last).
//
// A reservation itself never blocks and may exceed the limit: the data often
// exists already when its size is known (for example the output of a task).
// Instead, the single thread that feeds new data into the stages calls
// `waitUntilFits` before it does so, which blocks while the budget is
// exhausted, so that the memory stays close to the limit.
//
// NOTE: The budget must outlive all of its reservations.
class MemoryBudget : public ad_utility::NoCopyNoMove {
 private:
  size_t limit_;
  mutable std::mutex mutex_;
  std::condition_variable memoryWasReleased_;
  size_t used_ = 0;
  // The maximum of `used_` so far.
  size_t peak_ = 0;

 public:
  // The RAII handle of reserved memory, see above. A default-constructed or
  // moved-from reservation holds no memory.
  class Reservation {
   private:
    MemoryBudget* budget_ = nullptr;
    size_t numBytes_ = 0;

   public:
    Reservation() = default;
    Reservation(MemoryBudget& budget, size_t numBytes)
        : budget_{&budget}, numBytes_{numBytes} {}
    Reservation(Reservation&& other) noexcept
        : budget_{std::exchange(other.budget_, nullptr)},
          numBytes_{std::exchange(other.numBytes_, 0)} {}
    Reservation& operator=(Reservation&& other) noexcept {
      if (this != &other) {
        release();
        budget_ = std::exchange(other.budget_, nullptr);
        numBytes_ = std::exchange(other.numBytes_, 0);
      }
      return *this;
    }
    ~Reservation() { release(); }

    // Reserve `numBytes` more (never blocks). The reservation must belong to
    // a budget.
    void grow(size_t numBytes) {
      AD_CONTRACT_CHECK(budget_ != nullptr);
      budget_->add(numBytes);
      numBytes_ += numBytes;
    }

    // The number of bytes that this reservation holds.
    size_t numBytes() const { return numBytes_; }

    // Return the bytes to the budget. Afterwards, the reservation is empty.
    void release() {
      if (budget_ != nullptr) {
        budget_->subtract(std::exchange(numBytes_, 0));
        budget_ = nullptr;
      }
    }
  };

  // Construct a budget of `limit` bytes.
  explicit MemoryBudget(ad_utility::MemorySize limit)
      : limit_{limit.getBytes()} {}

  // Reserve `size` (never blocks, see above).
  Reservation reserve(ad_utility::MemorySize size) {
    add(size.getBytes());
    return Reservation{*this, size.getBytes()};
  }

  // Block until `size` more fits into the budget, or until `stopWaiting()`
  // returns true. Return whether `size` fits. The `stopWaiting` predicate
  // tells whether waiting is pointless, because nothing that holds a
  // reservation will release it without the help of the waiting thread. It is
  // evaluated with the lock held, whenever memory is released and at least
  // every `pollInterval`, because it may become true without any release.
  template <typename F>
  bool waitUntilFits(
      ad_utility::MemorySize size, F stopWaiting,
      std::chrono::milliseconds pollInterval = std::chrono::milliseconds{10}) {
    std::unique_lock lock{mutex_};
    while (!fits(size.getBytes())) {
      if (stopWaiting()) {
        return false;
      }
      memoryWasReleased_.wait_for(lock, pollInterval);
    }
    return true;
  }

  // The limit, the currently reserved memory, and the maximum of the latter
  // so far.
  ad_utility::MemorySize limit() const {
    return ad_utility::MemorySize::bytes(limit_);
  }
  ad_utility::MemorySize used() const {
    std::lock_guard lock{mutex_};
    return ad_utility::MemorySize::bytes(used_);
  }
  ad_utility::MemorySize peak() const {
    std::lock_guard lock{mutex_};
    return ad_utility::MemorySize::bytes(peak_);
  }

 private:
  // Whether `numBytes` more fit. NOTE: If nothing is reserved, everything
  // fits, so that a single reservation that is larger than the limit cannot
  // block forever.
  bool fits(size_t numBytes) const {
    return used_ == 0 || used_ + numBytes <= limit_;
  }

  void add(size_t numBytes) {
    std::lock_guard lock{mutex_};
    used_ += numBytes;
    peak_ = std::max(peak_, used_);
  }

  void subtract(size_t numBytes) {
    {
      std::lock_guard lock{mutex_};
      AD_CORRECTNESS_CHECK(numBytes <= used_);
      used_ -= numBytes;
    }
    memoryWasReleased_.notify_all();
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_MEMORYBUDGET_H
