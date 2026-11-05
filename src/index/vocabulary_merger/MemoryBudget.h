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
#include "util/UniqueCleanup.h"

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
// A reservation itself never waits for memory and may exceed the limit: the
// data often exists already when its size is known (for example the output of
// a task). Instead, the single thread that feeds new data into the stages
// calls `waitUntilFits` before it does so, which blocks while the budget is
// exhausted, so that the memory stays close to the limit.
//
// The functions that acquire the `mutex_` only do short and trivial work while
// they hold it, so apart from `waitUntilFits` none of them blocks for long.
//
// NOTE: The budget must outlive all of its reservations.
class MemoryBudget : public ad_utility::NoCopyNoMove {
 private:
  const size_t limit_;
  mutable std::mutex mutex_;
  std::condition_variable memoryWasReleased_;
  size_t used_ = 0;
  // The maximum of `used_` so far.
  size_t peak_ = 0;

 public:
  // The RAII handle of reserved memory, see above. A moved-from reservation
  // holds no memory.
  class Reservation {
   private:
    // The budget and the reserved bytes, which the `Release` returns to the
    // budget when the reservation is destroyed, released, or overwritten by a
    // move assignment.
    struct State {
      MemoryBudget* budget_;
      size_t numBytes_;
    };
    struct Release {
      void operator()(State&& state) const {
        state.budget_->subtract(state.numBytes_);
      }
    };
    ad_utility::unique_cleanup::UniqueCleanup<State, Release> state_;

    // Only `MemoryBudget::reserve` creates reservations, after it has added
    // the bytes to the budget (so that every reservation returns exactly what
    // was added for it).
    friend class MemoryBudget;
    Reservation(MemoryBudget& budget, size_t numBytes)
        : state_{State{&budget, numBytes}, Release{}} {}

   public:
    // Reserve `numBytes` more (never waits for memory). The reservation must
    // not be empty.
    void grow(size_t numBytes) {
      AD_CONTRACT_CHECK(state_.isActive());
      state_->budget_->add(numBytes);
      state_->numBytes_ += numBytes;
    }

    // The number of bytes that this reservation holds.
    size_t numBytes() const {
      return state_.isActive() ? state_->numBytes_ : 0;
    }

    // Return the bytes to the budget. Afterwards, the reservation is empty.
    void release() { std::move(state_).runNowIfActive(); }
  };

  // Construct a budget of `limit` bytes.
  explicit MemoryBudget(MemorySize limit) : limit_{limit.getBytes()} {}

  // Reserve `size` (never waits for memory, see above).
  Reservation reserve(MemorySize size) {
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
      MemorySize size, F stopWaiting,
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
  // so far. The `limit()` needs no lock, because the `limit_` never changes.
  MemorySize limit() const { return MemorySize::bytes(limit_); }
  MemorySize used() const {
    std::lock_guard lock{mutex_};
    return MemorySize::bytes(used_);
  }
  MemorySize peak() const {
    std::lock_guard lock{mutex_};
    return MemorySize::bytes(peak_);
  }

 private:
  // Whether `numBytes` more fit.
  // PRECONDITION: The `mutex_` is held.
  //
  // NOTE: If nothing is reserved, everything fits, so that a single
  // reservation that is larger than the limit cannot block forever.
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
