// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_REBUILDTRACKER_H
#define QLEVER_SRC_ENGINE_REBUILDTRACKER_H

#include <atomic>
#include <cstdint>
#include <optional>

#include "util/ResetWhenMoved.h"

namespace ad_utility {

// Keeps track of whether an index rebuild is running right now, and if one is,
// which rebuild it is. Rebuilds are numbered starting at 1, and the numbering
// starts again each time the server is started.
class RebuildTracker {
 public:
  // While an object of this class is alive, the rebuild it belongs to counts
  // as running. When it is destroyed, the rebuild is marked as finished.
  class Guard {
   public:
    ~Guard() {
      if (tracker_.value_ != nullptr) {
        // The number has to be cleared first. Once `running_` is false the next
        // rebuild can start, and clearing afterward would wipe out its number.
        tracker_.value_->currentId_.store(0);
        tracker_.value_->running_.store(false);
      }
    }

    // Handing the guard on is fine, because `ResetWhenMoved` empties the one
    // that was handed over from.
    Guard(Guard&&) = default;

    // Copying would mean two objects end the same rebuild, and assigning over
    // a guard would throw away a rebuild that has not been ended yet.
    Guard& operator=(Guard&&) = delete;
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;

   private:
    friend class RebuildTracker;
    explicit Guard(RebuildTracker* tracker) : tracker_(tracker) {}

    // The tracker to tell when the rebuild is over. It is null if this object
    // was moved out of, because then the object it was moved into does that
    // job instead.
    ResetWhenMoved<RebuildTracker*, nullptr> tracker_;
  };

  // Start a rebuild, unless one is already running. The rebuild counts as
  // running for as long as the returned guard is alive, so the result has to
  // be stored in a variable.
  [[nodiscard]] std::optional<Guard> tryBegin() {
    if (running_.exchange(true)) {
      return std::nullopt;
    }
    currentId_.store(nextId_.fetch_add(1) + 1);
    return Guard{this};
  }

  // The number (id) of the rebuild that is currently running.
  // Nothing if no rebuild is currently running.
  [[nodiscard]] std::optional<uint64_t> poll() const {
    auto id = currentId_.load();
    return id == 0 ? std::nullopt : std::optional(id);
  }

 private:
  // True while a rebuild is running. This is what keeps a second rebuild from
  // starting.
  std::atomic<bool> running_{false};

  // The number of the rebuild that is running, or 0 if none is running. Other
  // threads read this while the rebuild writes it, so it has to be atomic.
  std::atomic<uint64_t> currentId_{0};

  // Counts the rebuilds started so far. Only a caller that has claimed
  // `running_` touches it, so two rebuilds never get the same number.
  std::atomic<uint64_t> nextId_{0};
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_REBUILDTRACKER_H
