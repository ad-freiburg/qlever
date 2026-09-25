// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RECYCLINGPOOL_H
#define QLEVER_SRC_UTIL_RECYCLINGPOOL_H

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include "util/Synchronized.h"

namespace ad_utility {

// A thread-safe pool of objects (typically buffers whose memory is expensive to
// allocate) that are no longer needed and can therefore be reused. An object
// is obtained via `take`, which reuses an object from the pool if there is one
// and creates a new one otherwise, and it is handed back via `giveBack` once it
// is no longer needed.
//
// The pool has no fixed capacity. Instead it keeps track of the number of
// objects that are currently taken, and `giveBack` only accepts an object while
// that number is positive (otherwise the object is simply destroyed). The pool
// thus never holds more objects than were taken at the same time at some point,
// so it automatically adapts to the number of objects that are in flight in a
// pipeline. In particular, objects that were not obtained via `take` may also
// be given back without the pool growing without bounds.
template <typename T>
class RecyclingPool {
 private:
  struct State {
    std::vector<T> objects_;
    size_t numTaken_ = 0;
  };
  Synchronized<State> state_;

 public:
  // Return an object from the pool if there is one, and `makeNew()` otherwise.
  // Note: The returned object is in the state in which it was given back, so
  // the caller typically has to reset it (e.g. `clear()` a buffer).
  template <typename MakeNew>
  T take(const MakeNew& makeNew) {
    {
      auto state = state_.wlock();
      ++state->numTaken_;
      if (!state->objects_.empty()) {
        T result = std::move(state->objects_.back());
        state->objects_.pop_back();
        return result;
      }
    }
    // Create the new object without holding the lock.
    return std::invoke(makeNew);
  }

  // Store the `object`, which is no longer needed, in the pool for reuse by a
  // later call to `take`, unless no object is currently taken (see the class
  // comment). In the latter case the `object` is simply destroyed.
  void giveBack(T object) {
    auto state = state_.wlock();
    if (state->numTaken_ == 0) {
      return;
    }
    --state->numTaken_;
    state->objects_.push_back(std::move(object));
  }

  // Return the number of objects that are currently stored in the pool.
  size_t numStoredObjects() const { return state_.rlock()->objects_.size(); }

  // Return a `shared_ptr` that owns the `object` and gives it back to the
  // `pool` as soon as the last copy of the `shared_ptr` is destroyed. The
  // `pool` is shared, so that it may safely outlive all other owners.
  static std::shared_ptr<T> makeRecyclingOwner(
      std::shared_ptr<RecyclingPool> pool, T object) {
    auto recycle = [pool = std::move(pool)](T* ptr) {
      pool->giveBack(std::move(*ptr));
      delete ptr;
    };
    return std::shared_ptr<T>{new T{std::move(object)}, std::move(recycle)};
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RECYCLINGPOOL_H
