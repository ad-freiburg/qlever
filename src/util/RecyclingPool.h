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
#include <type_traits>
#include <vector>

#include "util/ExceptionHandling.h"
#include "util/Synchronized.h"

namespace ad_utility {

// A thread-safe pool of objects (typically large buffers) that are no longer
// needed and can therefore be reused. Note: The allocation of a fresh buffer is
// cheap in itself, because Linux only backs the memory of a process with
// physical pages once that memory is actually touched. The expensive part are
// the many page faults when a fresh buffer is filled for the first time, which
// are avoided by reusing the (already touched) memory of a buffer. An object
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

  // Moving an object out of the pool must not throw, so that `take` can't
  // leave the pool in an inconsistent state.
  static_assert(std::is_nothrow_move_constructible_v<T>);

 public:
  // Return an object from the pool if there is one, and `makeNew()` otherwise.
  // Note: The returned object is in the state in which it was given back, so
  // the caller typically has to reset it (e.g. `clear()` a buffer). If
  // `makeNew()` throws, then the pool remains unchanged.
  template <typename MakeNew>
  T take(const MakeNew& makeNew) {
    {
      auto state = state_.wlock();
      if (!state->objects_.empty()) {
        T result = std::move(state->objects_.back());
        state->objects_.pop_back();
        ++state->numTaken_;
        return result;
      }
    }
    // Create the new object without holding the lock, and only count it as
    // taken once its creation has succeeded.
    T result = std::invoke(makeNew);
    ++state_.wlock()->numTaken_;
    return result;
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
    // The `Holder` gives its object back to the pool when it is destroyed. The
    // returned `shared_ptr` points to the object, but owns the `Holder`.
    struct Holder {
      std::shared_ptr<RecyclingPool> pool_;
      T object_;
      Holder(std::shared_ptr<RecyclingPool> pool, T object)
          : pool_{std::move(pool)}, object_{std::move(object)} {}
      ~Holder() {
        terminateIfThrows([this]() { pool_->giveBack(std::move(object_)); },
                          "while giving an object back to a `RecyclingPool`");
      }
    };
    auto holder = std::make_shared<Holder>(std::move(pool), std::move(object));
    T* objectPtr = &holder->object_;
    return std::shared_ptr<T>{std::move(holder), objectPtr};
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RECYCLINGPOOL_H
