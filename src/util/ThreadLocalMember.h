// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_THREADLOCALMEMBER_H
#define QLEVER_SRC_UTIL_THREADLOCALMEMBER_H

#include <absl/base/attributes.h>
#include <absl/base/no_destructor.h>
#include <absl/functional/any_invocable.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
#include <type_traits>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/Synchronized.h"

namespace ad_utility {

namespace detail {

// The global bookkeeping of `ThreadLocalMember` (see below), which connects
// the objects with the values of the threads: it assigns each object a small
// id, which is reused after the object is destroyed, and it keeps track of the
// threads that have values. All member functions are threadsafe, they lock the
// single `Synchronized` state. They are only called on the slow paths of
// `ThreadLocalMember`.
//
// `Object` and `Thread` are the types of the objects and of the values of a
// thread. `Thread` must have the member functions `reset(uint32_t id)`, which
// resets the value for the object with the `id`, and `resize(size_t size)`.
template <typename Object, typename Thread>
class ThreadLocalMemberRegistry : public NoCopyNoMove {
 public:
  // The value of an id that has not been assigned yet.
  static constexpr uint32_t noId = std::numeric_limits<uint32_t>::max();

 private:
  struct State {
    // The object for each id, `nullptr` for ids that are currently unused.
    std::vector<Object*> objects_;
    // The ids that are currently unused, to keep the ids (and therefore the
    // per-thread arrays) small.
    std::vector<uint32_t> freeIds_;
    // The values of all threads that currently have some.
    std::vector<Thread*> threads_;
  };
  Synchronized<State, std::mutex> state_;

 public:
  // Return the value of `id`. If it is `noId`, then first assign an unused id
  // to `object` and store it in `id`.
  uint32_t getOrAllocateId(std::atomic<uint32_t>& id, Object* object) {
    auto state = state_.wlock();
    uint32_t result = id.load(std::memory_order_relaxed);
    if (result != noId) {
      return result;
    }
    if (!state->freeIds_.empty()) {
      result = state->freeIds_.back();
      state->freeIds_.pop_back();
      state->objects_[result] = object;
    } else {
      AD_CORRECTNESS_CHECK(state->objects_.size() < noId);
      result = static_cast<uint32_t>(state->objects_.size());
      state->objects_.push_back(object);
    }
    // The release synchronizes with the acquire in
    // `ThreadLocalMember::getIfExists`. It makes the resets in a previous
    // `releaseId` of the same id visible to the threads that see the id.
    id.store(result, std::memory_order_release);
    return result;
  }

  // Mark `id` as unused, and reset its value in all threads.
  void releaseId(uint32_t id) {
    auto state = state_.wlock();
    for (Thread* thread : state->threads_) {
      thread->reset(id);
    }
    state->objects_[id] = nullptr;
    state->freeIds_.push_back(id);
  }

  // Register the values of a thread.
  void registerThread(Thread* thread) {
    state_.wlock()->threads_.push_back(thread);
  }

  // Call `thread.resize(size)`. This has to be synchronized with
  // `releaseId`, which might reset values of the same thread concurrently.
  void resize(Thread& thread, size_t size) {
    auto lock = state_.wlock();
    thread.resize(size);
  }

  // Call `f(object, id)` for each object that is alive and has an id smaller
  // than `numIds`, and then unregister the values of the `thread`. The objects
  // cannot be destroyed concurrently, because `releaseId` waits for the lock.
  template <typename F>
  void unregisterThread(Thread* thread, size_t numIds, const F& f) {
    auto state = state_.wlock();
    numIds = std::min(numIds, state->objects_.size());
    for (size_t id = 0; id < numIds; ++id) {
      if (Object* object = state->objects_[id]) {
        f(*object, static_cast<uint32_t>(id));
      }
    }
    auto& threads = state->threads_;
    threads.erase(std::find(threads.begin(), threads.end(), thread));
  }
};

}  // namespace detail

// Emulate a non-static `thread_local` data member, which C++ doesn't have: each
// object of this class holds one value of type `T` per thread. The value of
// the calling thread is accessed via `getIfExists()` (the hot path, which is
// lock-free and has no read-modify-write operation) or `getOrCreate()`.
//
// When a thread exits, the `OnThreadExit` callback of each object is called
// with the value of that thread, e.g. to give resources that the value holds
// back to the object. When an object is destroyed, its values in all threads
// are reset to `T{}`.
//
// The implementation is similar to `folly::ThreadLocal`: each object lazily
// gets a small id, which is reused after the object is destroyed, and each
// thread has an array of values that is indexed by these ids. The ids and the
// threads are managed by a `detail::ThreadLocalMemberRegistry` (one per `T`).
template <typename T>
class ThreadLocalMember : public NoCopyNoMove {
  static_assert(std::is_default_constructible_v<T>,
                "The values of a `ThreadLocalMember` are created as `T{}`");

 public:
  // Called with the value of a thread when that thread exits. It is called
  // while the registry is locked, and must not throw. It might also
  // be called with a default-constructed value for a thread that never
  // accessed the object.
  using OnThreadExit = absl::AnyInvocable<void(T&)>;

 private:
  class ThreadValues;
  using Registry =
      detail::ThreadLocalMemberRegistry<ThreadLocalMember, ThreadValues>;

  // The part of a thread's values that is read on the hot path. It is
  // trivially destructible and constant-initialized, so accessing it requires
  // no initialization guard.
  struct FastState {
    ql::span<T> values_;
    // Set when the `ThreadValues` of this thread were destroyed at thread
    // exit. Accesses that happen afterwards (e.g. in the destructors of other
    // `thread_local` objects) then get no value.
    bool isDestroyed_ = false;
  };
  static_assert(std::is_trivially_destructible_v<FastState>);
  ABSL_CONST_INIT static inline thread_local FastState fastState_{};

  // The index of this object's value in the per-thread arrays. It is assigned
  // lazily by `getOrCreate()`, s.t. objects that are never used never lock
  // the registry.
  std::atomic<uint32_t> id_{Registry::noId};
  OnThreadExit onThreadExit_;

 public:
  // Construct with the given callback for exiting threads.
  explicit ThreadLocalMember(OnThreadExit onThreadExit = [](T&) {})
      : onThreadExit_{std::move(onThreadExit)} {}

  // Reset the values of all threads.
  ~ThreadLocalMember() {
    // There are no concurrent users of an object that is being destroyed.
    uint32_t id = id_.load(std::memory_order_relaxed);
    if (id != Registry::noId) {
      registry().releaseId(id);
    }
  }

  // Return the value of the calling thread, or `nullptr` if it hasn't been
  // created yet.
  T* getIfExists() const noexcept {
    // The acquire synchronizes with the release in `getOrAllocateId`.
    uint32_t id = id_.load(std::memory_order_acquire);
    const auto& values = fastState_.values_;
    return id < values.size() ? values.data() + id : nullptr;
  }

  // Return the value of the calling thread, and create it (as `T{}`) if it
  // doesn't exist yet. Return `nullptr` if the values of the calling thread
  // were already destroyed because the thread is exiting.
  T* getOrCreate() {
    if (T* value = getIfExists()) {
      return value;
    }
    if (fastState_.isDestroyed_) {
      return nullptr;
    }
    uint32_t id = registry().getOrAllocateId(id_, this);
    registry().resize(ThreadValues::getForCurrentThread(), size_t{id} + 1);
    return fastState_.values_.data() + id;
  }

 private:
  // Return the single registry for `T`. It is intentionally never destroyed,
  // because threads may exit, and objects with static storage duration may
  // be destroyed, after the destruction of all function-local static
  // variables.
  static Registry& registry() {
    static absl::NoDestructor<Registry> instance;
    return *instance;
  }

  // The values of a single thread. They are owned by a `thread_local` object
  // of this class, the destructor of which calls the `OnThreadExit` callbacks.
  class ThreadValues : public NoCopyNoMove {
   private:
    std::vector<T> values_;

   public:
    ThreadValues() { registry().registerThread(this); }

    // Call the `OnThreadExit` callbacks and unregister.
    ~ThreadValues() {
      registry().unregisterThread(
          this, values_.size(), [this](ThreadLocalMember& object, uint32_t id) {
            object.onThreadExit_(values_[id]);
          });
      fastState_ = {{}, true};
    }

    // Return the values of the calling thread, creating them on the first
    // call. The values must not have been destroyed yet (see
    // `FastState::isDestroyed_`).
    static ThreadValues& getForCurrentThread() {
      static thread_local ThreadValues values;
      return values;
    }

    // Make sure that there are at least `size` values. Only called via
    // `Registry::resize`.
    void resize(size_t size) {
      if (size > values_.size()) {
        values_.resize(std::max(size, 2 * values_.size()));
        fastState_.values_ = values_;
      }
    }

    // Reset the value with the given `id`, the object of which is being
    // destroyed. Only called via `Registry::releaseId`.
    void reset(uint32_t id) {
      if (id < values_.size()) {
        values_[id] = T{};
      }
    }
  };
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_THREADLOCALMEMBER_H
