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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <vector>

#include "util/Exception.h"

namespace ad_utility {

// Emulate a non-static `thread_local` data member, which C++ doesn't have: each
// object of this class holds one value of type `T` per thread. The value of
// the calling thread is accessed via `getIfExists()` (the hot path, which is
// lock-free and has no read-modify-write operation) or `getOrCreate()`.
//
// When a thread exits, the `OnThreadExit` callback of each object is called
// with the value of that thread, e.g. to give resources that the value holds
// back to the object. When an object is destroyed, its values in all threads
// are discarded.
//
// The implementation is similar to `folly::ThreadLocal`: each object lazily
// gets a small id, which is reused after the object is destroyed, and each
// thread has an array of values that is indexed by these ids. A global mutex
// (one per `T`) is only locked on the slow paths: when an object is used for
// the first time on a thread, when an object is destroyed, and when a thread
// exits.
template <typename T>
class ThreadLocalMember {
 public:
  // Called with the value of a thread when that thread exits. It is called
  // while the global mutex is held, and must not throw. It might also be
  // called with a default-constructed value for a thread that never accessed
  // the object.
  using OnThreadExit = std::function<void(T&)>;

 private:
  class ThreadValues;

  // The part of a thread's values that is read on the hot path. It is
  // trivially destructible and constant-initialized, so accessing it requires
  // no initialization guard.
  struct FastState {
    T* values_ = nullptr;
    size_t numValues_ = 0;
    // Set when the `ThreadValues` of this thread were destroyed at thread
    // exit. Accesses that happen afterwards (e.g. in the destructors of other
    // `thread_local` objects) then get no value.
    bool isDestroyed_ = false;
  };
  static inline thread_local FastState fastState_;

  // The global bookkeeping, all members are protected by `mutex_`.
  struct Registry {
    std::mutex mutex_;
    // The object for each id, `nullptr` for ids that are currently unused.
    std::vector<ThreadLocalMember*> objects_;
    // The ids that are currently unused, to keep the ids (and therefore the
    // per-thread arrays) small.
    std::vector<uint32_t> freeIds_;
    // The values of all threads that currently have some.
    std::vector<ThreadValues*> threads_;

    // Return the single global registry. It is intentionally never destroyed,
    // because threads may exit, and objects with static storage duration may
    // be destroyed, after the destruction of all function-local static
    // variables.
    static Registry& get() {
      static auto* registry = new Registry{};
      return *registry;
    }
  };

  static constexpr uint32_t noId = std::numeric_limits<uint32_t>::max();

  // The index of this object's value in the per-thread arrays. It is assigned
  // lazily by `getOrCreate()`, s.t. objects that are never used never lock
  // the registry.
  std::atomic<uint32_t> id_{noId};
  OnThreadExit onThreadExit_;

 public:
  // Construct with the given callback for exiting threads.
  explicit ThreadLocalMember(OnThreadExit onThreadExit)
      : onThreadExit_{std::move(onThreadExit)} {}

  // Discard the values of all threads.
  ~ThreadLocalMember() {
    // There are no concurrent users of an object that is being destroyed.
    uint32_t id = id_.load(std::memory_order_relaxed);
    if (id == noId) {
      return;
    }
    auto& registry = Registry::get();
    std::lock_guard lock{registry.mutex_};
    for (auto* thread : registry.threads_) {
      thread->reset(id);
    }
    registry.objects_[id] = nullptr;
    registry.freeIds_.push_back(id);
  }

  // The values are tied to the address of the object, so it can be neither
  // copied nor moved.
  ThreadLocalMember(const ThreadLocalMember&) = delete;
  ThreadLocalMember& operator=(const ThreadLocalMember&) = delete;

  // Return the value of the calling thread, or `nullptr` if it hasn't been
  // created yet.
  T* getIfExists() const noexcept {
    // The acquire synchronizes with the release in `getOrCreate`, which
    // happens after a previous object with the same id reset its values.
    uint32_t id = id_.load(std::memory_order_acquire);
    const auto& state = fastState_;
    return id < state.numValues_ ? state.values_ + id : nullptr;
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
    auto& registry = Registry::get();
    std::lock_guard lock{registry.mutex_};
    uint32_t id = id_.load(std::memory_order_relaxed);
    if (id == noId) {
      id = allocateId(registry);
      id_.store(id, std::memory_order_release);
    }
    ThreadValues::getForCurrentThread().ensureSize(size_t{id} + 1);
    return fastState_.values_ + id;
  }

 private:
  // Assign an unused id to this object and return it. The registry's `mutex_`
  // must be held.
  uint32_t allocateId(Registry& registry) {
    if (!registry.freeIds_.empty()) {
      uint32_t id = registry.freeIds_.back();
      registry.freeIds_.pop_back();
      registry.objects_[id] = this;
      return id;
    }
    AD_CORRECTNESS_CHECK(registry.objects_.size() < noId);
    registry.objects_.push_back(this);
    return static_cast<uint32_t>(registry.objects_.size() - 1);
  }

  // The values of a single thread. They are owned by a `thread_local` object
  // of this class, the destructor of which calls the `OnThreadExit` callbacks.
  class ThreadValues {
   private:
    std::vector<T> values_;

   public:
    // Register in the registry, the `mutex_` of which must be held.
    ThreadValues() { Registry::get().threads_.push_back(this); }

    // Call the `OnThreadExit` callbacks of all objects that are still alive
    // and unregister.
    ~ThreadValues() {
      auto& registry = Registry::get();
      std::lock_guard lock{registry.mutex_};
      size_t numIds = std::min(values_.size(), registry.objects_.size());
      for (size_t id = 0; id < numIds; ++id) {
        if (auto* object = registry.objects_[id]) {
          object->onThreadExit_(values_[id]);
        }
      }
      auto& threads = registry.threads_;
      threads.erase(std::find(threads.begin(), threads.end(), this));
      fastState_ = {nullptr, 0, true};
    }

    ThreadValues(const ThreadValues&) = delete;
    ThreadValues& operator=(const ThreadValues&) = delete;

    // Return the values of the calling thread, creating them on the first
    // call. The registry's `mutex_` must be held and the values must not have
    // been destroyed yet (see `FastState::isDestroyed_`).
    static ThreadValues& getForCurrentThread() {
      static thread_local ThreadValues values;
      return values;
    }

    // Make sure that there are at least `size` values. The registry's `mutex_`
    // must be held, because other threads might concurrently `reset` values.
    void ensureSize(size_t size) {
      if (size > values_.size()) {
        values_.resize(std::max(size, 2 * values_.size()));
        fastState_.values_ = values_.data();
        fastState_.numValues_ = values_.size();
      }
    }

    // Reset the value with the given `id`, the object of which is being
    // destroyed. The registry's `mutex_` must be held.
    void reset(uint32_t id) {
      if (id < values_.size()) {
        values_[id] = T{};
      }
    }
  };
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_THREADLOCALMEMBER_H
