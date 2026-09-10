// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COPYONWRITEPTR_H
#define QLEVER_SRC_UTIL_COPYONWRITEPTR_H

#include <memory>
#include <type_traits>
#include <utility>

#include "util/Exception.h"

namespace ad_utility {

// A pointer to a `T` with copy-on-write semantics. Copying a `CopyOnWritePtr`
// is cheap, because the copies share the pointee. Reading is always possible
// via the `const` accessors. Writing is only possible via `write()`, which
// clones the pointee first if (and only if) it is shared with another
// `CopyOnWritePtr`, so that a mutation never affects any copy that was made
// before. This is the building block for large structures of which frequent
// snapshots are taken, but where each mutation between two snapshots only
// touches a small part.
//
// A `CopyOnWritePtr` is never null: the default constructor creates a
// value-initialized `T`. The only exception is a moved-from `CopyOnWritePtr`,
// which may only be assigned to or destroyed.
//
// IMPORTANT NOTE: Creating a copy of a `CopyOnWritePtr` and calling `write()`
// on a `CopyOnWritePtr` that shares the same pointee must never happen
// concurrently. They have to be synchronized externally, typically by the write
// lock of the structure that owns the `CopyOnWritePtr`s. For example, the
// copies for the snapshots of the delta triples are created and the updates
// are applied under the same lock.
//
// Otherwise the following can happen: thread A calls `write()` and sees that
// the pointee is not shared, thread B then creates a copy, and thread A mutates
// the pointee in place, which thread B now reads. No copy-on-write scheme can
// prevent this: a copy that is created while a mutation is in progress observes
// a partially mutated pointee no matter how the decision to clone is made.
//
// Everything else is safe without further synchronization: reading via any copy
// (also concurrently with a `write()` on another copy, which operates on a
// clone), and also destroying a copy concurrently with a `write()`, which at
// worst causes one unnecessary clone.
template <typename T>
class CopyOnWritePtr {
  static_assert(std::is_copy_constructible_v<T>,
                "`write()` has to be able to clone the pointee");

 private:
  std::shared_ptr<T> ptr_;

 public:
  // Create a `CopyOnWritePtr` to a value-initialized `T`.
  CopyOnWritePtr() : ptr_{std::make_shared<T>()} {}

  // Create a `CopyOnWritePtr` to a copy (or a moved-in instance) of the
  // `value`.
  explicit CopyOnWritePtr(T value)
      : ptr_{std::make_shared<T>(std::move(value))} {}

  // Copying shares the pointee (this is the whole point), moving transfers
  // it and leaves the moved-from `CopyOnWritePtr` null, see the class comment.
  CopyOnWritePtr(const CopyOnWritePtr&) = default;
  CopyOnWritePtr& operator=(const CopyOnWritePtr&) = default;
  CopyOnWritePtr(CopyOnWritePtr&&) noexcept = default;
  CopyOnWritePtr& operator=(CopyOnWritePtr&&) noexcept = default;

  // Read access. Never clones.
  const T& operator*() const { return *ptr_; }
  const T* operator->() const { return ptr_.get(); }
  const T& read() const { return *ptr_; }

  // Write access. Clones the pointee first if it is shared with another
  // `CopyOnWritePtr`, so that the other `CopyOnWritePtr`s keep seeing the old
  // value. See the IMPORTANT note in the class comment for the synchronization
  // requirements.
  T& write() {
    AD_CONTRACT_CHECK(ptr_ != nullptr);
    if (isShared()) {
      ptr_ = std::make_shared<T>(*ptr_);
    }
    return *ptr_;
  }

  // Return `true` if the pointee is currently shared with at least one other
  // `CopyOnWritePtr`, that is, if the next call to `write()` would clone it.
  // Mostly useful for tests and assertions.
  bool isShared() const { return ptr_.use_count() > 1; }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COPYONWRITEPTR_H
