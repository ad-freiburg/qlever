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
// IMPORTANT: The decision whether to clone is based on the reference count of
// the pointee. It is therefore undefined behavior to copy a `CopyOnWritePtr`
// while another thread calls `write()` on a `CopyOnWritePtr` that shares the
// same pointee. Copies and writes have to be synchronized externally (in QLever
// typically by the write lock of the structure that owns the
// `CopyOnWritePtr`s). Reading via different copies is always safe, also
// concurrently with a `write()` on one of them, because that `write()` operates
// on a clone.
template <typename T>
class CopyOnWritePtr {
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
