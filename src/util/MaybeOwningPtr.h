// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_MAYBEOWNINGPTR_H
#define QLEVER_SRC_UTIL_MAYBEOWNINGPTR_H

#include <memory>

#include "util/Exception.h"

// The lifetime checks of `MaybeOwningPtr` are enabled under the same condition
// as `AD_EXPENSIVE_CHECK` (see `util/Exception.h`). They change the layout of
// `MaybeOwningPtr` and of all classes that contain it. These classes are
// therefore declared inside the inline namespace
// `QL_LIFETIME_CHECKS_NAMESPACE`, the name of which depends on this condition.
// Mixing translation units that were compiled with and without the checks then
// leads to a linker error instead of a silent ODR violation.
#if (!defined(NDEBUG) || defined(AD_ENABLE_EXPENSIVE_CHECKS))
#define QL_ENABLE_LIFETIME_CHECKS
#define QL_LIFETIME_CHECKS_NAMESPACE withLifetimeChecks
#else
#define QL_LIFETIME_CHECKS_NAMESPACE withoutLifetimeChecks
#endif

namespace ad_utility {
inline namespace QL_LIFETIME_CHECKS_NAMESPACE {

// A pointer to a `T` that either shares the ownership of the `T` (like a
// `std::shared_ptr`), or doesn't own it at all (like a raw pointer). Copying an
// owning `MaybeOwningPtr` increments the reference count of the `shared_ptr`,
// which is a contended atomic operation if many threads copy pointers to the
// same object. Copying a non-owning `MaybeOwningPtr` costs no atomic operation.
// A non-owning `MaybeOwningPtr` is typically obtained via `nonOwningCopy()`
// from an owning one, which then has to outlive all the non-owning copies. When
// the lifetime checks are enabled (see above), this is verified when a
// non-owning copy is destroyed or overwritten.
//
// Similar to `MemoryLimitTracker` and the allocators, the move operations copy
// rather than steal, so that a moved-from `MaybeOwningPtr` still keeps its
// object alive and remains usable.
template <typename T>
class MaybeOwningPtr {
 private:
  // The object, never `nullptr`.
  T* ptr_ = nullptr;
  // The owner of `*ptr_`, or `nullptr` if this pointer is non-owning.
  std::shared_ptr<T> owner_;
#ifdef QL_ENABLE_LIFETIME_CHECKS
  // For a non-owning pointer that was obtained from an owning one, a weak
  // pointer to the owned object, which must not have expired when this
  // pointer is destroyed or overwritten.
  std::weak_ptr<T> lifetimeCheck_;
  bool isLifetimeChecked_ = false;
#endif

 public:
  // Construct an owning pointer that shares the ownership of `owner`.
  explicit MaybeOwningPtr(std::shared_ptr<T> owner)
      : ptr_{owner.get()}, owner_{std::move(owner)} {
    AD_CONTRACT_CHECK(ptr_ != nullptr);
  }

  // Construct a non-owning pointer to `ptr`, whose lifetime is managed
  // elsewhere (e.g. a `static` object provided by the platform), and therefore
  // cannot be checked.
  static MaybeOwningPtr makeUncheckedNonOwning(T* ptr) {
    AD_CONTRACT_CHECK(ptr != nullptr);
    MaybeOwningPtr result;
    result.ptr_ = ptr;
    return result;
  }

  // Return a non-owning copy of this pointer. The owner of the object (i.e. an
  // owning `MaybeOwningPtr` from which this pointer was copied, or one of its
  // copies) has to outlive the result.
  MaybeOwningPtr nonOwningCopy() const {
    MaybeOwningPtr result;
    result.ptr_ = ptr_;
#ifdef QL_ENABLE_LIFETIME_CHECKS
    if (owner_ != nullptr) {
      result.lifetimeCheck_ = owner_;
      result.isLifetimeChecked_ = true;
    } else {
      result.lifetimeCheck_ = lifetimeCheck_;
      result.isLifetimeChecked_ = isLifetimeChecked_;
    }
#endif
    return result;
  }

  // Copying shares the ownership if this pointer is owning, and checks the
  // lifetime of the pointer that is overwritten.
  MaybeOwningPtr(const MaybeOwningPtr&) = default;
  MaybeOwningPtr& operator=(const MaybeOwningPtr& other) {
    checkLifetime();
    ptr_ = other.ptr_;
    owner_ = other.owner_;
#ifdef QL_ENABLE_LIFETIME_CHECKS
    lifetimeCheck_ = other.lifetimeCheck_;
    isLifetimeChecked_ = other.isLifetimeChecked_;
#endif
    return *this;
  }

  // The move operations copy, see the class comment.
  MaybeOwningPtr(MaybeOwningPtr&& other) noexcept : MaybeOwningPtr(other) {}
  MaybeOwningPtr& operator=(MaybeOwningPtr&& other) noexcept {
    return *this = other;
  }

  // Check that the owner of a non-owning pointer is still alive.
  ~MaybeOwningPtr() { checkLifetime(); }

  // Access the object.
  T* get() const noexcept { return ptr_; }
  T& operator*() const noexcept { return *ptr_; }
  T* operator->() const noexcept { return ptr_; }

  // Return true iff this pointer shares the ownership of its object.
  bool isOwning() const noexcept { return owner_ != nullptr; }

 private:
  MaybeOwningPtr() = default;

  // Terminate the program if this is a non-owning pointer, the owner of which
  // was already destroyed. Such a pointer was dangling at least since the
  // destruction of its owner.
  void checkLifetime() const noexcept {
#ifdef QL_ENABLE_LIFETIME_CHECKS
    // A violated check throws, which terminates the program, because this
    // function is `noexcept`.
    AD_CORRECTNESS_CHECK(
        !isLifetimeChecked_ || !lifetimeCheck_.expired(),
        "A non-owning `MaybeOwningPtr` (e.g. a non-owning copy of an "
        "allocator) outlived the owner of its object");
#endif
  }
};

}  // namespace QL_LIFETIME_CHECKS_NAMESPACE
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MAYBEOWNINGPTR_H
