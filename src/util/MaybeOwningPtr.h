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
#include <optional>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"

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
// There are no move operations, so moving falls back to copying. This way, a
// moved-from `MaybeOwningPtr` still keeps its object alive and remains usable,
// which is required e.g. for the allocators that hold a `MaybeOwningPtr`.
//
// NOTE: The pointer is stored separately from `owner_` (instead of e.g. a
// `std::variant` of a `shared_ptr` and a raw pointer), so that `get()` needs no
// branch.
template <typename T>
class MaybeOwningPtr {
 private:
  // The object, never `nullptr`.
  T* ptr_ = nullptr;
  // The owner of `*ptr_`, or `nullptr` if this pointer is non-owning.
  std::shared_ptr<T> owner_;
#ifdef QL_ENABLE_LIFETIME_CHECKS
  // For a non-owning pointer that was obtained from an owning one, a weak
  // pointer to the owned object, which must not have expired when this pointer
  // is destroyed or overwritten. It is `std::nullopt` for owning pointers and
  // for pointers from `makeUncheckedNonOwning`, which have no owner that could
  // be checked. (An empty `weak_ptr` would not work for the latter, because it
  // is reported as expired.)
  std::optional<std::weak_ptr<T>> lifetimeCheck_;
#endif

 public:
  // Construct an owning pointer that shares the ownership of `owner`.
  explicit MaybeOwningPtr(std::shared_ptr<T> owner)
      : ptr_{owner.get()}, owner_{std::move(owner)} {
    AD_CONTRACT_CHECK(ptr_ != nullptr);
  }

  // Copying shares the ownership if this pointer is owning. Copy assignment
  // also checks the lifetime of the pointer that is overwritten. As the move
  // operations are not declared, they fall back to these.
  MaybeOwningPtr(const MaybeOwningPtr& other) noexcept { copyFrom(other); }
  MaybeOwningPtr& operator=(const MaybeOwningPtr& other) noexcept {
    checkLifetime();
    copyFrom(other);
    return *this;
  }

  // Check that the owner of a non-owning pointer is still alive.
  ~MaybeOwningPtr() { checkLifetime(); }

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
      result.lifetimeCheck_ = std::weak_ptr<T>{owner_};
    } else {
      result.lifetimeCheck_ = lifetimeCheck_;
    }
#endif
    return result;
  }

  // Access the object.
  T* get() const noexcept { return ptr_; }
  T& operator*() const noexcept { return *ptr_; }
  T* operator->() const noexcept { return ptr_; }

  // Return true iff this pointer shares the ownership of its object.
  bool isOwning() const noexcept { return owner_ != nullptr; }

 private:
  MaybeOwningPtr() = default;

  // Copy all members from `other`. Copying a `shared_ptr` or a `weak_ptr`
  // never throws.
  void copyFrom(const MaybeOwningPtr& other) noexcept {
    ptr_ = other.ptr_;
    owner_ = other.owner_;
#ifdef QL_ENABLE_LIFETIME_CHECKS
    lifetimeCheck_ = other.lifetimeCheck_;
#endif
  }

  // Terminate the program if this is a non-owning pointer, the owner of which
  // was already destroyed. Such a pointer was dangling at least since the
  // destruction of its owner.
  void checkLifetime() const noexcept {
#ifdef QL_ENABLE_LIFETIME_CHECKS
    ad_utility::terminateIfThrows(
        [this]() {
          AD_CORRECTNESS_CHECK(
              !lifetimeCheck_.has_value() || !lifetimeCheck_->expired(),
              "A non-owning `MaybeOwningPtr` (e.g. a non-owning copy of an "
              "allocator) outlived the owner of its object");
        },
        "Checking the lifetime of a non-owning `MaybeOwningPtr`");
#endif
  }
};

}  // namespace QL_LIFETIME_CHECKS_NAMESPACE
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MAYBEOWNINGPTR_H
