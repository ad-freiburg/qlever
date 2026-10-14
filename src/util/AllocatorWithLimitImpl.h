// Copyright 2020 - 2026 The QLever Authors, in particular:
//
// 2020 Johannes Kalmbach <kalmbach@informatik.uni-freiburg.de>, UFR
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ALLOCATORWITHLIMITIMPL_H
#define QLEVER_SRC_UTIL_ALLOCATORWITHLIMITIMPL_H

#include <memory>

#include "backports/functional.h"
#include "util/MemoryLimitTracker.h"
#include "util/MemorySize/MemorySize.h"

// This header contains the *implementation* of the historical, memory-limit
// enforcing allocator. It used to live directly in `util/AllocatorWithLimit.h`
// as `ad_utility::AllocatorWithLimit`. It has been moved into the dedicated
// namespace `ad_utility::allocatorImpl` so that the public name
// `ad_utility::AllocatorWithLimit` can become an *alias* for the compile-time
// selectable `qlever::Allocator` seam (see `util/Allocator.h`). Existing code
// that uses `ad_utility::AllocatorWithLimit` therefore keeps working unchanged,
// while the concrete backend (this class or the `std::pmr`-based one) is chosen
// at compile time.
//
// The shared helper types (`detail::AllocationMemoryLeft`,
// `detail::MemoryLimitTracker`, `ClearOnAllocation`,
// `makeAllocationMemoryLeftThreadsafeObject`, ...) live in
// `util/MemoryLimitTracker.h` because they are part of the public API used
// across the code base.

// The concrete memory-limit enforcing allocator. See the file-level comment for
// why it lives in this dedicated namespace rather than directly in
// `ad_utility`.
namespace ad_utility::allocatorImpl {

/**
 * @brief Class to concurrently allocate memory up to a specified limit on the
 * total amount of memory allocated. The actual allocation is done by
 * std::allocator, but only when the limit is not exceeded.
 *
 * Memory allocated by copies of an Allocator will also count towards the limit
 * To use it, construct a first allocator by calling
 * AllocatorWithLimit{makeAllocationMemoryLeftThreadsafeObject(limit)} and then
 * pass copies of this allocator to your containers. You can also Create
 * Allocators for different types respecting the same total memory pool size,
 * e.g.
 *
 * auto memoryLeft = makeAllocationMemoryLeftThreadsafeObject(limitInBytes);
 * auto allocInt = AllocatorWithLimit<int>{memoryLeft};
 * auto limitedIntVec = std::vector<int, AllocatorWithLimit<int>>{allocInt};
 * auto allocString = AllocatorWithLimit<string>{memoryLeft};
 * auto limitedStringVec = std::vector<string,
 * AllocatorWithLimit<string>>{allocString};
 *
 * // now the total amount of memory allocated by limitedIntVec and
 * limitedStringVec may never exceed limitInBytes
 *
 * NOTE: For `std::vector` in particular, prefer the wrapper
 * `ad_utility::VectorWithMemoryLimit` (see `VectorWithMemoryLimit.h`) over
 * using this allocator directly as in the example above: it works around a
 * libc++ problem with the deleted default constructor and prevents accidental
 * copies.
 *
 * @tparam T the type of Elements that this allocator allocates
 */
template <typename T>
class AllocatorWithLimit {
 public:
  using value_type = T;

  // These special type aliases are necessary for using this allocator with
  // the STL. We always want it to be propagated to the target of copy and move
  // operations, s.T. the copy also counts towards the Limit.
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

 private:
  template <typename>
  friend class AllocatorWithLimit;

  detail::MemoryLimitTracker tracker_;
  [[no_unique_address]] std::allocator<T> allocator_;

 public:
  /// obtain an AllocationMemoryLeftThreadsafe by calls to
  /// makeAllocationMemoryLeftThreadsafeObject()
  explicit AllocatorWithLimit(
      detail::AllocationMemoryLeftThreadsafe ml,
      ClearOnAllocation clearOnAllocation = ad_utility::noop)
      : tracker_{std::move(ml), std::move(clearOnAllocation)} {}

  /// Obtain an AllocatorWithLimit<OtherType> that refers to the same limit.
  template <typename U>
  AllocatorWithLimit<U> as() const {
    return AllocatorWithLimit<U>(*this);
  }

  // This allocator has no default constructor, as it always requires a memory
  // limit. Note that some standard-library implementations (in particular
  // libc++) sometimes behave strangely with non-default-constructible
  // allocators; in particular, the default constructors of some
  // standard-library containers are then no longer SFINAE-friendly. See
  // `VectorWithMemoryLimit.h` for details.
  AllocatorWithLimit() = delete;

  CPP_template(typename U)(requires(!ql::concepts::same_as<U, T>))
      AllocatorWithLimit(const AllocatorWithLimit<U>& other)  // NOLINT
      : tracker_{other.tracker_} {}

  // Defaulted copy operations.
  AllocatorWithLimit(const AllocatorWithLimit&) = default;
  AllocatorWithLimit& operator=(const AllocatorWithLimit&) = default;

  // The tracker's noexcept move operations copy rather than steal (keeping the
  // moved-from allocator valid and sharing the same budget), so defaulting the
  // allocator move operations here is both correct and noexcept.
  static_assert(
      std::is_nothrow_move_constructible_v<detail::MemoryLimitTracker>);
  static_assert(std::is_nothrow_move_assignable_v<detail::MemoryLimitTracker>);
  static_assert(std::is_nothrow_copy_constructible_v<std::allocator<T>>);
  static_assert(std::is_nothrow_copy_assignable_v<std::allocator<T>>);
  AllocatorWithLimit(AllocatorWithLimit&&) noexcept = default;
  AllocatorWithLimit& operator=(AllocatorWithLimit&&) noexcept = default;

  ~AllocatorWithLimit() = default;

  // An allocator must have a function "allocate" with exactly this signature.
  // TODO<C++20> : the exact signature of allocate changes
  T* allocate(std::size_t n) {
    const auto bytes = MemorySize::bytes(n * sizeof(T));
    tracker_.reserveOrThrow(bytes);
    // If the underlying allocator fails (e.g. an injected bounded upstream that
    // throws when exhausted), release the bytes we just reserved so the tracker
    // does not stay permanently over-counted.
    try {
      return allocator_.allocate(n);
    } catch (...) {
      tracker_.release(bytes);
      throw;
    }
  }

  // An allocator must have a function "deallocate" with exactly this signature.
  void deallocate(T* p, std::size_t n) {
    allocator_.deallocate(p, n);
    tracker_.release(MemorySize::bytes(n * sizeof(T)));
  }

  /// Return the number of bytes, that this allocator and all of its copies
  /// currently have available
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return tracker_.amountMemoryLeft();
  }

  template <typename V>
  bool operator==(const AllocatorWithLimit<V>& v) const {
    return tracker_ == v.tracker_;
  }
  template <typename V>
  bool operator!=(const AllocatorWithLimit<V>& v) const {
    return !(*this == v);
  }

  static AllocatorWithLimit makeUnlimited() {
    return makeLimited(MemorySize::max());
  }
  static AllocatorWithLimit makeLimited(
      MemorySize limit,
      ClearOnAllocation clearOnAllocation = ad_utility::noop) {
    return AllocatorWithLimit{makeAllocationMemoryLeftThreadsafeObject(limit),
                              std::move(clearOnAllocation)};
  }
};

}  // namespace ad_utility::allocatorImpl

#endif  // QLEVER_SRC_UTIL_ALLOCATORWITHLIMITIMPL_H
