// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (April of 2020,
// kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>

#include <functional>
#include <memory>

#include "util/MemorySize/MemorySize.h"
#include "util/Synchronized.h"

namespace ad_utility {

namespace detail {

// An Exception to be thrown when trying to allocate more memory than was
// specified as a limit
class AllocationExceedsLimitException : public std::exception {
 public:
  AllocationExceedsLimitException(MemorySize requestedMemory,
                                  MemorySize freeMemory)
      : _message{absl::StrCat("Tried to allocate ", requestedMemory.asString(),
                              ", but only ", freeMemory.asString(),
                              " were available. Clear the cache or allow more "
                              "memory for QLever during startup")} {};

  const char* what() const noexcept override { return _message.c_str(); }

 private:
  const std::string _message;
};

// Class to keep track of the amount of memory that is left for allocation. When
// not enough memory is left, an AllocationExceedsLimitException is thrown. Note
// that need a separate class for this because there can be many Allocation
// objects at the same time (hence the wrapper class and the synchronization
// below).
class AllocationMemoryLeft {
  // Remaining free memory.
  MemorySize free_;

 public:
  AllocationMemoryLeft(MemorySize n) : free_(n) {}

  // Called before memory is allocated.
  bool decrease_if_enough_left_or_return_false(MemorySize n) noexcept {
    if (n <= free_) {
      free_ -= n;
      return true;
    } else {
      return false;
    }
  }

  // Called before memory is allocated.
  void decrease_if_enough_left_or_throw(MemorySize n) {
    if (!decrease_if_enough_left_or_return_false(n)) {
      throw AllocationExceedsLimitException{n, free_};
    }
  }

  // Called after memory is deallocated.
  void increase(MemorySize n) { free_ += n; }
  [[nodiscard]] MemorySize amountMemoryLeft() const { return free_; }
};

/*
 * Threadsafe Wrapper around AllocationMemoryLeft.
 * Copies of objects of this class will refer to the same AllocationMemoryLeft
 * object Concurrent access is handled via ad_utility::Synchronized.
 */
class AllocationMemoryLeftThreadsafe {
 public:
  AllocationMemoryLeftThreadsafe() = delete;
  using T =
      std::shared_ptr<ad_utility::Synchronized<AllocationMemoryLeft, SpinLock>>;
  explicit AllocationMemoryLeftThreadsafe(T ptr) : ptr_{std::move(ptr)} {}
  T& ptr() { return ptr_; }
  const T& ptr() const { return ptr_; }

  friend bool operator==(const AllocationMemoryLeftThreadsafe& a,
                         const AllocationMemoryLeftThreadsafe& b) {
    return a.ptr_ == b.ptr_;
  }

 private:
  T ptr_;
};
}  // namespace detail

// setup a shared Allocation state. For the usage see documentation of the
// Limited Allocator class
inline detail::AllocationMemoryLeftThreadsafe
makeAllocationMemoryLeftThreadsafeObject(MemorySize n) {
  return detail::AllocationMemoryLeftThreadsafe{std::make_shared<
      ad_utility::Synchronized<detail::AllocationMemoryLeft, SpinLock>>(n)};
}

/*
A lambda for use with `AllocatorWithLimit`.

Called, when there is not enough memory left for an allocation and is supposed
to try to free the given amount of memory.

The lambda is given at construction.
*/
using ClearOnAllocation = std::function<void(MemorySize)>;

/// A Noop lambda that will be used as a template default parameter
/// in the `AllocatorWithLimit` class.
inline ClearOnAllocation noClearOnAllocation = [](MemorySize) {};

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
  detail::AllocationMemoryLeftThreadsafe
      memoryLeft_;                       // shared number of free bytes
  ClearOnAllocation clearOnAllocation_;  // TODO<joka921> comment
  std::allocator<T> allocator_;

 public:
  /// obtain an AllocationMemoryLeftThreadsafe by calls to
  /// makeAllocationMemoryLeftThreadsafeObject()
  explicit AllocatorWithLimit(
      detail::AllocationMemoryLeftThreadsafe ml,
      ClearOnAllocation clearOnAllocation = noClearOnAllocation)
      : memoryLeft_{std::move(ml)},
        clearOnAllocation_{std::move(clearOnAllocation)} {}

  /// Obtain an AllocatorWithLimit<OtherType> that refers to the
  /// same limit.
  template <typename U>
  AllocatorWithLimit<U> as() {
    return AllocatorWithLimit<U>(memoryLeft_);
  }
  AllocatorWithLimit() = delete;

  template <typename U>
  requires(!std::same_as<U, T>)
  AllocatorWithLimit(const AllocatorWithLimit<U>& other)
      : memoryLeft_{other.getMemoryLeft()},
        clearOnAllocation_(other.clearOnAllocation()){};

  // Defaulted copy operations.
  AllocatorWithLimit(const AllocatorWithLimit&) = default;
  AllocatorWithLimit& operator=(const AllocatorWithLimit&) = default;

  // We deliberately let the move assignment call the copy assignment, because
  // when a `vector<..., AllocatorWithLimit>` is moved from, we still want the
  // vector that was moved from to have a valid allocator that uses the same
  // memory limit. Note: We could also implicitly leave out the move operators,
  // then the copy operators would be called implicitly, but
  // 1. It is hard to reason about things that are implicitly not there.
  // 2. This would inhibit move semantics from `std::vector` unless the copy
  // operations are also `noexcept`, so we need
  //    some extra code anyway.
  AllocatorWithLimit(AllocatorWithLimit&& other) noexcept try
      : AllocatorWithLimit(static_cast<const AllocatorWithLimit&>(other)) {
    // Empty body, because all the logic is done by the delegated constructor
    // and the catch block.
  } catch (const std::exception& e) {
    auto log = [&e](auto& stream) {
      stream << "The move constructor of `AllocatorWithLimit` threw an "
                "exception with message "
             << e.what() << " .This should never happen, terminating"
             << std::endl;
    };
    log(ad_utility::Log::getLog<ERROR>());
    log(std::cerr);
    std::terminate();
  }
  AllocatorWithLimit& operator=(AllocatorWithLimit&& other) noexcept {
    ad_utility::terminateIfThrows(
        [self = this, &other] {
          *self = static_cast<const AllocatorWithLimit&>(other);
        },
        "The move assignment operator of `AllocatorWithLimit` should never "
        "throw in practice.");
    return *this;
  }

  // An allocator must have a function "allocate" with exactly this signature.
  // TODO<C++20> : the exact signature of allocate changes
  T* allocate(std::size_t n) {
    // Subtract the amount of memory we want to allocate from the amount of
    // memory left. This will throw an exception if not enough memory is left.
    const auto bytesNeeded = MemorySize::bytes(n * sizeof(T));
    const bool wasEnoughLeft =
        memoryLeft_.ptr()->wlock()->decrease_if_enough_left_or_return_false(
            bytesNeeded);
    if (!wasEnoughLeft) {
      AD_CORRECTNESS_CHECK(clearOnAllocation_);
      clearOnAllocation_(bytesNeeded);
      memoryLeft_.ptr()->wlock()->decrease_if_enough_left_or_throw(bytesNeeded);
    }
    // the actual allocation
    return allocator_.allocate(n);
  }

  // An allocator must have a function "deallocate" with exactly this signature.
  void deallocate(T* p, std::size_t n) {
    // free the memory
    allocator_.deallocate(p, n);
    // Update the amount of memory left.
    memoryLeft_.ptr()->wlock()->increase(MemorySize::bytes(n * sizeof(T)));
  }

  /// Return the number of bytes, that this allocator and all of its copies
  /// currently have available
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    // casting is ok, because the actual numFreeBytes call
    // is const, and everything else is locking
    return const_cast<AllocatorWithLimit*>(this)
        ->memoryLeft_.ptr()
        ->wlock()
        ->amountMemoryLeft();
  }

  const auto& getMemoryLeft() const { return memoryLeft_; }
  const auto& clearOnAllocation() const { return clearOnAllocation_; }

  // The STL needs two allocators to be equal if and only they refer to the same
  // memory pool. For us, they are hence equal if they use the same
  // AllocationMemoryLeft object.00
  template <typename V>
  bool operator==(const AllocatorWithLimit<V>& v) const {
    return memoryLeft_ == v.memoryLeft_;
  }
  template <typename V>
  bool operator!=(const AllocatorWithLimit<V>& v) const {
    return !(*this == v);
  }
};

// Return a new allocator with the specified limit.
template <typename T>
AllocatorWithLimit<T> makeAllocatorWithLimit(MemorySize limit) {
  return AllocatorWithLimit<T>{makeAllocationMemoryLeftThreadsafeObject(limit)};
}

// Return a new allocator with the maximal possible limit.
template <typename T>
AllocatorWithLimit<T> makeUnlimitedAllocator() {
  return makeAllocatorWithLimit<T>(MemorySize::max());
}

}  // namespace ad_utility
