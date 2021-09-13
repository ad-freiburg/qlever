//
// Created by johannes on 27.04.20.
//

#ifndef QLEVER_ALLOCATORWITHLIMIT_H
#define QLEVER_ALLOCATORWITHLIMIT_H

#include <atomic>
#include <memory>

#include "Synchronized.h"

namespace ad_utility {

namespace detail {

// An Exception to be thrown when trying to allocate more memory than was
// specified as a limit
class AllocationExceedsLimitException : public std::exception {
 public:
  AllocationExceedsLimitException(size_t requestedBytes, size_t freeBytes)
      : _message{"Tried to allocate " + std::to_string(requestedBytes >> 20) +
                 "MB, but only " + std::to_string(freeBytes >> 20) +
                 "MB  were available. " +
                 "Clear the cache or allow more memory for QLever during "
                 "startup"} {};

 private:
  const char* what() const noexcept override { return _message.c_str(); }
  const std::string _message;
};

// Class to keep track of the amount of memory that is left for allocation. When
// not enough memory is left, an AllocationExceedsLimitException is thrown. Note
// that need a separate class for this because there can be many Allocation
// objects at the same time (hence the wrapper class and the synchronization
// below).
class AllocationMemoryLeft {
  size_t free_;  // the number of free bytes
                 // throw AllocationExceedsLimitException on failure
 public:
  AllocationMemoryLeft(size_t n) : free_(n) {}

  // Called before memory is allocated.
  void decrease_if_enough_left(size_t n) {
    if (n <= free_) {
      free_ -= n;
    } else {
      throw AllocationExceedsLimitException{n, free_};
    }
  }

  // Called after memory is deallocated.
  void increase(size_t n) { free_ += n; }
  [[nodiscard]] size_t numFreeBytes() const { return free_; }
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
makeAllocationMemoryLeftThreadsafeObject(size_t n) {
  return detail::AllocationMemoryLeftThreadsafe{std::make_shared<
      ad_utility::Synchronized<detail::AllocationMemoryLeft, SpinLock>>(n)};
}

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
      memoryLeft_;  // shared number of free bytes
  std::allocator<T> allocator_;

 public:
  /// obtain an AllocationMemoryLeftThreadsafe by calls to
  /// makeAllocationMemoryLeftThreadsafeObject()
  explicit AllocatorWithLimit(detail::AllocationMemoryLeftThreadsafe ml)
      : memoryLeft_(std::move(ml)) {}

  /// Obtain an AllocatorWithLimit<OtherType> that refers to the
  /// same limit.
  template <typename U>
  AllocatorWithLimit<U> as() {
    return AllocatorWithLimit<U>(memoryLeft_);
  }
  AllocatorWithLimit() = delete;

  template <typename U>
  AllocatorWithLimit(const AllocatorWithLimit<U>& other)
      : memoryLeft_(other.getMemoryLeft()){};

  // An allocator must have a function "allocate" with exactly this signature.
  // TODO<C++20> : the exact signature of allocate changes
  T* allocate(std::size_t n) {
    // Subtract the amount of memory we want to allocate from the amount of
    // memory left. This will throw an exception if not enough memory is left.
    memoryLeft_.ptr()->wlock()->decrease_if_enough_left(n * sizeof(T));
    // the actual allocation
    return allocator_.allocate(n);
  }

  // An allocator must have a function "deallocate" with exactly this signature.
  void deallocate(T* p, std::size_t n) {
    // free the memory
    allocator_.deallocate(p, n);
    // Update the amount of memory left.
    memoryLeft_.ptr()->wlock()->increase(n * sizeof(T));
  }

  /// Return the number of bytes, that this allocator and all of its copies
  /// currently have available
  [[nodiscard]] size_t numFreeBytes() const {
    // casting is ok, because the actual numFreeBytes call
    // is const, and everything else is locking
    return const_cast<AllocatorWithLimit*>(this)
        ->memoryLeft_.ptr()
        ->wlock()
        ->numFreeBytes();
  }

  const auto& getMemoryLeft() const { return memoryLeft_; }

  // The STL needs two allocators to be equal if and only they refer to the same
  // memory pool. For us, they are hence equal if they use the same
  // AllocationMemoryLeft object.00
  template <typename V>
  bool operator==(const AllocatorWithLimit<V>& v) {
    return memoryLeft_ == v.memoryLeft_;
  }
  template <typename V>
  bool operator!=(const AllocatorWithLimit<V>& v) {
    return !(*this == v);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_ALLOCATORWITHLIMIT_H
