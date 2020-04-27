//
// Created by johannes on 27.04.20.
//

#ifndef QLEVER_LIMITEDALLOCATOR_H
#define QLEVER_LIMITEDALLOCATOR_H

#include <atomic>
#include "Synchronized.h"

namespace ad_utility {

class LimitException : public std::exception {
  const char* what() const noexcept override {
    return "Tried to allocate more than the specified limit";
  }
};

class AllocationLimits {
  size_t free_;  // the number of free bytes
                 // throw LimitException on failure
 public:
  AllocationLimits(size_t n) : free_(n) {}

  void allocate(size_t n) {
    if (n <= free_) {
      free_ -= n;
    } else {
      throw LimitException{};
    }
  }

  void deallocate(size_t n) { free_ += n; }
};

using AllocationState =
    std::shared_ptr<ad_utility::Synchronized<AllocationLimits, SpinLock>>;

AllocationState makeAllocationState(size_t n) {
  return std::make_shared<ad_utility::Synchronized<AllocationLimits, SpinLock>>(
      n);
}

template <typename T>
class LimitedAllocator {
 public:
  using value_type = T;

 private:
  AllocationState state_;
  std::allocator<T> alloc_;

 public:
  LimitedAllocator(AllocationState s) : state_(std::move(s)) {}
  // TODO<C++20> : the exact signature of allocate changes
  T* allocate(std::size_t n) {
    state_->wlock()->allocate(n * sizeof(T));
    return alloc_.allocate(n);
  }

  void deallocate(T* p, std::size_t n) {
    state_->wlock()->deallocate(n * sizeof(T));
    alloc_.deallocate(p, n);
  }

  template <typename U, typename V>
  friend bool operator==(const LimitedAllocator<U>& u,
                         const LimitedAllocator<V>& v) {
    return u.state_ == v.state_;
  }
  template <typename U, typename V>
  friend bool operator!=(const LimitedAllocator<U>& u,
                         const LimitedAllocator<V>& v) {
    return !(u == v);
  }
};
}  // namespace ad_utility

#endif  // QLEVER_LIMITEDALLOCATOR_H
