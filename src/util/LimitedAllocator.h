//
// Created by johannes on 27.04.20.
//

#ifndef QLEVER_LIMITEDALLOCATOR_H
#define QLEVER_LIMITEDALLOCATOR_H

#include <atomic>
#include <locale>
#include <sstream>
#include "../util/ReadableNumberFact.h"
#include "Synchronized.h"

namespace ad_utility {

class LimitException : public std::exception {
 public:
  LimitException(std::string msg) : msg_(msg) {}
  virtual ~LimitException() {}
 private:
  const char* what() const noexcept override {
    return msg_.c_str();
    // return "Tried to allocate more than the specified limit";
  }
  std::string msg_;
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
      // NEW(Hannah): In the exception (which also appears in the QLever UI), be
      // explicit about how much we tried to allocate and what was still
      // available.
      std::ostringstream os;
      std::locale loc;
      ad_utility::ReadableNumberFacet facet(1);
      std::locale locWithNumberGrouping(loc, &facet);
      os.imbue(locWithNumberGrouping);
      os << "Tried to allocate " << (n / 1000000) << " MB, but only "
                                 << (free_ / 1000000) << " MB left";
      throw LimitException(os.str());
    }
  }

  void deallocate(size_t n) { free_ += n; }
  [[nodiscard]] size_t numFreeBytes() const { return free_; }
};

class AllocationState {
 public:
  AllocationState() = delete;
  using T =
      std::shared_ptr<ad_utility::Synchronized<AllocationLimits, SpinLock>>;
  explicit AllocationState(T ptr) : ptr_{std::move(ptr)} {}
  T& ptr() { return ptr_; }

 private:
  T ptr_;
};

inline AllocationState makeAllocationState(size_t n) {
  return AllocationState{
      std::make_shared<ad_utility::Synchronized<AllocationLimits, SpinLock>>(
          n)};
}

template <typename T>
class LimitedAllocator {
 public:
  using value_type = T;

 private:
  AllocationState state_;
  std::allocator<T> alloc_;

 public:
  explicit LimitedAllocator(AllocationState s) : state_(std::move(s)) {}
  LimitedAllocator() = delete;
  // TODO<C++20> : the exact signature of allocate changes
  T* allocate(std::size_t n) {
    state_.ptr()->wlock()->allocate(n * sizeof(T));
    return alloc_.allocate(n);
  }

  void deallocate(T* p, std::size_t n) {
    state_.ptr()->wlock()->deallocate(n * sizeof(T));
    alloc_.deallocate(p, n);
  }

  [[nodiscard]] size_t numFreeBytes() {
    return state_.ptr()->wlock()->numFreeBytes();
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
