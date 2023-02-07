//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_UNINITIALIZEDALLOCATOR_H
#define QLEVER_UNINITIALIZEDALLOCATOR_H

#include <vector>

namespace ad_utility {
// Allocator adaptor that interposes construct() calls to
// convert value initialization into default initialization.
// This makes a difference for builtin arithmetic types like int, which are zero
// after a value initialization and have an undefined value after default
// initialization. Using this allocator adapter for a std::vector<int> makes a
// growing resize O(oldSize) while it being O(newSize) with the zeroing  value
// initialization. Source:
// https://stackoverflow.com/questions/21028299/is-this-behavior-of-vectorresizesize-type-n-under-c11-and-boost-container/21028912#21028912
// Most functionality is inherited from the underlying allocator type A
template <typename T, typename A = std::allocator<T>>
class default_init_allocator : public A {
  typedef std::allocator_traits<A> a_t;

 public:
  // The `rebind` struct specifies how to cast an allocator to a different value
  // type. Since `default_init_allocator` has two template arguments, we need to
  // make explicit here how to do this.
  template <typename U>
  struct rebind {
    using other =
        default_init_allocator<U, typename a_t::template rebind_alloc<U>>;
  };

  // Inherit all constructors from the base allocator.
  using A::A;

  // "Copy" and "Move" construction from underlying type
  default_init_allocator(const A& a) : A{a} {}
  default_init_allocator(A&& a) : A{std::move(a)} {}
  default_init_allocator(const default_init_allocator&) = default;
  default_init_allocator(default_init_allocator&&) = default;
  default_init_allocator& operator=(default_init_allocator&&) = default;
  default_init_allocator& operator=(const default_init_allocator&) = default;

  // overload the construct template with default initialization
  template <typename U>
  void construct(U* ptr) noexcept(
      std::is_nothrow_default_constructible<U>::value) {
    // new without parentheses after the type performs default-initialization
    // see https://en.cppreference.com/w/cpp/language/new (paragraph
    // "Construction")
    ::new (static_cast<void*>(ptr)) U;
  }
  // initialization with parameters, the actual construction is completely
  // performed by the underlying allocator.
  template <typename U, typename... Args>
  void construct(U* ptr, Args&&... args) {
    a_t::construct(static_cast<A&>(*this), ptr, std::forward<Args>(args)...);
  }
};

/// Alias for a `std::vector<T>` that uses the `default_init_allocator`
template <typename T>
using UninitializedVector = std::vector<T, default_init_allocator<T>>;
}  // namespace ad_utility

#endif  // QLEVER_UNINITIALIZEDALLOCATOR_H
