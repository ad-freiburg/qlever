// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>

#include "util/AllocateShared.h"

namespace {

// A minimal allocator that forwards to `std::allocator`, but counts the
// allocations in the counter it points to.
template <typename T>
struct CountingAllocator {
  using value_type = T;
  size_t* numAllocations_;

  explicit CountingAllocator(size_t* numAllocations)
      : numAllocations_{numAllocations} {}

  // Allocators have to be convertible between their rebound versions, because
  // `std::allocate_shared` rebinds the allocator to its internal type.
  template <typename U>
  CountingAllocator(const CountingAllocator<U>& other)
      : numAllocations_{other.numAllocations_} {}

  T* allocate(size_t n) {
    ++*numAllocations_;
    return std::allocator<T>{}.allocate(n);
  }
  void deallocate(T* pointer, size_t n) {
    std::allocator<T>{}.deallocate(pointer, n);
  }
};

// A class that uses `DEFINE_MAKE_SHARED_MEMBER` just like the classes in the
// query engine do.
class ClassWithAllocator {
 public:
  explicit ClassWithAllocator(size_t* numAllocations)
      : allocator_{numAllocations} {}

  // define a `makeShared` member function that has the same interface as
  // `std::make_shared`, but allocates via the `allocator_` (see
  // `util/AllocateShared.h`).
  DEFINE_MAKE_SHARED_MEMBER(allocator_)

 private:
  CountingAllocator<char> allocator_;
};

}  // namespace

// _____________________________________________________________________________
TEST(AllocateShared, makeShared) {
  size_t numAllocations = 0;
  // Note: `makeShared` is `const`, so it can be called on a `const` object.
  const ClassWithAllocator classWithAllocator{&numAllocations};

  // The arguments are perfectly forwarded to the constructor (here: a move-only
  // argument).
  auto pointer = classWithAllocator.makeShared<std::unique_ptr<int>>(
      std::make_unique<int>(42));
  EXPECT_EQ(**pointer, 42);

  // The object and the control block of the `shared_ptr` were allocated
  // together via the allocator of the class.
  EXPECT_EQ(numAllocations, 1);
}
