// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <range/v3/range/conversion.hpp>
#include <string>
#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "util/ContainersWithAllocator.h"

using namespace ad_utility::memory_literals;

// Test that `qlm::vector` is a `std::vector` with a `qlever::Allocator`.
TEST(ContainersWithAllocator, vectorIsAStdVector) {
  static_assert(std::is_base_of_v<std::vector<int, qlm::Allocator<int>>,
                                  qlm::vector<int>>);
  qlm::vector<int> v({1, 2, 3}, qlever::makeUnlimitedAllocator<int>());
  v.push_back(4);
  EXPECT_THAT(v, ::testing::ElementsAre(1, 2, 3, 4));

  // Assigning an initializer list works and keeps the allocator.
  auto alloc = qlever::makeAllocatorWithLimit<int>(1_MB);
  qlm::vector<int> w{alloc};
  w = {5, 6};
  EXPECT_THAT(w, ::testing::ElementsAre(5, 6));
  EXPECT_EQ(w.get_allocator(), alloc);
}

// Test that `::ranges::to<qlm::vector>` deduces the element type, also when
// the allocator is for a different element type.
TEST(ContainersWithAllocator, rangesToDeducesElementType) {
  auto alloc = qlever::makeAllocatorWithLimit<char>(1_MB);
  auto v = ql::views::iota(int64_t{3}, int64_t{6}) |
           ::ranges::to<qlm::vector>(alloc);
  static_assert(std::is_same_v<decltype(v), qlm::vector<int64_t>>);
  EXPECT_THAT(v, ::testing::ElementsAre(3, 4, 5));
  EXPECT_EQ(v.get_allocator(), qlm::Allocator<int64_t>(alloc));

  // The function form works as well.
  std::vector<std::string> strings{"a", "b"};
  auto w = ::ranges::to<qlm::vector>(strings, alloc);
  static_assert(std::is_same_v<decltype(w), qlm::vector<std::string>>);
  EXPECT_THAT(w, ::testing::ElementsAre("a", "b"));
}

// Test all branches of `qlm::detail::isConstructibleWithAllocator`. The checks
// are `static_assert`s because the helper is `consteval`.
TEST(ContainersWithAllocator, isConstructibleWithAllocator) {
  using Alloc = qlm::Allocator<int>;
  using Base = std::vector<int, Alloc>;
  using qlm::detail::isConstructibleWithAllocator;
  // No arguments.
  static_assert(!isConstructibleWithAllocator<Base, Alloc>);
  // The last argument is not an allocator. In particular, checking an iterator
  // pair must not instantiate the default allocator argument of `Base`.
  static_assert(!isConstructibleWithAllocator<Base, Alloc, int*, int*>);
  static_assert(!isConstructibleWithAllocator<Base, Alloc, size_t>);
  // The last argument is an allocator (possibly for a different element type)
  // and `Base` is constructible from the arguments.
  static_assert(isConstructibleWithAllocator<Base, Alloc, const Alloc&>);
  static_assert(isConstructibleWithAllocator<Base, Alloc, size_t, Alloc>);
  static_assert(isConstructibleWithAllocator<Base, Alloc, int*, int*, Alloc>);
  static_assert(isConstructibleWithAllocator<Base, Alloc, int*, int*,
                                             qlm::Allocator<char>>);
  // The last argument is an allocator, but `Base` is not constructible from
  // the arguments.
  static_assert(!isConstructibleWithAllocator<Base, Alloc, std::string, Alloc>);
}
