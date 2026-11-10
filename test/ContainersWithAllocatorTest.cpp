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
