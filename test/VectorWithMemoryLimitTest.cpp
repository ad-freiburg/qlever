// Copyright 2026, University of Freiburg,
//
// Chair of Algorithms and Data Structures.
//
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "backports/concepts.h"
#include "util/AllocatorWithLimit.h"
#include "util/GTestHelpers.h"
#include "util/VectorWithMemoryLimit.h"

using ad_utility::AllocatorWithLimit;
using ad_utility::makeAllocationMemoryLeftThreadsafeObject;
using ad_utility::VectorWithMemoryLimit;
using namespace ad_utility::memory_literals;

namespace {
// Create an `AllocatorWithLimit<T>` with the given `limit`.
template <typename T = int>
AllocatorWithLimit<T> makeAllocator(ad_utility::MemorySize limit) {
  return AllocatorWithLimit<T>{makeAllocationMemoryLeftThreadsafeObject(limit)};
}
}  // namespace

// _____________________________________________________________________________
// The `VectorWithMemoryLimit` is neither default-constructible nor copyable,
// but it is movable.
TEST(VectorWithMemoryLimit, typeProperties) {
  static_assert(
      !ql::concepts::default_initializable<VectorWithMemoryLimit<int>>);
  static_assert(!ql::concepts::copyable<VectorWithMemoryLimit<int>>);
  static_assert(std::is_move_constructible_v<VectorWithMemoryLimit<int>>);
  static_assert(std::is_move_assignable_v<VectorWithMemoryLimit<int>>);
  static_assert(!std::is_copy_constructible_v<VectorWithMemoryLimit<int>>);
  static_assert(!std::is_copy_assignable_v<VectorWithMemoryLimit<int>>);
  // It really is a `std::vector` (with the limited allocator).
  static_assert(std::is_base_of_v<std::vector<int, AllocatorWithLimit<int>>,
                                  VectorWithMemoryLimit<int>>);
}

// _____________________________________________________________________________
// Construction from an allocator and basic `std::vector` operations work.
TEST(VectorWithMemoryLimit, constructionAndBasicOperations) {
  VectorWithMemoryLimit<int> v{makeAllocator(2_MB)};
  EXPECT_TRUE(v.empty());
  v.push_back(1);
  v.push_back(2);
  v.push_back(3);
  EXPECT_THAT(v, ::testing::ElementsAre(1, 2, 3));
  EXPECT_EQ(v.size(), 3u);
}

// _____________________________________________________________________________
// The `initializer_list` constructor is explicitly forwarded.
TEST(VectorWithMemoryLimit, initializerListConstructor) {
  VectorWithMemoryLimit<int> v{{4, 5, 6}, makeAllocator(2_MB)};
  EXPECT_THAT(v, ::testing::ElementsAre(4, 5, 6));
}

// _____________________________________________________________________________
// Moving transfers the contents and leaves the moved-from vector empty.
TEST(VectorWithMemoryLimit, move) {
  VectorWithMemoryLimit<int> v{makeAllocator(2_MB)};
  v.push_back(1);
  v.push_back(2);
  VectorWithMemoryLimit<int> moved{std::move(v)};
  EXPECT_THAT(moved, ::testing::ElementsAre(1, 2));

  VectorWithMemoryLimit<int> moveAssigned{makeAllocator(2_MB)};
  moveAssigned = std::move(moved);
  EXPECT_THAT(moveAssigned, ::testing::ElementsAre(1, 2));
}

// _____________________________________________________________________________
// `clone()` produces an independent deep copy.
TEST(VectorWithMemoryLimit, clone) {
  VectorWithMemoryLimit<int> v{makeAllocator(2_MB)};
  v.push_back(1);
  v.push_back(2);
  VectorWithMemoryLimit<int> copy = v.clone();
  EXPECT_THAT(copy, ::testing::ElementsAre(1, 2));
  // The copy is independent of the original.
  copy.push_back(3);
  EXPECT_THAT(v, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(copy, ::testing::ElementsAre(1, 2, 3));
}

// _____________________________________________________________________________
// The memory limit of the underlying allocator is enforced.
TEST(VectorWithMemoryLimit, memoryLimitIsEnforced) {
  // 18 bytes suffice for a few `int`s, but not for arbitrarily many.
  VectorWithMemoryLimit<int> v{makeAllocator(18_B)};
  v.push_back(5);  // allocate 4 bytes -> works.
  v.push_back(4);  // allocate 8 bytes, then free 4 -> works.
  EXPECT_THAT(v, ::testing::ElementsAre(5, 4));
  // Allocate 16 bytes -> fails (allocate before freeing the old 8).
  EXPECT_THROW(v.push_back(1),
               ad_utility::detail::AllocationExceedsLimitException);
}
