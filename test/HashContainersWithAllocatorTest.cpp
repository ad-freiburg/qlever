// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "util/ContainersWithAllocator.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/MemoryLimitTracker.h"

namespace {

using namespace ad_utility::memory_literals;
using ad_utility::detail::AllocationExceedsLimitException;

// Create a `qlm::Allocator<T>` with the given `limit`.
template <typename T>
qlm::Allocator<T> makeAllocator(ad_utility::MemorySize limit) {
  return qlever::makeAllocatorWithLimit<T>(limit);
}

// Insert `0, 1, ...` into `set` until the memory limit is exceeded, and check
// that the failing insertion leaves the `set` unchanged.
template <typename Set>
void testExceptionSafety(Set& set) {
  size_t i = 0;
  try {
    for (;; ++i) {
      set.insert(i);
    }
  } catch (const AllocationExceedsLimitException&) {
  }
  EXPECT_GT(i, 0u);
  EXPECT_EQ(set.size(), i);
  for (size_t j = 0; j < i; ++j) {
    EXPECT_EQ(set.count(j), 1u);
  }
  EXPECT_EQ(set.count(i), 0u);
}

// Assign `map[0], map[1], ...` until the memory limit is exceeded, and check
// that the failing insertion leaves the `map` unchanged.
template <typename Map>
void testMapExceptionSafety(Map& map) {
  size_t i = 0;
  try {
    for (;; ++i) {
      map[i] = 42;
    }
  } catch (const AllocationExceedsLimitException&) {
  }
  EXPECT_GT(i, 0u);
  EXPECT_EQ(map.size(), i);
  for (size_t j = 0; j < i; ++j) {
    EXPECT_EQ(map.at(j), 42);
  }
  EXPECT_EQ(map.count(i), 0u);
}

}  // namespace

// Test that all the hash containers with a memory limit throw when the limit
// is exceeded, and are left unchanged by the failing insertion.
TEST(HashContainersWithAllocator, memoryLimit) {
  {
    qlm::flat_hash_set<size_t> set{makeAllocator<size_t>(1_kB)};
    testExceptionSafety(set);
  }
  {
    qlm::node_hash_set<size_t> set{makeAllocator<size_t>(1_kB)};
    testExceptionSafety(set);
  }
  {
    ad_utility::HashSetWithMemoryLimit<size_t> set{makeAllocator<size_t>(1_kB)};
    testExceptionSafety(set);
  }
  {
    qlm::flat_hash_map<size_t, int> map{
        makeAllocator<std::pair<const size_t, int>>(1_kB)};
    testMapExceptionSafety(map);
  }
  {
    qlm::node_hash_map<size_t, int> map{
        makeAllocator<std::pair<const size_t, int>>(1_kB)};
    testMapExceptionSafety(map);
  }
  {
    ad_utility::HashMapWithMemoryLimit<size_t, int> map{
        makeAllocator<std::pair<const size_t, int>>(1_kB)};
    testMapExceptionSafety(map);
  }
}

// Test that the elements of the `node_...` containers don't move when the
// container grows.
TEST(HashContainersWithAllocator, nodeContainersHavePointerStability) {
  qlm::node_hash_map<int, std::string> map{
      makeAllocator<std::pair<const int, std::string>>(1_MB)};
  const std::string* first = &map[0];
  qlm::node_hash_set<int> set{makeAllocator<int>(1_MB)};
  const int* firstElement = &*set.insert(0).first;
  for (int i = 1; i < 1000; ++i) {
    map[i] = "x";
    set.insert(i);
  }
  EXPECT_EQ(first, &map.at(0));
  EXPECT_EQ(firstElement, &*set.find(0));
}

// Test that strings can be looked up via a `std::string_view` without
// materializing a `std::string`, because the default hash and equality
// functions are the transparent ones from `absl`.
TEST(HashContainersWithAllocator, heterogeneousLookup) {
  qlm::flat_hash_map<std::string, int> map{
      makeAllocator<std::pair<const std::string, int>>(1_MB)};
  map["hello"] = 42;
  std::string_view key = "hello";
  ASSERT_TRUE(map.contains(key));
  EXPECT_EQ(map.find(key)->second, 42);
  EXPECT_FALSE(map.contains(std::string_view{"world"}));

  ad_utility::HashSet<std::string> set{"abc"};
  EXPECT_TRUE(set.contains(std::string_view{"abc"}));
  ad_utility::HashMap<std::string, int> hashMap{{"abc", 3}};
  EXPECT_EQ(hashMap.find(std::string_view{"abc"})->second, 3);
}
