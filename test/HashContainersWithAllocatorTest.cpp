// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "backports/unordered.h"
#include "util/ContainersWithAllocator.h"
#include "util/Forward.h"
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

// Test that `ql::supportsHeterogeneousLookup` is true exactly for the Boost
// containers (since Boost 1.82) with transparent hash and equality functions.
TEST(HashContainersWithAllocator, supportsHeterogeneousLookup) {
  constexpr bool expected =
#ifdef QLEVER_HAS_BOOST_UNORDERED_NODE
      true;
#else
      false;
#endif
  // The default hash and equality functions from `absl` are transparent for
  // strings, but not for integers.
  EXPECT_EQ(
      (ql::supportsHeterogeneousLookup<ad_utility::HashMap<std::string, int>>),
      expected);
  EXPECT_EQ(ql::supportsHeterogeneousLookup<ad_utility::HashSet<std::string>>,
            expected);
  EXPECT_EQ(ql::supportsHeterogeneousLookup<qlm::node_hash_set<std::string>>,
            expected);
  EXPECT_FALSE(ql::supportsHeterogeneousLookup<ad_utility::HashSet<int>>);
  EXPECT_FALSE(
      (ql::supportsHeterogeneousLookup<std::unordered_map<
           std::string, int, absl::DefaultHashContainerHash<std::string>,
           absl::DefaultHashContainerEq<std::string>>>));
}

// Test `ql::heterogeneous`, which converts the key only if needed.
TEST(HashContainersWithAllocator, heterogeneous) {
  // Return the type of the key that is passed to the function.
  auto getKey = [](auto&& key, const auto&) { return key; };
  std::string_view key = "abc";

  ad_utility::HashMap<std::string, int> map;
  using KeyType = decltype(ql::heterogeneous(key, map, getKey));
  EXPECT_EQ((std::is_same_v<KeyType, std::string_view>),
            ql::supportsHeterogeneousLookup<decltype(map)>);
  EXPECT_EQ(ql::heterogeneous(key, map, getKey), "abc");

  // For a container without heterogeneous lookups, the key is converted.
  ad_utility::HashMap<std::string, int, std::hash<std::string>,
                      std::equal_to<std::string>>
      nonTransparent;
  EXPECT_TRUE(
      (std::is_same_v<decltype(ql::heterogeneous(key, nonTransparent, getKey)),
                      std::string>));
  // A key that already has the `key_type` is passed on unchanged.
  std::string stringKey = "abc";
  EXPECT_EQ(&ql::heterogeneous(stringKey, nonTransparent,
                               [](auto&& k, const auto&) -> decltype(auto) {
                                 return AD_FWD(k);
                               }),
            &stringKey);
}

// Test the functions in the `ql::unordered` namespace with keys of a different
// type than the `key_type`, for a hash map with and without transparent hash
// and equality functions.
TEST(HashContainersWithAllocator, unorderedFunctions) {
  auto testMap = [](auto map) {
    std::string_view hello = "hello";
    auto [it, isNew] = ql::unordered::try_emplace(map, hello, 42);
    EXPECT_TRUE(isNew);
    EXPECT_EQ(it->first, "hello");
    EXPECT_FALSE(ql::unordered::try_emplace(map, hello, 43).second);
    EXPECT_EQ(ql::unordered::at(map, hello), 42);
    EXPECT_FALSE(ql::unordered::insert_or_assign(map, hello, 44).second);
    EXPECT_EQ(ql::unordered::subscript(map, hello), 44);
    ql::unordered::subscript(map, std::string_view{"new"}) = 3;
    EXPECT_EQ(map.size(), 2u);
    EXPECT_TRUE(ql::unordered::contains(map, hello));
    EXPECT_FALSE(ql::unordered::contains(map, std::string_view{"world"}));
    EXPECT_EQ(ql::unordered::count(map, hello), 1u);
    EXPECT_EQ(ql::unordered::find(map, hello)->second, 44);
    EXPECT_EQ(ql::unordered::find(std::as_const(map), hello)->second, 44);
    EXPECT_TRUE(ql::unordered::find(map, std::string_view{"world"}) ==
                map.end());
    EXPECT_EQ(ql::unordered::erase(map, hello), 1u);
    EXPECT_EQ(ql::unordered::erase(map, hello), 0u);
    EXPECT_EQ(map.size(), 1u);
  };
  testMap(ad_utility::HashMap<std::string, int>{});
  testMap(ad_utility::HashMap<std::string, int, std::hash<std::string>,
                              std::equal_to<std::string>>{});
  testMap(qlm::node_hash_map<std::string, int>{
      makeAllocator<std::pair<const std::string, int>>(1_MB)});

  auto testSet = [](auto set) {
    std::string_view hello = "hello";
    EXPECT_TRUE(ql::unordered::insert(set, hello).second);
    EXPECT_FALSE(ql::unordered::insert(set, hello).second);
    EXPECT_TRUE(ql::unordered::contains(set, hello));
    EXPECT_EQ(ql::unordered::erase(set, hello), 1u);
    EXPECT_TRUE(set.empty());
  };
  testSet(ad_utility::HashSet<std::string>{});
  testSet(ad_utility::HashSet<std::string, std::hash<std::string>,
                              std::equal_to<std::string>>{});
  testSet(qlm::flat_hash_set<std::string>{makeAllocator<std::string>(1_MB)});
}

// Test that `ql::erase_if` works for all the hash containers.
TEST(HashContainersWithAllocator, eraseIf) {
  auto isOdd = [](int i) { return i % 2 == 1; };
  auto testSet = [&isOdd](auto set) {
    for (int i = 0; i < 10; ++i) {
      set.insert(i);
    }
    EXPECT_EQ(ql::erase_if(set, isOdd), 5u);
    EXPECT_EQ(set.size(), 5u);
    EXPECT_EQ(set.count(1), 0u);
    EXPECT_EQ(set.count(2), 1u);
  };
  testSet(ad_utility::HashSet<int>{});
  testSet(qlm::flat_hash_set<int>{makeAllocator<int>(1_MB)});
  testSet(qlm::node_hash_set<int>{makeAllocator<int>(1_MB)});

  auto valueIsOdd = [&isOdd](const auto& pair) { return isOdd(pair.second); };
  auto testMap = [&valueIsOdd](auto map) {
    for (int i = 0; i < 10; ++i) {
      map[i] = i;
    }
    EXPECT_EQ(ql::erase_if(map, valueIsOdd), 5u);
    EXPECT_EQ(map.size(), 5u);
    EXPECT_EQ(map.count(1), 0u);
    EXPECT_EQ(map.count(2), 1u);
  };
  testMap(ad_utility::HashMap<int, int>{});
  testMap(ad_utility::HashMapWithMemoryLimit<int, int>{
      makeAllocator<std::pair<const int, int>>(1_MB)});
  testMap(qlm::node_hash_map<int, int>{
      makeAllocator<std::pair<const int, int>>(1_MB)});
}
