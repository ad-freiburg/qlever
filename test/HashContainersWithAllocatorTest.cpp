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
#include <utility>

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

// Test the heterogeneous lookups and insertions with keys of a different type
// than the `key_type`, which all the hash containers support if their hash and
// equality functions are transparent (as the defaults from `absl` are for
// strings).
TEST(HashContainersWithAllocator, heterogeneousMemberFunctions) {
  auto testMap = [](auto map) {
    std::string_view hello = "hello";
    auto [it, isNew] = map.try_emplace(hello, 42);
    EXPECT_TRUE(isNew);
    EXPECT_EQ(it->first, "hello");
    EXPECT_FALSE(map.try_emplace(hello, 43).second);
    EXPECT_EQ(map.at(hello), 42);
    EXPECT_EQ(std::as_const(map).at(hello), 42);
    EXPECT_FALSE(map.insert_or_assign(hello, 44).second);
    EXPECT_EQ(map[hello], 44);
    map[std::string_view{"new"}] = 3;
    EXPECT_EQ(map.size(), 2u);
    EXPECT_TRUE(map.contains(hello));
    EXPECT_FALSE(map.contains(std::string_view{"world"}));
    EXPECT_EQ(map.count(hello), 1u);
    EXPECT_EQ(map.find(hello)->second, 44);
    EXPECT_EQ(std::as_const(map).find(hello)->second, 44);
    EXPECT_TRUE(map.find(std::string_view{"world"}) == map.end());
    EXPECT_EQ(map.erase(hello), 1u);
    EXPECT_EQ(map.erase(hello), 0u);
    EXPECT_EQ(map.size(), 1u);

    // The member functions that don't take a key still work.
    map.insert({"a", 1});
    map.insert(std::pair<const std::string, int>{"b", 2});
    map.erase(map.find("a"));
    EXPECT_EQ(map.size(), 2u);
    map.erase(map.begin(), map.end());
    EXPECT_TRUE(map.empty());
  };
  testMap(ad_utility::HashMap<std::string, int>{});
  testMap(ad_utility::HashMapWithMemoryLimit<std::string, int>{
      makeAllocator<std::pair<const std::string, int>>(1_MB)});
  testMap(qlm::node_hash_map<std::string, int>{
      makeAllocator<std::pair<const std::string, int>>(1_MB)});

  auto testSet = [](auto set) {
    std::string_view hello = "hello";
    EXPECT_TRUE(set.insert(hello).second);
    EXPECT_FALSE(set.insert(hello).second);
    EXPECT_TRUE(set.contains(hello));
    EXPECT_EQ(set.count(hello), 1u);
    EXPECT_EQ(*set.find(hello), "hello");
    EXPECT_EQ(set.erase(hello), 1u);
    EXPECT_TRUE(set.empty());

    // The member functions that don't take a key still work.
    set.insert({std::string{"a"}, std::string{"b"}});
    set.insert(set.begin(), std::string{"c"});
    EXPECT_EQ(set.size(), 3u);
    set.erase(set.find("c"));
    EXPECT_EQ(set.size(), 2u);
  };
  testSet(ad_utility::HashSet<std::string>{});
  testSet(ad_utility::HashSetWithMemoryLimit<std::string>{
      makeAllocator<std::string>(1_MB)});
  testSet(qlm::flat_hash_set<std::string>{makeAllocator<std::string>(1_MB)});
  testSet(qlm::node_hash_set<std::string>{makeAllocator<std::string>(1_MB)});

  // Keys can also be passed as braced initializer lists.
  ad_utility::HashMap<std::pair<int, int>, int> pairMap;
  pairMap[{1, 2}] = 3;
  EXPECT_TRUE(pairMap.contains({1, 2}));
  EXPECT_EQ(pairMap.at({1, 2}), 3);
  EXPECT_TRUE(pairMap.find({2, 1}) == pairMap.end());
  EXPECT_TRUE(pairMap.try_emplace({2, 1}, 4).second);
  EXPECT_EQ(pairMap.erase({1, 2}), 1u);
}

namespace {
// True iff `map.find(key)`, `map.try_emplace(key, 0)`, or `map[key]` compile.
template <typename Map, typename Key, typename = void>
constexpr bool canFind = false;
template <typename Map, typename Key>
constexpr bool canFind<
    Map, Key,
    std::void_t<decltype(std::declval<Map&>().find(std::declval<Key>()))>> =
    true;

template <typename Map, typename Key, typename = void>
constexpr bool canTryEmplace = false;
template <typename Map, typename Key>
constexpr bool
    canTryEmplace<Map, Key,
                  std::void_t<decltype(std::declval<Map&>().try_emplace(
                      std::declval<Key>(), 0))>> = true;

template <typename Map, typename Key, typename = void>
constexpr bool canSubscript = false;
template <typename Map, typename Key>
constexpr bool canSubscript<
    Map, Key,
    std::void_t<decltype(std::declval<Map&>()[std::declval<Key>()])>> = true;
}  // namespace

// Test the hash maps with non-transparent hash and equality functions. They
// can only be used with keys that are implicitly convertible to the
// `key_type`.
TEST(HashContainersWithAllocator, nonTransparentHashes) {
  using NonTransparent =
      ad_utility::HashMap<std::string, int, std::hash<std::string>,
                          std::equal_to<std::string>>;
  NonTransparent map;
  map["abc"] = 1;
  EXPECT_EQ(map.at(std::string{"abc"}), 1);
  EXPECT_TRUE(map.contains("abc"));
  EXPECT_TRUE(map.try_emplace("def", 2).second);
  EXPECT_EQ(map.erase("abc"), 1u);

  // With the Boost containers, heterogeneous lookups and insertions with a
  // non-transparent hash function don't compile, so that a missing
  // transparent hash function is detected. (With the `HeterogeneousContainer`
  // wrapper, they also don't compile, but the errors happen when instantiating
  // the function body, which we can't detect here.)
  using Transparent = ad_utility::HashMap<std::string, int>;
  static_assert(canFind<Transparent, std::string_view>);
  static_assert(canTryEmplace<Transparent, std::string_view>);
  static_assert(canSubscript<Transparent, std::string_view>);
#ifdef QLEVER_HAS_BOOST_UNORDERED_NODE
  static_assert(!canFind<NonTransparent, std::string_view>);
  static_assert(!canTryEmplace<NonTransparent, std::string_view>);
  static_assert(!canSubscript<NonTransparent, std::string_view>);
#endif
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
