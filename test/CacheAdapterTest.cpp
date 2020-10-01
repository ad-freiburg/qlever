// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include <chrono>
#include <thread>

#include "../../src/engine/QueryExecutionContext.h"
#include "../../src/util/CacheAdapter.h"
#include "../../src/util/Timer.h"

auto finish = [](auto&& val) { val.moveFromInProgressToCache(); };
auto nothing = []([[maybe_unused]] auto&& val) {};

template <typename T>
auto waiting_function(T result, size_t milliseconds) {
  return [=]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
    return result;
  };
}

auto wait_and_throw_function(size_t milliseconds) {
  return [=]() -> std::string {
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
    throw std::runtime_error("this is bound to fail");
  };
}

using SimpleAdapter =
    ad_utility::CacheAdapter<ad_utility::LRUCache<int, std::string>>;

TEST(CacheAdapter, initialize) { auto a = SubtreeCache(42ul); }

TEST(CacheAdapter, publicInterface) {
  SimpleAdapter a{3ul};
  ad_utility::Timer t;
  t.start();
  // takes 100 ms to compute
  auto res = a.computeOnce(3, waiting_function("3"s, 100));
  t.stop();
  // initial computation: yields "3", was not cached and takes 100 milliseconds
  ASSERT_EQ("3"s, *res._resultPointer);
  ASSERT_FALSE(res._wasCached);
  ASSERT_GE(t.msecs(), 100);
  ASSERT_EQ(1ul, a.numCachedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());

  t.reset();
  t.start();
  // takes 0 msecs to compute, as the request is served from the cache.
  auto res2 = a.computeOnce(3, waiting_function("3"s, 100));
  t.stop();
  // computing result again: still yields "3", was cached and takes 0
  // milliseconds (result is read from cache)
  ASSERT_EQ("3"s, *res2._resultPointer);
  ASSERT_TRUE(res2._wasCached);
  ASSERT_EQ(res._resultPointer, res2._resultPointer);
  ASSERT_LE(t.msecs(), 5);
  ASSERT_EQ(1ul, a.numCachedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
}

TEST(CacheAdapter, concurrentComputation) {
  auto a = SimpleAdapter(3ul);
  auto compute = [&]() {
    return a.computeOnce(3, waiting_function("3"s, 100));
  };
  auto fut = std::async(std::launch::async, compute);
  // the background thread is now computing for 100 milliseconds, wait for
  // some time s.t. the computation has safely started
  // note: This test might fail on a single-threaded system.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  // now the background computation should be ongoing and registered as
  // "in progress"
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  // this call waits for the background task to compute, and then fetches the
  // result. After this call completes, nothing is in progress and the result
  // is cached.
  auto res = compute();
  ASSERT_EQ(1ul, a.numCachedElements());
  ASSERT_EQ(0ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_EQ("3"s, *res._resultPointer);
  auto res2 = fut.get();
  ASSERT_EQ(res._resultPointer, res2._resultPointer);
}

TEST(CacheAdapter, abort) {
  auto a = SimpleAdapter(3ul);
  auto compute = [&]() {
    return a.computeOnce(3, waiting_function("3"s, 100));
  };
  auto computeThrow = [&]() {
    return a.computeOnce(3, wait_and_throw_function(100));
  };
  auto fut = std::async(std::launch::async, computeThrow);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));
  ASSERT_THROW(compute(), ad_utility::AbortedByOtherThreadException);
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(0ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_THROW(fut.get(), std::runtime_error);
}
