// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "../../src/util/Cache.h"
#include "../../src/util/CacheAdapter.h"
#include "../../src/util/Timer.h"

using namespace std::literals;

template <typename T>
auto waiting_function(T result, size_t milliseconds,
                      std::atomic<bool>* f = nullptr) {
  return [result, milliseconds, f]() {
    if (f) {
      *f = true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
    return result;
  };
}

auto wait_and_throw_function(size_t milliseconds,
                             std::atomic<bool>* f = nullptr) {
  return [f, milliseconds]() -> std::string {
    if (f) {
      *f = true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
    throw std::runtime_error("this is bound to fail");
  };
}

using SimpleAdapter =
    ad_utility::CacheAdapter<ad_utility::LRUCache<int, std::string>>;

TEST(CacheAdapter, sequentialComputation) {
  SimpleAdapter a{3ul};
  ad_utility::Timer t;
  t.start();
  // Fake computation that takes 100ms and returns value "3", which is then
  // stored under key 3.
  auto res = a.computeOnce(3, waiting_function("3"s, 100));
  t.stop();
  ASSERT_EQ("3"s, *res._resultPointer);
  ASSERT_FALSE(res._wasCached);
  ASSERT_GE(t.msecs(), 100);
  ASSERT_EQ(1ul, a.numCachedElements());
  ASSERT_EQ(0ul, a.numPinnedElements());
  // No other results currently being computed
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
  ASSERT_EQ(0ul, a.numPinnedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
}

TEST(CacheAdapter, sequentialPinnedComputation) {
  SimpleAdapter a{3ul};
  ad_utility::Timer t;
  t.start();
  // Fake computation that takes 100ms and returns value "3", which is then
  // stored under key 3.
  auto res = a.computeOncePinned(3, waiting_function("3"s, 100));
  t.stop();
  ASSERT_EQ("3"s, *res._resultPointer);
  ASSERT_FALSE(res._wasCached);
  ASSERT_GE(t.msecs(), 100);
  ASSERT_EQ(1ul, a.numPinnedElements());
  ASSERT_EQ(0ul, a.numCachedElements());
  // No other results currently being computed
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());

  t.reset();
  t.start();
  // takes 0 msecs to compute, as the request is served from the cache.
  // we don't request a pin, but the original computation was pinned
  auto res2 = a.computeOnce(3, waiting_function("3"s, 100));
  t.stop();
  // computing result again: still yields "3", was cached and takes 0
  // milliseconds (result is read from cache)
  ASSERT_EQ("3"s, *res2._resultPointer);
  ASSERT_TRUE(res2._wasCached);
  ASSERT_EQ(res._resultPointer, res2._resultPointer);
  ASSERT_LE(t.msecs(), 5);
  ASSERT_EQ(1ul, a.numPinnedElements());
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
}

TEST(CacheAdapter, sequentialPinnedUpgradeComputation) {
  SimpleAdapter a{3ul};
  ad_utility::Timer t;
  t.start();
  // Fake computation that takes 100ms and returns value "3", which is then
  // stored under key 3.
  auto res = a.computeOnce(3, waiting_function("3"s, 100));
  t.stop();
  ASSERT_EQ("3"s, *res._resultPointer);
  ASSERT_FALSE(res._wasCached);
  ASSERT_GE(t.msecs(), 100);
  ASSERT_EQ(0ul, a.numPinnedElements());
  ASSERT_EQ(1ul, a.numCachedElements());
  // No other results currently being computed
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());

  t.reset();
  t.start();
  // takes 0 msecs to compute, as the request is served from the cache.
  // request a pin, the result should be read from the cache and upgraded
  // to a pinned result.
  auto res2 = a.computeOncePinned(3, waiting_function("3"s, 100));
  t.stop();
  // computing result again: still yields "3", was cached and takes 0
  // milliseconds (result is read from cache)
  ASSERT_EQ("3"s, *res2._resultPointer);
  ASSERT_TRUE(res2._wasCached);
  ASSERT_EQ(res._resultPointer, res2._resultPointer);
  ASSERT_LE(t.msecs(), 5);
  ASSERT_EQ(1ul, a.numPinnedElements());
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
}

TEST(CacheAdapter, concurrentComputation) {
  auto a = SimpleAdapter(3ul);
  std::atomic<bool> flag = false;
  auto compute = [&a, &flag]() {
    return a.computeOnce(3, waiting_function("3"s, 100, &flag));
  };
  auto resultFuture = std::async(std::launch::async, compute);
  // the background thread is now computing for 100 milliseconds, wait for
  // some time s.t. the computation has safely started
  // note: This test might fail on a single-threaded system.
  while (!flag) {
  }
  // now the background computation should be ongoing and registered as
  // "in progress"
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  // this call waits for the background task to compute, and then fetches the
  // result. After this call completes, nothing is in progress and the result
  // is cached.
  auto result = compute();
  ASSERT_EQ(1ul, a.numCachedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_FALSE(result._wasCached);
  auto result2 = resultFuture.get();
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_FALSE(result2._wasCached);
}

TEST(CacheAdapter, concurrentPinnedComputation) {
  auto a = SimpleAdapter(3ul);
  std::atomic<bool> flag = false;
  auto compute = [&a, &flag]() {
    return a.computeOncePinned(3, waiting_function("3"s, 100, &flag));
  };
  auto resultFuture = std::async(std::launch::async, compute);
  // the background thread is now computing for 100 milliseconds, wait for
  // some time s.t. the computation has safely started
  // note: This test might fail on a single-threaded system.
  // now the background computation should be ongoing and registered as
  // "in progress"
  while (!flag) {
  }
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  // this call waits for the background task to compute, and then fetches the
  // result. After this call completes, nothing is in progress and the result
  // is cached.
  auto result = compute();
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(1ul, a.numPinnedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_FALSE(result._wasCached);
  auto result2 = resultFuture.get();
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_FALSE(result2._wasCached);
}

TEST(CacheAdapter, concurrentPinnedUpgradeComputation) {
  auto a = SimpleAdapter(3ul);
  std::atomic<bool> flag = false;
  auto compute = [&a, &flag]() {
    return a.computeOnce(3, waiting_function("3"s, 100, &flag));
  };
  auto resultFuture = std::async(std::launch::async, compute);
  // the background thread is now computing for 100 milliseconds, wait for
  // some time s.t. the computation has safely started
  // note: This test might fail on a single-threaded system.
  while (!flag) {
  }
  // now the background computation should be ongoing and registered as
  // "in progress"
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  // this call waits for the background task to compute, and then fetches the
  // result. After this call completes, nothing is in progress and the result
  // is cached.
  auto result = a.computeOncePinned(3, waiting_function("3"s, 100));
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(1ul, a.numPinnedElements());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_FALSE(result._wasCached);
  auto result2 = resultFuture.get();
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_FALSE(result2._wasCached);
}

TEST(CacheAdapter, abort) {
  auto a = SimpleAdapter(3ul);
  std::atomic<bool> flag = false;
  auto compute = [&a, &flag]() {
    return a.computeOnce(3, waiting_function("3"s, 100, &flag));
  };
  auto computeWithError = [&a, &flag]() {
    return a.computeOnce(3, wait_and_throw_function(100, &flag));
  };
  auto fut = std::async(std::launch::async, computeWithError);
  while (!flag) {
  }
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(0ul, a.numPinnedElements());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));
  ASSERT_THROW(compute(), ad_utility::WaitedForResultWhichThenFailedException);
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(0ul, a.numPinnedElements());
  ASSERT_EQ(0ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_THROW(fut.get(), std::runtime_error);
}

TEST(CacheAdapter, abortPinned) {
  auto a = SimpleAdapter(3ul);
  std::atomic<bool> flag = false;
  auto compute = [&]() {
    return a.computeOncePinned(3, waiting_function("3"s, 100, &flag));
  };
  auto computeWithError = [&a, &flag]() {
    return a.computeOncePinned(3, wait_and_throw_function(100, &flag));
  };
  auto fut = std::async(std::launch::async, computeWithError);
  while (!flag) {
  }
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(0ul, a.numPinnedElements());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));
  ASSERT_THROW(compute(), ad_utility::WaitedForResultWhichThenFailedException);
  ASSERT_EQ(0ul, a.numCachedElements());
  ASSERT_EQ(0ul, a.numPinnedElements());
  ASSERT_EQ(0ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_THROW(fut.get(), std::runtime_error);
}
