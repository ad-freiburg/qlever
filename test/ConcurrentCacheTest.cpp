// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "util/Cache.h"
#include "util/ConcurrentCache.h"
#include "util/DefaultValueSizeGetter.h"
#include "util/Timer.h"

using namespace std::literals;
using namespace std::chrono_literals;

class ConcurrentSignal {
  std::atomic_flag flag_;

 public:
  void notify() {
    flag_.test_and_set();
    flag_.notify_all();
  }

  void wait() { flag_.wait(false); }
};

// For the lifecycle of the tests, we have to know, when a computation has
// started and the computation has to wait for an external signal to complete.
// This can be achieved using two ConcurrentSignals.
struct StartStopSignal {
  ConcurrentSignal hasStartedSignal_;
  ConcurrentSignal mayFinishSignal_;
};

template <typename T>
auto waiting_function(T result, size_t milliseconds,
                      StartStopSignal* signal = nullptr) {
  return [result, signal, milliseconds]() {
    if (signal) {
      // signal that the operation has started
      signal->hasStartedSignal_.notify();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{milliseconds});
    if (signal) {
      // wait for the test case to allow finishing the operation
      signal->mayFinishSignal_.wait();
    }
    return result;
  };
}

auto wait_and_throw_function(size_t milliseconds,
                             StartStopSignal* signal = nullptr) {
  return [signal, milliseconds]() -> std::string {
    if (signal) {
      signal->hasStartedSignal_.notify();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{milliseconds});
    if (signal) {
      signal->mayFinishSignal_.wait();
    }
    throw std::runtime_error("this is bound to fail");
  };
}

using SimpleConcurrentLruCache =
    ad_utility::ConcurrentCache<ad_utility::LRUCache<
        int, std::string, ad_utility::StringSizeGetter<std::string>>>;

namespace {
auto returnTrue = [](const auto&) { return true; };
}  // namespace

TEST(ConcurrentCache, sequentialComputation) {
  SimpleConcurrentLruCache a{3ul};
  ad_utility::Timer t{ad_utility::Timer::Started};
  // Fake computation that takes 5ms and returns value "3", which is then
  // stored under key 3.
  auto result = a.computeOnce(3, waiting_function("3"s, 5), false, returnTrue);
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
  ASSERT_GE(t.msecs(), 5ms);
  ASSERT_EQ(1ul, a.numNonPinnedEntries());
  ASSERT_EQ(0ul, a.numPinnedEntries());
  // No other results currently being computed
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());

  t.reset();
  t.start();
  // takes 0 msecs to compute, as the request is served from the cache.
  auto result2 = a.computeOnce(3, waiting_function("3"s, 5), false, returnTrue);
  // computing result again: still yields "3", was cached and takes 0
  // milliseconds (result is read from cache)
  ASSERT_EQ("3"s, *result2._resultPointer);
  ASSERT_EQ(result2._cacheStatus, ad_utility::CacheStatus::cachedNotPinned);
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_LE(t.msecs(), 5ms);
  ASSERT_EQ(1ul, a.numNonPinnedEntries());
  ASSERT_EQ(0ul, a.numPinnedEntries());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
}

TEST(ConcurrentCache, sequentialPinnedComputation) {
  SimpleConcurrentLruCache a{3ul};
  ad_utility::Timer t{ad_utility::Timer::Started};
  // Fake computation that takes 5ms and returns value "3", which is then
  // stored under key 3.
  auto result =
      a.computeOncePinned(3, waiting_function("3"s, 5), false, returnTrue);
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
  ASSERT_GE(t.msecs(), 5ms);
  ASSERT_EQ(1ul, a.numPinnedEntries());
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  // No other results currently being computed
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());

  t.reset();
  t.start();
  // takes 0 msecs to compute, as the request is served from the cache.
  // we don't request a pin, but the original computation was pinned
  auto result2 = a.computeOnce(3, waiting_function("3"s, 5), false, returnTrue);
  // computing result again: still yields "3", was cached and takes 0
  // milliseconds (result is read from cache)
  ASSERT_EQ("3"s, *result2._resultPointer);
  ASSERT_EQ(result2._cacheStatus, ad_utility::CacheStatus::cachedPinned);
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_LE(t.msecs(), 5ms);
  ASSERT_EQ(1ul, a.numPinnedEntries());
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
}

TEST(ConcurrentCache, sequentialPinnedUpgradeComputation) {
  SimpleConcurrentLruCache a{3ul};
  ad_utility::Timer t{ad_utility::Timer::Started};
  // Fake computation that takes 5ms and returns value "3", which is then
  // stored under key 3.
  auto result = a.computeOnce(3, waiting_function("3"s, 5), false, returnTrue);
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
  ASSERT_GE(t.msecs(), 5ms);
  ASSERT_EQ(0ul, a.numPinnedEntries());
  ASSERT_EQ(1ul, a.numNonPinnedEntries());
  // No other results currently being computed
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());

  t.reset();
  t.start();
  // takes 0 msecs to compute, as the request is served from the cache.
  // request a pin, the result should be read from the cache and upgraded
  // to a pinned result.
  auto result2 =
      a.computeOncePinned(3, waiting_function("3"s, 5), false, returnTrue);
  // computing result again: still yields "3", was cached and takes 0
  // milliseconds (result is read from cache)
  ASSERT_EQ("3"s, *result2._resultPointer);
  ASSERT_EQ(result2._cacheStatus, ad_utility::CacheStatus::cachedNotPinned);
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_LE(t.msecs(), 5ms);
  ASSERT_EQ(1ul, a.numPinnedEntries());
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
}

TEST(ConcurrentCache, concurrentComputation) {
  auto a = SimpleConcurrentLruCache(3ul);
  StartStopSignal signal;
  auto compute = [&a, &signal]() {
    return a.computeOnce(3, waiting_function("3"s, 5, &signal), false,
                         returnTrue);
  };
  auto resultFuture = std::async(std::launch::async, compute);
  signal.hasStartedSignal_.wait();
  // now the background computation is ongoing and registered as
  // "in progress"
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  signal.mayFinishSignal_.notify();
  // this call waits for the background task to compute, and then fetches the
  // result. After this call completes, nothing is in progress and the result
  // is cached.
  auto result = compute();
  ASSERT_EQ(1ul, a.numNonPinnedEntries());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
  auto result2 = resultFuture.get();
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_EQ(result2._cacheStatus, ad_utility::CacheStatus::computed);
}

TEST(ConcurrentCache, concurrentPinnedComputation) {
  auto a = SimpleConcurrentLruCache(3ul);
  StartStopSignal signal;
  auto compute = [&a, &signal]() {
    return a.computeOncePinned(3, waiting_function("3"s, 5, &signal), false,
                               returnTrue);
  };
  auto resultFuture = std::async(std::launch::async, compute);
  signal.hasStartedSignal_.wait();
  // now the background computation is ongoing and registered as
  // "in progress"
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  signal.mayFinishSignal_.notify();

  // this call waits for the background task to compute, and then fetches the
  // result. After this call completes, nothing is in progress and the result
  // is cached.
  auto result = compute();
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(1ul, a.numPinnedEntries());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
  auto result2 = resultFuture.get();
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_EQ(result2._cacheStatus, ad_utility::CacheStatus::computed);
}

TEST(ConcurrentCache, concurrentPinnedUpgradeComputation) {
  auto a = SimpleConcurrentLruCache(3ul);
  StartStopSignal signal;
  auto compute = [&a, &signal]() {
    return a.computeOnce(3, waiting_function("3"s, 5, &signal), false,
                         returnTrue);
  };
  auto resultFuture = std::async(std::launch::async, compute);
  signal.hasStartedSignal_.wait();
  // now the background computation is ongoing and registered as
  // "in progress"
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  signal.mayFinishSignal_.notify();

  // this call waits for the background task to compute, and then fetches the
  // result. After this call completes, nothing is in progress and the result
  // is cached.
  auto result =
      a.computeOncePinned(3, waiting_function("3"s, 5), false, returnTrue);
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(1ul, a.numPinnedEntries());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.empty());
  ASSERT_EQ("3"s, *result._resultPointer);
  ASSERT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
  auto result2 = resultFuture.get();
  ASSERT_EQ(result._resultPointer, result2._resultPointer);
  ASSERT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
}

TEST(ConcurrentCache, abort) {
  auto a = SimpleConcurrentLruCache(3ul);
  StartStopSignal signal;
  auto compute = [&a, &signal]() {
    return a.computeOnce(3, waiting_function("3"s, 5, &signal), false,
                         returnTrue);
  };
  auto computeWithError = [&a, &signal]() {
    return a.computeOnce(3, wait_and_throw_function(5, &signal), false,
                         returnTrue);
  };
  auto fut = std::async(std::launch::async, computeWithError);
  signal.hasStartedSignal_.wait();
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(0ul, a.numPinnedEntries());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));

  signal.mayFinishSignal_.notify();
  ASSERT_THROW(compute(), ad_utility::WaitedForResultWhichThenFailedException);
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(0ul, a.numPinnedEntries());
  ASSERT_EQ(0ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_THROW(fut.get(), std::runtime_error);
}

TEST(ConcurrentCache, abortPinned) {
  auto a = SimpleConcurrentLruCache(3ul);
  StartStopSignal signal;
  auto compute = [&]() {
    return a.computeOncePinned(3, waiting_function("3"s, 5, &signal), false,
                               returnTrue);
  };
  auto computeWithError = [&a, &signal]() {
    return a.computeOncePinned(3, wait_and_throw_function(5, &signal), false,
                               returnTrue);
  };
  auto fut = std::async(std::launch::async, computeWithError);
  signal.hasStartedSignal_.wait();
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(0ul, a.numPinnedEntries());
  ASSERT_EQ(1ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_TRUE(a.getStorage().wlock()->_inProgress.contains(3));
  signal.mayFinishSignal_.notify();
  ASSERT_THROW(compute(), ad_utility::WaitedForResultWhichThenFailedException);
  ASSERT_EQ(0ul, a.numNonPinnedEntries());
  ASSERT_EQ(0ul, a.numPinnedEntries());
  ASSERT_EQ(0ul, a.getStorage().wlock()->_inProgress.size());
  ASSERT_THROW(fut.get(), std::runtime_error);
}

TEST(ConcurrentCache, cacheStatusToString) {
  using enum ad_utility::CacheStatus;
  EXPECT_EQ(toString(cachedNotPinned), "cached_not_pinned");
  EXPECT_EQ(toString(cachedPinned), "cached_pinned");
  EXPECT_EQ(toString(computed), "computed");
  EXPECT_EQ(toString(notInCacheAndNotComputed), "not_in_cache_not_computed");

  auto outOfBounds = static_cast<ad_utility::CacheStatus>(
      static_cast<int>(notInCacheAndNotComputed) + 1);
  EXPECT_ANY_THROW(toString(outOfBounds));
}
