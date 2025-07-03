// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>

#include "util/Cache.h"
#include "util/ConcurrentCache.h"
#include "util/DefaultValueSizeGetter.h"
#include "util/GTestHelpers.h"
#include "util/Parameters.h"
#include "util/Timer.h"
#include "util/jthread.h"

using namespace std::literals;
using namespace std::chrono_literals;
using namespace ad_utility::memory_literals;
using ::testing::Pointee;

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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(ConcurrentCache, isNotCachedIfUnsuitable) {
  SimpleConcurrentLruCache cache{};

  cache.clearAll();

  auto result = cache.computeOnce(
      0, []() { return "abc"; }, false, [](const auto&) { return false; });

  EXPECT_EQ(cache.numNonPinnedEntries(), 0);
  EXPECT_EQ(cache.numPinnedEntries(), 0);
  EXPECT_THAT(result._resultPointer, Pointee("abc"s));
}

namespace {
// A very particular helper for the following tests.
// On construction, it captures the number of references to
// the `inProgressMap` of the `cache` at the key `0`.
// It then has a method to block until that use count changes,
// which means that another thread is waiting for the same
// result.
struct UseCounter {
  SimpleConcurrentLruCache& cache_;
  long useCount_;
  explicit UseCounter(SimpleConcurrentLruCache& cache)
      : cache_{cache},
        useCount_{
            cache.getStorage().wlock()->_inProgress[0].second.use_count()} {}
  void waitForChange() {
    while (cache_.getStorage().wlock()->_inProgress[0].second.use_count() <=
           useCount_) {
      std::this_thread::sleep_for(1ms);
    }
  }
};
}  // namespace

// _____________________________________________________________________________
TEST(ConcurrentCache, isNotCachedIfUnsuitableWhenWaitingForPendingComputation) {
  SimpleConcurrentLruCache cache{};

  auto resultInProgress = std::make_shared<
      ad_utility::ConcurrentCacheDetail::ResultInProgress<std::string>>();

  cache.clearAll();
  cache.getStorage().wlock()->_inProgress[0] =
      std::pair(false, resultInProgress);

  UseCounter useCounter{cache};
  ad_utility::JThread thread{[&]() {
    useCounter.waitForChange();
    resultInProgress->finish(nullptr);
  }};

  auto result = cache.computeOnce(
      0, []() { return "abc"; }, false, [](const auto&) { return false; });

  EXPECT_EQ(cache.numNonPinnedEntries(), 0);
  EXPECT_EQ(cache.numPinnedEntries(), 0);
  EXPECT_THAT(result._resultPointer, Pointee("abc"s));
}

// _____________________________________________________________________________
TEST(ConcurrentCache, isCachedIfSuitableWhenWaitingForPendingComputation) {
  SimpleConcurrentLruCache cache{};

  auto resultInProgress = std::make_shared<
      ad_utility::ConcurrentCacheDetail::ResultInProgress<std::string>>();

  cache.clearAll();
  cache.getStorage().wlock()->_inProgress[0] =
      std::pair(false, resultInProgress);

  UseCounter useCounter{cache};
  ad_utility::JThread thread{[&]() {
    useCounter.waitForChange();
    resultInProgress->finish(nullptr);
  }};

  auto result = cache.computeOnce(
      0, []() { return "abc"; }, false, [](const auto&) { return true; });

  EXPECT_EQ(cache.numNonPinnedEntries(), 1);
  EXPECT_EQ(cache.numPinnedEntries(), 0);
  EXPECT_THAT(result._resultPointer, Pointee("abc"s));
  EXPECT_EQ(result._cacheStatus, ad_utility::CacheStatus::computed);
  EXPECT_TRUE(cache.cacheContains(0));
}

// _____________________________________________________________________________
TEST(ConcurrentCache,
     isCachedIfSuitableWhenWaitingForPendingComputationPinned) {
  SimpleConcurrentLruCache cache{};

  // Simulate a computation with the same cache key that is currently in
  // progress so the new computation waits for the result.
  auto resultInProgress = std::make_shared<
      ad_utility::ConcurrentCacheDetail::ResultInProgress<std::string>>();

  cache.clearAll();
  cache.getStorage().wlock()->_inProgress[0] =
      std::pair(false, resultInProgress);

  UseCounter useCounter{cache};
  ad_utility::JThread thread{[&]() {
    useCounter.waitForChange();
    resultInProgress->finish(nullptr);
  }};

  auto result = cache.computeOncePinned(
      0, []() { return "abc"; }, false, [](const auto&) { return true; });

  EXPECT_EQ(cache.numNonPinnedEntries(), 0);
  EXPECT_EQ(cache.numPinnedEntries(), 1);
  EXPECT_THAT(result._resultPointer, Pointee("abc"s));
  EXPECT_TRUE(cache.cacheContains(0));
}

// _____________________________________________________________________________
TEST(ConcurrentCache, ifUnsuitableForCacheAndPinnedThrowsException) {
  SimpleConcurrentLruCache cache{};

  cache.clearAll();

  EXPECT_THROW(
      cache.computeOncePinned(
          0, []() { return "abc"; }, false, [](const auto&) { return false; }),
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(ConcurrentCache,
     ifUnsuitableWhenWaitingForPendingComputationAndPinnedThrowsException) {
  SimpleConcurrentLruCache cache{};

  auto resultInProgress = std::make_shared<
      ad_utility::ConcurrentCacheDetail::ResultInProgress<std::string>>();

  cache.clearAll();
  cache.getStorage().wlock()->_inProgress[0] =
      std::pair(false, resultInProgress);

  UseCounter useCounter{cache};
  ad_utility::JThread thread{[&]() {
    useCounter.waitForChange();
    resultInProgress->finish(nullptr);
  }};

  EXPECT_THROW(
      cache.computeOncePinned(
          0, []() { return "abc"; }, false, [](const auto&) { return false; }),
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(ConcurrentCache, testTryInsertIfNotPresentDoesWorkCorrectly) {
  auto hasValue = [](std::string value) {
    using namespace ::testing;
    using CS = SimpleConcurrentLruCache::ResultAndCacheStatus;
    return Optional(
        Field("_resultPointer", &CS::_resultPointer, Pointee(value)));
  };

  SimpleConcurrentLruCache cache{};

  auto expectContainsSingleElementAtKey0 =
      [&](bool pinned, std::string expected,
          ad_utility::source_location l =
              ad_utility::source_location::current()) {
        using namespace ::testing;
        auto trace = generateLocationTrace(l);
        auto value = cache.getIfContained(0);
        EXPECT_THAT(value, hasValue(expected));
        if (pinned) {
          EXPECT_NE(cache.pinnedSize(), 0_B);
          EXPECT_EQ(cache.nonPinnedSize(), 0_B);
        } else {
          EXPECT_EQ(cache.pinnedSize(), 0_B);
          EXPECT_NE(cache.nonPinnedSize(), 0_B);
        }
      };

  cache.tryInsertIfNotPresent(false, 0, std::make_shared<std::string>("abc"));

  expectContainsSingleElementAtKey0(false, "abc");

  cache.tryInsertIfNotPresent(false, 0, std::make_shared<std::string>("def"));

  expectContainsSingleElementAtKey0(false, "abc");

  cache.tryInsertIfNotPresent(true, 0, std::make_shared<std::string>("ghi"));

  expectContainsSingleElementAtKey0(true, "abc");

  cache.clearAll();

  cache.tryInsertIfNotPresent(true, 0, std::make_shared<std::string>("jkl"));

  expectContainsSingleElementAtKey0(true, "jkl");
}

TEST(ConcurrentCache, computeButDontStore) {
  SimpleConcurrentLruCache cache{};

  // The last argument of `computeOnce...`: For the sake of this test, all
  // results are suitable for the cache. Note: In the `computeButDontStore`
  // function this argument is ignored, because the results are never stored in
  // the cache.
  auto alwaysSuitable = [](auto&&) { return true; };
  // Store the element in the cache.
  cache.computeOnce(
      42, []() { return "42"; }, false, alwaysSuitable);

  // The result is read from the cache, so we get "42", not "blubb".
  auto res = cache.computeButDontStore(
      42, []() { return "blubb"; }, false, alwaysSuitable);
  EXPECT_EQ(*res._resultPointer, "42");

  // The same with `onlyReadFromCache` == true;
  res = cache.computeButDontStore(
      42, []() { return "blubb"; }, true, alwaysSuitable);
  EXPECT_EQ(*res._resultPointer, "42");

  cache.clearAll();

  // Compute, but don't store.
  res = cache.computeButDontStore(
      42, []() { return "blubb"; }, false, alwaysSuitable);
  EXPECT_EQ(*res._resultPointer, "blubb");

  // Nothing is stored in the cache, so we cannot read it.
  EXPECT_FALSE(cache.getIfContained(42).has_value());
  res = cache.computeButDontStore(
      42, []() { return "blubb"; }, true, alwaysSuitable);
  EXPECT_EQ(res._resultPointer, nullptr);
}
