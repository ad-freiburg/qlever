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

#include <algorithm>
#include <atomic>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "util/AsyncResourcePool.h"
#include "util/AsyncTestHelpers.h"
#include "util/Forward.h"
#include "util/GTestHelpers.h"

using ad_utility::AsyncResourcePool;
using ad_utility::asyncWithResource;
namespace net = boost::asio;

namespace {
// A pool without resources, which is a plain counting semaphore.
using Semaphore = AsyncResourcePool<void>;
using Permit = Semaphore::Handle;

// A move-only resource that is not default-constructible, to check that the
// pool does not require more of its resources than it has to.
struct MoveOnlyResource {
  int value_;
  explicit MoveOnlyResource(int value) : value_{value} {}
  MoveOnlyResource(MoveOnlyResource&&) = default;
  MoveOnlyResource& operator=(MoveOnlyResource&&) = default;
  MoveOnlyResource(const MoveOnlyResource&) = delete;
  MoveOnlyResource& operator=(const MoveOnlyResource&) = delete;
};

// Detect whether a `Handle` has a `get()` member function, which it must not
// have for an `AsyncResourcePool<void>`.
template <typename Handle, typename = void>
struct HasGet : std::false_type {};
template <typename Handle>
struct HasGet<Handle, std::void_t<decltype(std::declval<Handle&>().get())>>
    : std::true_type {};

// Take out a single resource and return it (and the error code of the
// acquisition) to the caller.
template <typename ResourceType>
net::awaitable<std::tuple<boost::system::error_code,
                          typename AsyncResourcePool<ResourceType>::Handle>>
acquire(AsyncResourcePool<ResourceType> pool) {
  co_return co_await pool.asyncAcquire(net::as_tuple(net::use_awaitable));
}

// Yield to the other coroutines until `condition` holds.
net::awaitable<void> yieldUntil(net::io_context& ioContext,
                                std::function<bool()> condition) {
  while (!condition()) {
    co_await net::post(ioContext, net::use_awaitable);
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(AsyncResourcePool, zeroResourcesIsIllegal) {
  net::io_context ioContext;
  AD_EXPECT_THROW_WITH_MESSAGE((Semaphore{ioContext.get_executor(), 0}),
                               ::testing::HasSubstr("numResources > 0"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (AsyncResourcePool<int>{ioContext.get_executor(), std::vector<int>{}}),
      ::testing::HasSubstr("numResources > 0"));
}

// _____________________________________________________________________________
TEST(AsyncResourcePool, onlyANonVoidHandleHasAGetter) {
  static_assert(!Semaphore::hasResources);
  static_assert(AsyncResourcePool<int>::hasResources);
  static_assert(!HasGet<Semaphore::Handle>::value);
  static_assert(HasGet<AsyncResourcePool<int>::Handle>::value);
  // For a pool of `const T` the getter hands out a `const T&`, although a
  // mutable `T` is stored internally.
  static_assert(std::is_same_v<
                decltype(std::declval<AsyncResourcePool<int>::Handle&>().get()),
                int&>);
  static_assert(
      std::is_same_v<
          decltype(std::declval<AsyncResourcePool<const int>::Handle&>().get()),
          const int&>);
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, acquireWhileResourcesAreFree) {
  // All three permits are free, so all three acquisitions succeed immediately.
  Semaphore semaphore{ioContext.get_executor(), 3};
  std::vector<Permit> permits;
  for (size_t i = 0; i < 3; ++i) {
    auto [errorCode, permit] = co_await acquire<void>(semaphore);
    EXPECT_FALSE(errorCode);
    EXPECT_TRUE(permit.isValid());
    permits.push_back(std::move(permit));
  }
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, defaultConstructedHandleIsEmpty) {
  Permit permit;
  EXPECT_FALSE(permit.isValid());
  // Releasing an empty handle is a no-op and in particular does not touch any
  // pool.
  permit.release();
  EXPECT_FALSE(permit.isValid());
  co_return;
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, acquireSuspendsUntilAResourceIsReturned) {
  Semaphore semaphore{ioContext.get_executor(), 1};
  auto [errorCode, permit] = co_await acquire<void>(semaphore);
  EXPECT_FALSE(errorCode);
  EXPECT_TRUE(permit.isValid());

  // A second acquisition has to wait, because the only permit is taken.
  std::atomic<bool> secondWasAcquired{false};
  net::co_spawn(
      ioContext,
      [&semaphore, &secondWasAcquired]() -> net::awaitable<void> {
        auto [errorCode, permit] = co_await acquire<void>(semaphore);
        EXPECT_FALSE(errorCode);
        EXPECT_TRUE(permit.isValid());
        secondWasAcquired.store(true);
      },
      net::detached);

  // Give the second coroutine a chance to run and to suspend.
  for (size_t i = 0; i < 10; ++i) {
    co_await net::post(ioContext, net::use_awaitable);
  }
  EXPECT_FALSE(secondWasAcquired.load());

  // Returning the permit unblocks it.
  permit.release();
  EXPECT_FALSE(permit.isValid());
  co_await yieldUntil(
      ioContext, [&secondWasAcquired] { return secondWasAcquired.load(); });
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, destructorReturnsTheResource) {
  Semaphore semaphore{ioContext.get_executor(), 1};
  {
    auto [errorCode, permit] = co_await acquire<void>(semaphore);
    EXPECT_TRUE(permit.isValid());
  }
  // The destructor of the handle has returned the permit, so the next
  // acquisition succeeds (it would hang otherwise).
  auto [errorCode, permit] = co_await acquire<void>(semaphore);
  EXPECT_FALSE(errorCode);
  EXPECT_TRUE(permit.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, moveAssignmentReturnsTheOverwrittenResource) {
  Semaphore semaphore{ioContext.get_executor(), 2};
  auto [errorCodeA, permitA] = co_await acquire<void>(semaphore);
  auto [errorCodeB, permitB] = co_await acquire<void>(semaphore);
  EXPECT_TRUE(permitA.isValid());
  EXPECT_TRUE(permitB.isValid());
  // Both permits are taken, so this overwrites (and thereby returns) `permitA`
  // and empties `permitB`.
  permitA = std::move(permitB);
  EXPECT_TRUE(permitA.isValid());
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_FALSE(permitB.isValid());
  // Exactly one permit is free again, so this acquisition succeeds.
  auto [errorCodeC, permitC] = co_await acquire<void>(semaphore);
  EXPECT_FALSE(errorCodeC);
  EXPECT_TRUE(permitC.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, cancelWakesUpTheWaiters) {
  Semaphore semaphore{ioContext.get_executor(), 1};
  auto [errorCode, permit] = co_await acquire<void>(semaphore);
  EXPECT_TRUE(permit.isValid());

  std::atomic<size_t> numCancelled{0};
  for (size_t i = 0; i < 3; ++i) {
    net::co_spawn(
        ioContext,
        [&semaphore, &numCancelled]() -> net::awaitable<void> {
          auto [errorCode, permit] = co_await acquire<void>(semaphore);
          EXPECT_TRUE(errorCode);
          EXPECT_FALSE(permit.isValid());
          ++numCancelled;
        },
        net::detached);
  }
  semaphore.cancel();
  co_await yieldUntil(ioContext,
                      [&numCancelled] { return numCancelled.load() == 3; });
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, cancelIsNotSticky) {
  // `cancel()` only wakes up the current waiters, it does not discard the
  // resources that are still free, see the NOTE at `AsyncResourcePool::cancel`.
  Semaphore semaphore{ioContext.get_executor(), 1};
  semaphore.cancel();
  auto [errorCode, permit] = co_await acquire<void>(semaphore);
  EXPECT_FALSE(errorCode);
  EXPECT_TRUE(permit.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, handleOutlivesThePoolObject) {
  // The handle keeps the state of the pool alive, so returning the resource
  // after the `AsyncResourcePool` object is gone is well-defined.
  Permit permit;
  {
    Semaphore semaphore{ioContext.get_executor(), 1};
    auto [errorCode, acquired] = co_await acquire<void>(semaphore);
    EXPECT_TRUE(acquired.isValid());
    permit = std::move(acquired);
  }
  permit.release();
  EXPECT_FALSE(permit.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, asyncWithResourceScopesTheResourceToTheWork) {
  Semaphore semaphore{ioContext.get_executor(), 1};
  std::atomic<size_t> numRunning{0};
  std::atomic<size_t> maxRunning{0};
  std::atomic<size_t> numDone{0};

  // A "work" operation that is only complete once `finish` was posted, so that
  // the test can control when the permit is returned.
  using Latch =
      net::experimental::concurrent_channel<void(boost::system::error_code)>;
  Latch latch{ioContext.get_executor(), 4};

  auto makeWork = [&](size_t) {
    return [&](auto&& completionHandler) {
      ++numRunning;
      maxRunning.store(std::max(maxRunning.load(), numRunning.load()));
      // Complete as soon as the `latch` is opened once.
      latch.async_receive(
          [&numRunning, &numDone, handler = AD_FWD(completionHandler)](
              const boost::system::error_code&) mutable {
            --numRunning;
            ++numDone;
            std::move(handler)();
          });
    };
  };

  for (size_t i = 0; i < 3; ++i) {
    asyncWithResource(semaphore, makeWork(i), net::detached);
  }
  // Only a single permit exists, so at most one work item may run at a time.
  for (size_t i = 0; i < 3; ++i) {
    latch.try_send(boost::system::error_code{});
    co_await yieldUntil(ioContext,
                        [&numDone, i] { return numDone.load() == i + 1; });
  }
  EXPECT_EQ(maxRunning.load(), 1u);
  EXPECT_EQ(numRunning.load(), 0u);
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, asyncWithResourceReportsACancelledAcquisition) {
  Semaphore semaphore{ioContext.get_executor(), 1};
  auto [errorCode, permit] = co_await acquire<void>(semaphore);
  EXPECT_TRUE(permit.isValid());

  std::atomic<bool> workWasRun{false};
  std::atomic<bool> completed{false};
  auto work = [&workWasRun](auto&& completionHandler) {
    workWasRun.store(true);
    std::move(completionHandler)();
  };
  asyncWithResource(semaphore, work,
                    [&completed](const boost::system::error_code& errorCode) {
                      EXPECT_TRUE(errorCode);
                      completed.store(true);
                    });
  semaphore.cancel();
  co_await yieldUntil(ioContext, [&completed] { return completed.load(); });
  EXPECT_FALSE(workWasRun.load());
}

// _____________________________________________________________________________
ASYNC_TEST_N(AsyncResourcePool, multiThreaded, 4) {
  // Many acquisitions from several threads, all of which have to be serialized
  // by the two permits.
  constexpr size_t numPermits = 2;
  constexpr size_t numTasks = 64;
  Semaphore semaphore{ioContext.get_executor(), numPermits};
  std::atomic<size_t> numRunning{0};
  std::atomic<size_t> numDone{0};
  std::atomic<size_t> maxRunning{0};

  for (size_t i = 0; i < numTasks; ++i) {
    net::co_spawn(
        ioContext,
        [&]() -> net::awaitable<void> {
          auto [errorCode, permit] = co_await acquire<void>(semaphore);
          EXPECT_FALSE(errorCode);
          size_t running = ++numRunning;
          size_t previousMax = maxRunning.load();
          while (previousMax < running &&
                 !maxRunning.compare_exchange_weak(previousMax, running)) {
          }
          // Yield once, so that the resources are really held concurrently.
          co_await net::post(ioContext, net::use_awaitable);
          --numRunning;
          ++numDone;
        },
        net::detached);
  }
  co_await yieldUntil(ioContext,
                      [&numDone] { return numDone.load() == numTasks; });
  EXPECT_LE(maxRunning.load(), numPermits);
  EXPECT_GT(maxRunning.load(), 0u);
}

// The tests from here on are about the actual resources of a non-`void` pool.

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, countConstructorValueInitializesTheResources) {
  AsyncResourcePool<int> pool{ioContext.get_executor(), 2};
  for (size_t i = 0; i < 2; ++i) {
    auto [errorCode, handle] = co_await acquire<int>(pool);
    EXPECT_FALSE(errorCode);
    EXPECT_TRUE(handle.isValid());
    EXPECT_EQ(handle.get(), 0);
  }
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, vectorConstructorTakesOneResourcePerElement) {
  AsyncResourcePool<std::string> pool{ioContext.get_executor(),
                                      std::vector<std::string>{"a", "b", "c"}};
  std::vector<std::string> acquired;
  std::vector<AsyncResourcePool<std::string>::Handle> handles;
  for (size_t i = 0; i < 3; ++i) {
    auto [errorCode, handle] = co_await acquire<std::string>(pool);
    EXPECT_FALSE(errorCode);
    EXPECT_TRUE(handle.isValid());
    acquired.push_back(handle.get());
    handles.push_back(std::move(handle));
  }
  // The channel is FIFO, so the resources come out in the order of the vector.
  EXPECT_THAT(acquired, ::testing::ElementsAre("a", "b", "c"));
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, prototypeConstructorCopiesTheResource) {
  AsyncResourcePool<std::string> pool{ioContext.get_executor(), 2,
                                      std::string{"hello"}};
  for (size_t i = 0; i < 2; ++i) {
    auto [errorCode, handle] = co_await acquire<std::string>(pool);
    EXPECT_TRUE(handle.isValid());
    EXPECT_EQ(handle.get(), "hello");
  }
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, modificationsOfTheResourceArePreserved) {
  // The single resource is handed back and forth, and the modifications that
  // are made through the handle survive, because the resource itself (and not
  // just a permit) is returned to the pool.
  AsyncResourcePool<int> pool{ioContext.get_executor(), 1};
  for (int expected = 0; expected < 3; ++expected) {
    auto [errorCode, handle] = co_await acquire<int>(pool);
    EXPECT_TRUE(handle.isValid());
    EXPECT_EQ(handle.get(), expected);
    ++handle.get();
  }
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, moveOnlyResources) {
  // A resource that is neither copyable nor default-constructible can only be
  // set up via the vector constructor.
  std::vector<MoveOnlyResource> resources;
  resources.emplace_back(17);
  AsyncResourcePool<MoveOnlyResource> pool{ioContext.get_executor(),
                                           std::move(resources)};
  {
    auto [errorCode, handle] = co_await acquire<MoveOnlyResource>(pool);
    EXPECT_TRUE(handle.isValid());
    EXPECT_EQ(handle.get().value_, 17);
    handle.get().value_ = 18;
  }
  auto [errorCode, handle] = co_await acquire<MoveOnlyResource>(pool);
  EXPECT_TRUE(handle.isValid());
  EXPECT_EQ(handle.get().value_, 18);
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, constResources) {
  AsyncResourcePool<const std::string> pool{
      ioContext.get_executor(), std::vector<std::string>{"immutable"}};
  auto [errorCode, handle] = co_await acquire<const std::string>(pool);
  EXPECT_TRUE(handle.isValid());
  static_assert(std::is_same_v<decltype(handle.get()), const std::string&>);
  EXPECT_EQ(handle.get(), "immutable");
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, getOnAnEmptyHandleIsIllegal) {
  AsyncResourcePool<int>::Handle handle;
  EXPECT_FALSE(handle.isValid());
  AD_EXPECT_THROW_WITH_MESSAGE(handle.get(), ::testing::HasSubstr("isValid()"));
  co_return;
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncResourcePool, asyncWithResourcePassesTheResourceToTheWork) {
  AsyncResourcePool<int> pool{ioContext.get_executor(), 1};
  std::atomic<size_t> numDone{0};
  // Each work item increments the single resource, so the increments have to
  // add up, because the resource is returned to the pool after every item.
  auto work = [](int& resource, auto&& completionHandler) {
    ++resource;
    std::move(completionHandler)();
  };
  for (size_t i = 0; i < 3; ++i) {
    asyncWithResource(pool, work,
                      [&numDone](const boost::system::error_code& errorCode) {
                        EXPECT_FALSE(errorCode);
                        ++numDone;
                      });
  }
  co_await yieldUntil(ioContext, [&numDone] { return numDone.load() == 3; });
  auto [errorCode, handle] = co_await acquire<int>(pool);
  EXPECT_TRUE(handle.isValid());
  EXPECT_EQ(handle.get(), 3);
}
