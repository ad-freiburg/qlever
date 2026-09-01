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
#include <tuple>
#include <utility>
#include <vector>

#include "util/AsyncSemaphore.h"
#include "util/AsyncTestHelpers.h"
#include "util/Forward.h"
#include "util/GTestHelpers.h"

using ad_utility::AsyncSemaphore;
using ad_utility::asyncWithPermit;
namespace net = boost::asio;

namespace {
using Permit = AsyncSemaphore::Permit;

// Take out a single permit and return it (and the error code of the
// acquisition) to the caller.
net::awaitable<std::tuple<boost::system::error_code, Permit>> acquire(
    AsyncSemaphore semaphore) {
  co_return co_await semaphore.asyncAcquire(net::as_tuple(net::use_awaitable));
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
TEST(AsyncSemaphore, zeroPermitsIsIllegal) {
  net::io_context ioContext;
  AD_EXPECT_THROW_WITH_MESSAGE((AsyncSemaphore{ioContext.get_executor(), 0}),
                               ::testing::HasSubstr("numPermits > 0"));
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncSemaphore, acquireWhilePermitsAreFree) {
  // All three permits are free, so all three acquisitions succeed immediately.
  AsyncSemaphore semaphore{ioContext.get_executor(), 3};
  std::vector<Permit> permits;
  for (size_t i = 0; i < 3; ++i) {
    auto [errorCode, permit] = co_await acquire(semaphore);
    EXPECT_FALSE(errorCode);
    EXPECT_TRUE(permit.isValid());
    permits.push_back(std::move(permit));
  }
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncSemaphore, defaultConstructedPermitIsEmpty) {
  Permit permit;
  EXPECT_FALSE(permit.isValid());
  // Releasing an empty permit is a no-op and in particular does not touch any
  // semaphore.
  permit.release();
  EXPECT_FALSE(permit.isValid());
  co_return;
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncSemaphore, acquireSuspendsUntilAPermitIsReturned) {
  AsyncSemaphore semaphore{ioContext.get_executor(), 1};
  auto [errorCode, permit] = co_await acquire(semaphore);
  EXPECT_FALSE(errorCode);
  EXPECT_TRUE(permit.isValid());

  // A second acquisition has to wait, because the only permit is taken.
  std::atomic<bool> secondWasAcquired{false};
  net::co_spawn(
      ioContext,
      [&semaphore, &secondWasAcquired]() -> net::awaitable<void> {
        auto [errorCode, permit] = co_await acquire(semaphore);
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
ASYNC_TEST(AsyncSemaphore, destructorReturnsThePermit) {
  AsyncSemaphore semaphore{ioContext.get_executor(), 1};
  {
    auto [errorCode, permit] = co_await acquire(semaphore);
    EXPECT_TRUE(permit.isValid());
  }
  // The destructor of the permit has returned it, so the next acquisition
  // succeeds (it would hang otherwise).
  auto [errorCode, permit] = co_await acquire(semaphore);
  EXPECT_FALSE(errorCode);
  EXPECT_TRUE(permit.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncSemaphore, moveAssignmentReturnsTheOverwrittenPermit) {
  AsyncSemaphore semaphore{ioContext.get_executor(), 2};
  auto [errorCodeA, permitA] = co_await acquire(semaphore);
  auto [errorCodeB, permitB] = co_await acquire(semaphore);
  EXPECT_TRUE(permitA.isValid());
  EXPECT_TRUE(permitB.isValid());
  // Both permits are taken, so this overwrites (and thereby returns) `permitA`
  // and empties `permitB`.
  permitA = std::move(permitB);
  EXPECT_TRUE(permitA.isValid());
  // NOLINTNEXTLINE(bugprone-use-after-move)
  EXPECT_FALSE(permitB.isValid());
  // Exactly one permit is free again, so this acquisition succeeds.
  auto [errorCodeC, permitC] = co_await acquire(semaphore);
  EXPECT_FALSE(errorCodeC);
  EXPECT_TRUE(permitC.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncSemaphore, cancelWakesUpTheWaiters) {
  AsyncSemaphore semaphore{ioContext.get_executor(), 1};
  auto [errorCode, permit] = co_await acquire(semaphore);
  EXPECT_TRUE(permit.isValid());

  std::atomic<size_t> numCancelled{0};
  for (size_t i = 0; i < 3; ++i) {
    net::co_spawn(
        ioContext,
        [&semaphore, &numCancelled]() -> net::awaitable<void> {
          auto [errorCode, permit] = co_await acquire(semaphore);
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
ASYNC_TEST(AsyncSemaphore, cancelIsNotSticky) {
  // `cancel()` only wakes up the current waiters, it does not discard the
  // permits that are still free, see the NOTE at `AsyncSemaphore::cancel`.
  AsyncSemaphore semaphore{ioContext.get_executor(), 1};
  semaphore.cancel();
  auto [errorCode, permit] = co_await acquire(semaphore);
  EXPECT_FALSE(errorCode);
  EXPECT_TRUE(permit.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncSemaphore, permitOutlivesTheSemaphoreObject) {
  // The permit keeps the state of the semaphore alive, so returning it after
  // the `AsyncSemaphore` object is gone is well-defined.
  Permit permit;
  {
    AsyncSemaphore semaphore{ioContext.get_executor(), 1};
    auto [errorCode, acquired] = co_await acquire(semaphore);
    EXPECT_TRUE(acquired.isValid());
    permit = std::move(acquired);
  }
  permit.release();
  EXPECT_FALSE(permit.isValid());
}

// _____________________________________________________________________________
ASYNC_TEST(AsyncSemaphore, asyncWithPermitScopesThePermitToTheWork) {
  AsyncSemaphore semaphore{ioContext.get_executor(), 1};
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
    asyncWithPermit(semaphore, makeWork(i), net::detached);
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
ASYNC_TEST(AsyncSemaphore, asyncWithPermitReportsACancelledAcquisition) {
  AsyncSemaphore semaphore{ioContext.get_executor(), 1};
  auto [errorCode, permit] = co_await acquire(semaphore);
  EXPECT_TRUE(permit.isValid());

  std::atomic<bool> workWasRun{false};
  std::atomic<bool> completed{false};
  auto work = [&workWasRun](auto&& completionHandler) {
    workWasRun.store(true);
    std::move(completionHandler)();
  };
  asyncWithPermit(semaphore, work,
                  [&completed](const boost::system::error_code& errorCode) {
                    EXPECT_TRUE(errorCode);
                    completed.store(true);
                  });
  semaphore.cancel();
  co_await yieldUntil(ioContext, [&completed] { return completed.load(); });
  EXPECT_FALSE(workWasRun.load());
}

// _____________________________________________________________________________
ASYNC_TEST_N(AsyncSemaphore, multiThreaded, 4) {
  // Many acquisitions from several threads, all of which have to be serialized
  // by the two permits.
  constexpr size_t numPermits = 2;
  constexpr size_t numTasks = 64;
  AsyncSemaphore semaphore{ioContext.get_executor(), numPermits};
  std::atomic<size_t> numRunning{0};
  std::atomic<size_t> numDone{0};
  std::atomic<size_t> maxRunning{0};

  for (size_t i = 0; i < numTasks; ++i) {
    net::co_spawn(
        ioContext,
        [&]() -> net::awaitable<void> {
          auto [errorCode, permit] = co_await acquire(semaphore);
          EXPECT_FALSE(errorCode);
          size_t running = ++numRunning;
          size_t previousMax = maxRunning.load();
          while (previousMax < running &&
                 !maxRunning.compare_exchange_weak(previousMax, running)) {
          }
          // Yield once, so that the permits are really held concurrently.
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
