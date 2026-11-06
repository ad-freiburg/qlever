//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <thread>

#include "util/AsioHelpers.h"
#include "util/AsyncTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/http/beast.h"

using namespace ad_utility;
namespace {
using WorkGuard = net::executor_work_guard<net::io_context::executor_type>;
[[nodiscard]] WorkGuard makeWorkGuard(net::io_context& ctx) {
  return WorkGuard{ctx.get_executor()};
}
}  // namespace
// _________________________________________________________________________
TEST(AsioHelpers, runFunctionOnExecutorVoid) {
  net::io_context ctx;
  bool a = false;
  auto workGuard = makeWorkGuard(ctx);
  runFunctionOnExecutor(ctx.get_executor(), [&]() { a = true; }, net::detached);
  EXPECT_FALSE(a);
  ctx.poll();
  EXPECT_TRUE(a);

  // The exception disappears into the void, because the result is `detached`.
  runFunctionOnExecutor(
      ctx.get_executor(), [&]() { throw std::runtime_error{"blim"}; },
      net::detached);
  ctx.poll();

  a = false;
  auto fut = runFunctionOnExecutor(
      ctx.get_executor(), [&]() { a = true; }, net::use_future);
  EXPECT_FALSE(a);
  EXPECT_EQ(1, ctx.poll());
  EXPECT_TRUE(a);
  fut.get();

  a = false;
  fut = runFunctionOnExecutor(
      ctx.get_executor(),
      [&]() {
        a = true;
        throw std::runtime_error{"blim"};
      },
      net::use_future);
  EXPECT_FALSE(a);
  EXPECT_EQ(1, ctx.poll());
  EXPECT_TRUE(a);
  EXPECT_THROW(fut.get(), std::runtime_error);
}

// _________________________________________________________________________
TEST(AsioHelpers, runFunctionOnExecutorValue) {
  net::io_context ctx;
  auto workGuard = makeWorkGuard(ctx);
  auto fut = runFunctionOnExecutor(
      ctx.get_executor(), [&]() { return 12; }, net::use_future);
  EXPECT_EQ(1, ctx.poll());
  EXPECT_EQ(fut.get(), 12);

  fut = runFunctionOnExecutor(
      ctx.get_executor(), [&]() -> int { throw std::runtime_error{"blim"}; },
      net::use_future);
  EXPECT_EQ(1, ctx.poll());
  EXPECT_THROW(fut.get(), std::runtime_error);
}

// _________________________________________________________________________
TEST(AsioHelpers, runFunctionOnExecutorStrands) {
  net::io_context ctx;
  auto workGuard = makeWorkGuard(ctx);
  // Used to check that the asynchronous code is run at all.
  std::atomic<int> sanityCounter = 0;
  auto strand1 = net::make_strand(ctx);
  auto strand2 = net::make_strand(ctx);

  auto fut = runFunctionOnExecutor(
      strand1,
      [&]() {
        EXPECT_TRUE(strand1.running_in_this_thread());
        EXPECT_FALSE(strand2.running_in_this_thread());
        ++sanityCounter;
      },
      net::use_future);
  EXPECT_EQ(1, ctx.poll());
  fut.get();
  EXPECT_EQ(sanityCounter, 1);
  sanityCounter = 0;

  auto nestedCoroutine = [&]() -> net::awaitable<void> {
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    co_await runFunctionOnExecutor(
        strand2,
        [&]() {
          EXPECT_TRUE(strand2.running_in_this_thread());
          EXPECT_FALSE(strand1.running_in_this_thread());
          ++sanityCounter;
        },
        net::use_awaitable);
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    ++sanityCounter;
  };

  fut = net::co_spawn(strand1, nestedCoroutine(), net::use_future);
  ctx.poll();
  fut.get();
  EXPECT_EQ(sanityCounter, 2);
}

// _________________________________________________________________________
ASYNC_TEST(AsioHelpers, verifyInterruptibleDoesCheckCancellationHandle) {
  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  auto sleeperTask = []() -> net::awaitable<void> {
    net::steady_timer timer{co_await net::this_coro::executor,
                            DESIRED_CANCELLATION_CHECK_INTERVAL * 2};
    co_await timer.async_wait(net::deferred);
  }();

  EXPECT_THROW(
      co_await ad_utility::interruptible(std::move(sleeperTask), handle),
      ad_utility::CancellationException);
}

// _________________________________________________________________________
ASYNC_TEST(AsioHelpers, verifyInterruptibleDoesReportOnlyFirstError) {
  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  auto sleeperTask = []() -> net::awaitable<void> {
    throw std::runtime_error{"My error"};
    co_return;
  }();

  EXPECT_THROW(
      co_await ad_utility::interruptible(std::move(sleeperTask), handle),
      ad_utility::CancellationException);
}

// _________________________________________________________________________
ASYNC_TEST(AsioHelpers, verifyInterruptibleDoesPropagateError) {
  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  auto sleeperTask = []() -> net::awaitable<void> {
    throw std::runtime_error{"My error"};
    co_return;
  }();

  EXPECT_THROW(
      co_await ad_utility::interruptible(std::move(sleeperTask), handle),
      std::runtime_error);
}

// _________________________________________________________________________
ASYNC_TEST(AsioHelpers, verifyEarlyCancellationOfCallbackDoesCancelEarly) {
  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  auto sleeperTask = []() -> net::awaitable<void> { co_return; }();
  std::promise<std::function<void()>> promise{};
  auto future = promise.get_future();
  ad_utility::JThread cancelTask{[&future, &handle]() {
    future.get()();
    // Make sure first iteration is not affected
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
    handle->cancel(ad_utility::CancellationState::MANUAL);
  }};

  EXPECT_NO_THROW(co_await ad_utility::interruptible(
      std::move(sleeperTask), handle, std::move(promise)));
}

// _________________________________________________________________________
TEST(AsioHelpers, makeSureRunAndWaitForAwaitableWorksAsExpected) {
  net::io_context ioContext;
  auto workGuard = makeWorkGuard(ioContext);
  auto task = []() -> net::awaitable<int> { co_return 42; }();

  EXPECT_EQ(42, ad_utility::runAndWaitForAwaitable(std::move(task), ioContext));
}

// _________________________________________________________________________
TEST(AsioHelpers, makeSureRunAndWaitForAwaitablePropagatesErrorCorrectly) {
  net::io_context ioContext;
  auto workGuard = makeWorkGuard(ioContext);
  auto task = []() -> net::awaitable<int> {
    throw std::exception{};
    // Make the compiler throw the exception as part of the coroutine, not
    // when creating it.
    co_return 0;
  }();

  EXPECT_THROW(ad_utility::runAndWaitForAwaitable(std::move(task), ioContext),
               std::exception);
}

namespace {
// Occupy the single thread of the `pool` until the returned promise is
// fulfilled, and return only once the thread is occupied.
std::shared_ptr<std::promise<void>> occupyPool(net::static_thread_pool& pool) {
  auto release = std::make_shared<std::promise<void>>();
  auto occupied = std::make_shared<std::promise<void>>();
  net::post(pool, [release = release->get_future().share(), occupied]() {
    occupied->set_value();
    release.wait();
  });
  occupied->get_future().wait();
  return release;
}

// Wait until all tasks that were posted to the single-threaded `pool` before
// this call have run.
void waitForPool(net::static_thread_pool& pool) {
  std::promise<void> done;
  net::post(pool, [&done]() { done.set_value(); });
  done.get_future().wait();
}
}  // namespace

// Test that `runOnThreadPoolWithQueueDeadline` throws when no thread of the
// pool picks the function up in time and the pool then skips the function,
// and that a function which is picked up in time runs as usual.
TEST(AsioHelpers, runOnThreadPoolWithQueueDeadline) {
  using namespace std::chrono_literals;
  net::static_thread_pool pool{1};
  std::atomic<bool> functionHasRun{false};
  auto function = [&functionHasRun]() {
    functionHasRun = true;
    return 42;
  };
  // Run the `function` on the `pool` with the given `maxQueueWait` and return
  // its result (the coroutine runs on a strand of a fresh `io_context`).
  auto run = [&pool](std::chrono::milliseconds maxQueueWait, auto function) {
    net::io_context ctx;
    return runAndWaitForAwaitable(
        runOnThreadPoolWithQueueDeadline(pool, std::move(function),
                                         maxQueueWait, "deadline passed"),
        ctx);
  };

  // The thread of the pool is busy: the deadline passes, and the pool skips
  // the function when the thread is free again.
  auto release = occupyPool(pool);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(run(50ms, function),
                                        ::testing::StrEq("deadline passed"),
                                        ThreadPoolQueueDeadlineError);
  release->set_value();
  waitForPool(pool);
  EXPECT_FALSE(functionHasRun);

  // The thread is free: the function runs, also when it takes longer than the
  // deadline, also when it returns nothing, and its exception is propagated.
  EXPECT_EQ(run(50ms, function), 42);
  EXPECT_TRUE(functionHasRun);
  EXPECT_EQ(run(50ms,
                []() {
                  std::this_thread::sleep_for(150ms);
                  return 7;
                }),
            7);
  bool voidFunctionHasRun = false;
  run(50ms, [&voidFunctionHasRun]() { voidFunctionHasRun = true; });
  EXPECT_TRUE(voidFunctionHasRun);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      run(50ms, []() -> int { throw std::runtime_error{"function failed"}; }),
      ::testing::StrEq("function failed"), std::runtime_error);

  // The thread is busy and the coroutine is cancelled (via `interruptible`)
  // while waiting: the pool skips the function as well.
  functionHasRun = false;
  release = occupyPool(pool);
  auto handle = std::make_shared<CancellationHandle<>>();
  std::thread cancelAfterDelay{[&handle]() {
    std::this_thread::sleep_for(100ms);
    handle->cancel(CancellationState::MANUAL);
  }};
  net::io_context ctx;
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      runAndWaitForAwaitable(
          interruptible(runOnThreadPoolWithQueueDeadline(pool, function, 10s,
                                                         "deadline passed"),
                        handle),
          ctx),
      ::testing::_, CancellationException);
  cancelAfterDelay.join();
  release->set_value();
  waitForPool(pool);
  EXPECT_FALSE(functionHasRun);
}
