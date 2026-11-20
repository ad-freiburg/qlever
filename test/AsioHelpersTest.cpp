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

// Test that `coSpawnWithPostedCompletion` passes the result or the exception
// of the coroutine to the handler (for a coroutine with and without a result),
// and that the handler runs on its associated executor.
TEST(AsioHelpers, coSpawnWithPostedCompletion) {
  net::io_context ctx;
  auto workGuard = makeWorkGuard(ctx);
  auto returnValue = []() -> net::awaitable<int> { co_return 42; };
  auto throwing = []() -> net::awaitable<int> {
    throw std::runtime_error{"blim"};
    co_return 0;
  };
  auto returnVoid = [](bool& flag) -> net::awaitable<void> {
    flag = true;
    co_return;
  };

  // The result is passed to the handler.
  std::optional<int> result;
  std::exception_ptr exception;
  coSpawnWithPostedCompletion(ctx.get_executor(), returnValue(),
                              [&](std::exception_ptr e, int value) {
                                exception = std::move(e);
                                result = value;
                              });
  ctx.poll();
  EXPECT_EQ(result, 42);
  EXPECT_EQ(exception, nullptr);

  // An exception is passed to the handler, together with a default-constructed
  // value.
  result.reset();
  coSpawnWithPostedCompletion(ctx.get_executor(), throwing(),
                              [&](std::exception_ptr e, int value) {
                                exception = std::move(e);
                                result = value;
                              });
  ctx.poll();
  EXPECT_EQ(result, 0);
  EXPECT_THROW(std::rethrow_exception(exception), std::runtime_error);

  // A coroutine without a result, with a future as the completion token.
  bool flag = false;
  auto future = coSpawnWithPostedCompletion(ctx.get_executor(),
                                            returnVoid(flag), net::use_future);
  ctx.poll();
  future.get();
  EXPECT_TRUE(flag);

  // The handler is posted to its associated executor, not to the executor of
  // the coroutine.
  auto strand = net::make_strand(ctx);
  bool handlerRanOnStrand = false;
  coSpawnWithPostedCompletion(
      ctx.get_executor(), returnValue(),
      net::bind_executor(strand, [&](std::exception_ptr, int) {
        handlerRanOnStrand = strand.running_in_this_thread();
      }));
  ctx.poll();
  EXPECT_TRUE(handlerRanOnStrand);
}

// Test that the completion handler of `coSpawnWithPostedCompletion` is really
// posted (and not dispatched, i.e. invoked inline): compared to a plain
// `co_spawn` with the same coroutine and a handler on the same executor, it
// takes exactly one more handler of the `io_context` until the completion
// handler has run.
TEST(AsioHelpers, coSpawnWithPostedCompletionReallyPosts) {
  auto returnValue = []() -> net::awaitable<int> { co_return 42; };
  // Return the number of handlers that the `io_context` runs until the
  // completion handler has run, when the `spawn` function is used.
  auto numStepsUntilHandlerRan = [&returnValue](auto spawn) {
    net::io_context ctx;
    bool handlerRan = false;
    spawn(ctx.get_executor(), returnValue(),
          [&handlerRan](std::exception_ptr, int) { handlerRan = true; });
    // NOTE: The loop also ends when the `io_context` runs out of work, so that
    // a handler that never runs fails the test instead of hanging it.
    size_t numSteps = 0;
    while (!handlerRan && ctx.poll_one() == 1) {
      ++numSteps;
    }
    EXPECT_TRUE(handlerRan);
    return numSteps;
  };
  size_t numStepsPlain =
      numStepsUntilHandlerRan([](auto executor, auto awaitable, auto handler) {
        net::co_spawn(executor, std::move(awaitable), std::move(handler));
      });
  size_t numStepsPosted =
      numStepsUntilHandlerRan([](auto executor, auto awaitable, auto handler) {
        coSpawnWithPostedCompletion(executor, std::move(awaitable),
                                    std::move(handler));
      });
  EXPECT_EQ(numStepsPosted, numStepsPlain + 1);
}

// Test that an exception which the coroutine throws on a thread of a pool
// reaches a handler that runs on another thread, many times in a row (the
// hand-over of an exception between threads is what ThreadSanitizer flags if
// it happens from inside the `catch` block, see `CallFunctionAndPassToHandler`
// in `RunFunctionOnExecutor.h`).
TEST(AsioHelpers, coSpawnWithPostedCompletionAcrossThreads) {
  net::thread_pool pool{4};
  net::io_context ctx;
  auto workGuard = makeWorkGuard(ctx);
  auto throwing = []() -> net::awaitable<int> {
    throw std::runtime_error{"blim"};
    co_return 0;
  };

  // The coroutines run on the pool, the handlers on the `io_context` that the
  // main thread runs, each handler drops its reference to the exception.
  constexpr size_t numRuns = 100;
  size_t numExceptions = 0;
  for (size_t i = 0; i < numRuns; ++i) {
    coSpawnWithPostedCompletion(
        pool.get_executor(), throwing(),
        net::bind_executor(ctx.get_executor(), [&](std::exception_ptr e, int) {
          EXPECT_THROW(std::rethrow_exception(std::move(e)),
                       std::runtime_error);
          ++numExceptions;
        }));
  }
  for (size_t i = 0; i < numRuns; ++i) {
    ctx.run_one();
  }
  pool.join();
  EXPECT_EQ(numExceptions, numRuns);
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
