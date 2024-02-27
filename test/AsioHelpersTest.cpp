//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/AsioHelpers.h"
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
  runFunctionOnExecutor(
      ctx.get_executor(), [&]() { a = true; }, net::detached);
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
