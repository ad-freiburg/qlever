//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/AsioHelpers.h"
#include "util/http/beast.h"

using namespace ad_utility;
// _________________________________________________________________________
TEST(AsioHelpers, runFunctionOnExecutorVoid) {
  net::io_context ctx;
  bool a = false;
  runFunctionOnExecutor(
      ctx.get_executor(), [&]() { a = true; }, net::detached);
  EXPECT_FALSE(a);
  ctx.poll();
  EXPECT_TRUE(a);

  // The exception disappears into the void, because the result is `detached`.
  ctx.restart();
  runFunctionOnExecutor(
      ctx.get_executor(), [&]() { throw std::runtime_error{"blim"}; },
      net::detached);
  ctx.poll();

  a = false;
  ctx.restart();
  auto fut = runFunctionOnExecutor(
      ctx.get_executor(), [&]() { a = true; }, net::use_future);
  EXPECT_FALSE(a);
  EXPECT_EQ(1, ctx.poll());
  EXPECT_TRUE(a);
  fut.get();

  a = false;
  ctx.restart();
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
  auto fut = runFunctionOnExecutor(
      ctx.get_executor(), [&]() { return 12; }, net::use_future);
  EXPECT_EQ(1, ctx.poll());
  EXPECT_EQ(fut.get(), 12);

  ctx.restart();
  fut = runFunctionOnExecutor(
      ctx.get_executor(), [&]() -> int { throw std::runtime_error{"blim"}; },
      net::use_future);
  EXPECT_EQ(1, ctx.poll());
  EXPECT_THROW(fut.get(), std::runtime_error);
}

// _________________________________________________________________________
TEST(AsioHelpers, runFunctionOnExecutorStrands) {
  net::io_context ctx;
  auto strand1 = net::make_strand(ctx);
  auto strand2 = net::make_strand(ctx);

  auto fut = runFunctionOnExecutor(
      strand1,
      [&]() {
        EXPECT_TRUE(strand1.running_in_this_thread());
        EXPECT_FALSE(strand2.running_in_this_thread());
      },
      net::use_future);
  EXPECT_EQ(1, ctx.poll());
  fut.get();

  ctx.restart();
  auto nestedCoroutine = [&]() -> net::awaitable<void> {
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
    co_await runFunctionOnExecutor(
        strand2,
        [&]() {
          EXPECT_TRUE(strand2.running_in_this_thread());
          EXPECT_FALSE(strand1.running_in_this_thread());
        },
        net::use_awaitable);
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_FALSE(strand2.running_in_this_thread());
  };

  fut = net::co_spawn(strand1, nestedCoroutine(), net::use_future);
  ctx.run();
  fut.get();
}
