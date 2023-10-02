//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include "util/AsioHelpers.h"

namespace net = boost::asio;

using ad_utility::originalExecutor;
using namespace boost::asio::experimental::awaitable_operators;

TEST(AsioHelpers, originalExecutor) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand2]() -> net::awaitable<int> {
    co_await net::post(strand2, net::use_awaitable);
    // sanity check
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    co_return 1337;
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable,
                         strand1]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    auto value = co_await originalExecutor(innerAwaitable());
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_EQ(value, 1337);
    sanityCounter++;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

TEST(AsioHelpers, originalExecutorVoidOverload) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  bool sanityFlag = false;

  auto outerAwaitable = [&sanityFlag, strand1,
                         strand2]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    co_await originalExecutor(net::post(strand2, net::use_awaitable));
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    sanityFlag = true;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_TRUE(sanityFlag);
}

// _____________________________________________________________________________

TEST(AsioHelpers, originalExecutorWhenException) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand2]() -> net::awaitable<int> {
    co_await net::post(strand2, net::use_awaitable);
    // sanity check
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    throw std::runtime_error{"Expected"};
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable,
                         strand1]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_THROW(co_await originalExecutor(innerAwaitable()),
                 std::runtime_error);
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

TEST(AsioHelpers, originalExecutorVoidOverloadWhenException) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand2]() -> net::awaitable<void> {
    co_await net::post(strand2, net::use_awaitable);
    // sanity check
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    throw std::runtime_error{"Expected"};
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable,
                         strand1]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_THROW(co_await originalExecutor(innerAwaitable()),
                 std::runtime_error);
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

template <typename T, typename Rep, typename Period>
net::awaitable<T> cancelAfter(net::awaitable<T> coroutine,
                              std::chrono::duration<Rep, Period> duration) {
  net::steady_timer timer{co_await net::this_coro::executor, duration};
  co_await (std::move(coroutine) || timer.async_wait(net::use_awaitable));
}
// _____________________________________________________________________________

// Checks that behavior is consistent for cancellation case
TEST(AsioHelpers, originalExecutorWhenCancelled) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);
  auto strand3 = net::make_strand(ioContext);
  net::deadline_timer infiniteTimer{ioContext,
                                    static_cast<net::deadline_timer::time_type>(
                                        boost::posix_time::pos_infin)};

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand2,
                         &infiniteTimer]() -> net::awaitable<void> {
    co_await net::post(strand2, net::use_awaitable);
    // sanity check
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    co_await infiniteTimer.async_wait(net::use_awaitable);
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable, strand1, strand2,
                         strand3]() -> net::awaitable<void> {
    co_await net::post(strand1, net::use_awaitable);
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_THROW(co_await originalExecutor(innerAwaitable()),
                 boost::system::system_error);
    // Verify we're on the strand the cancellation happened
    EXPECT_TRUE(strand3.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand3,
                cancelAfter(outerAwaitable(), std::chrono::milliseconds(10)),
                net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}

// _____________________________________________________________________________

// Checks that behavior is consistent for cancellation case
TEST(AsioHelpers, originalExecutorVoidOverloadWhenCancelled) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);
  auto strand3 = net::make_strand(ioContext);
  net::deadline_timer infiniteTimer{ioContext,
                                    static_cast<net::deadline_timer::time_type>(
                                        boost::posix_time::pos_infin)};

  uint32_t sanityCounter = 0;

  auto innerAwaitable = [&sanityCounter, strand2,
                         &infiniteTimer]() -> net::awaitable<void> {
    co_await net::post(strand2, net::use_awaitable);
    // sanity check
    EXPECT_TRUE(strand2.running_in_this_thread());
    sanityCounter++;
    co_await infiniteTimer.async_wait(net::use_awaitable);
  };

  auto outerAwaitable = [&sanityCounter, &innerAwaitable, strand1, strand2,
                         strand3]() -> net::awaitable<void> {
    co_await net::post(strand1, net::use_awaitable);
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    EXPECT_THROW(co_await originalExecutor(innerAwaitable()),
                 boost::system::system_error);
    // Verify we're on the strand the cancellation happened
    EXPECT_TRUE(strand3.running_in_this_thread());
    sanityCounter++;
  };

  net::co_spawn(strand3,
                cancelAfter(outerAwaitable(), std::chrono::milliseconds(10)),
                net::detached);

  ioContext.run();

  EXPECT_EQ(sanityCounter, 2);
}
