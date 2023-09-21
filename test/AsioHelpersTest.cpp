//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include "util/AsioHelpers.h"

namespace net = boost::asio;

using ad_utility::sameExecutor;

TEST(AsioHelpers, sameExecutor) {
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
                         &strand1]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    auto value = co_await sameExecutor(innerAwaitable());
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

TEST(AsioHelpers, sameExecutorVoidOverload) {
  net::io_context ioContext{};
  auto strand1 = net::make_strand(ioContext);
  auto strand2 = net::make_strand(ioContext);

  bool sanityFlag = false;

  auto outerAwaitable = [&sanityFlag, strand1,
                         strand2]() -> net::awaitable<void> {
    // Sanity check
    EXPECT_TRUE(strand1.running_in_this_thread());
    co_await sameExecutor(net::post(strand2, net::use_awaitable));
    // Verify we're back on the same strand
    EXPECT_TRUE(strand1.running_in_this_thread());
    sanityFlag = true;
  };

  net::co_spawn(strand1, outerAwaitable(), net::detached);

  ioContext.run();

  EXPECT_TRUE(sanityFlag);
}
