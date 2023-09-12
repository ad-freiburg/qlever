//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/experimental/awaitable_operators.hpp>

#include "util/AsyncTestHelpers.h"
#include "util/Exception.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace net = boost::asio;

using ad_utility::websocket::QueryToSocketDistributor;
using namespace boost::asio::experimental::awaitable_operators;
using namespace std::string_literals;

using ::testing::Pointee;

template <typename T>
net::awaitable<T> withTimeout(net::awaitable<T> t) {
  net::deadline_timer timer{co_await net::this_coro::executor,
                            boost::posix_time::seconds(2)};
  auto variant =
      co_await (std::move(t) || timer.async_wait(net::use_awaitable));
  if (std::holds_alternative<T>(variant)) {
    co_return std::get<0>(variant);
  }
  throw std::runtime_error{"Timeout while waiting for awaitable"};
}

// Hack to allow ASSERT_*() macros to work with ASYNC_TEST
#define return co_return

ASYNC_TEST(QueryToSocketDistributor, signalStartWorks) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  EXPECT_FALSE(co_await queryToSocketDistributor.signalStartIfNotStarted());
  EXPECT_TRUE(co_await queryToSocketDistributor.signalStartIfNotStarted());
  EXPECT_TRUE(co_await queryToSocketDistributor.signalStartIfNotStarted());
}

ASYNC_TEST(QueryToSocketDistributor, signalEndThrowsWhenNotStarted) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  EXPECT_THROW(co_await queryToSocketDistributor.signalEnd(),
               ad_utility::Exception);
}

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateThrowsWhenFinished) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  co_await queryToSocketDistributor.signalStartIfNotStarted();
  co_await queryToSocketDistributor.signalEnd();
  EXPECT_THROW(co_await queryToSocketDistributor.addQueryStatusUpdate("Abc"),
               ad_utility::Exception);
}

ASYNC_TEST(QueryToSocketDistributor, signalEndRunsCleanup) {
  bool executed = false;
  QueryToSocketDistributor queryToSocketDistributor{ioContext,
                                                    [&]() { executed = true; }};
  co_await queryToSocketDistributor.signalStartIfNotStarted();
  EXPECT_FALSE(executed);
  co_await queryToSocketDistributor.signalEnd();
  EXPECT_TRUE(executed);
}

ASYNC_TEST(QueryToSocketDistributor, destructorRunsCleanup) {
  bool executed = false;
  {
    QueryToSocketDistributor queryToSocketDistributor{
        ioContext, [&]() { executed = true; }};
    EXPECT_FALSE(executed);
  }
  EXPECT_TRUE(executed);
  // Make sure this is interpreted as a coroutine
  co_return;
}

ASYNC_TEST(QueryToSocketDistributor, doubleSignalEndThrowsError) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  co_await queryToSocketDistributor.signalStartIfNotStarted();
  co_await queryToSocketDistributor.signalEnd();
  EXPECT_THROW(co_await queryToSocketDistributor.signalEnd(),
               ad_utility::Exception);
}

ASYNC_TEST(QueryToSocketDistributor, signalEndWakesUpListeners) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  co_await queryToSocketDistributor.signalStartIfNotStarted();
  auto listener = [&]() -> net::awaitable<void> {
    waiting = true;
    co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(0));
  };

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(waiting);
    co_await queryToSocketDistributor.signalEnd();
  };
  co_await (listener() && broadcaster());
}

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateWakesUpListeners) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  co_await queryToSocketDistributor.signalStartIfNotStarted();
  auto listener = [&]() -> net::awaitable<void> {
    waiting = true;
    auto result =
        co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(0));
    EXPECT_THAT(result, Pointee("Abc"s));
  };

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(waiting);
    co_await queryToSocketDistributor.addQueryStatusUpdate("Abc");
  };
  co_await (listener() && broadcaster());
}

ASYNC_TEST(QueryToSocketDistributor, listeningBeforeStartWorks) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  auto listener = [&]() -> net::awaitable<void> {
    waiting = true;
    auto result =
        co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(0));
    EXPECT_THAT(result, Pointee("Abc"s));
  };

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(waiting);
    co_await queryToSocketDistributor.signalStartIfNotStarted();
    co_await queryToSocketDistributor.addQueryStatusUpdate("Abc");
  };
  co_await (listener() && broadcaster());
}

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateBeforeListenersWorks) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  bool doneAdding = false;
  co_await queryToSocketDistributor.signalStartIfNotStarted();

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_FALSE(waiting);
    co_await queryToSocketDistributor.addQueryStatusUpdate("Abc");
    co_await queryToSocketDistributor.addQueryStatusUpdate("Def");
    doneAdding = true;
  };

  auto listener = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(doneAdding);
    waiting = true;
    auto result =
        co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(0));
    EXPECT_THAT(result, Pointee("Abc"s));
    result =
        co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(1));
    EXPECT_THAT(result, Pointee("Def"s));
  };
  co_await (broadcaster() && listener());
}

ASYNC_TEST(QueryToSocketDistributor, signalEndDoesNotPreventConsumptionOfRest) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  bool doneAdding = false;
  co_await queryToSocketDistributor.signalStartIfNotStarted();

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_FALSE(waiting);
    co_await queryToSocketDistributor.addQueryStatusUpdate("Abc");
    co_await queryToSocketDistributor.addQueryStatusUpdate("Def");
    co_await queryToSocketDistributor.signalEnd();
    doneAdding = true;
  };

  auto listener = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(doneAdding);
    waiting = true;
    auto result =
        co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(0));
    EXPECT_THAT(result, Pointee("Abc"s));
    result =
        co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(1));
    EXPECT_THAT(result, Pointee("Def"s));
    result =
        co_await withTimeout(queryToSocketDistributor.waitForNextDataPiece(2));
    ASSERT_FALSE(result);
  };
  co_await (broadcaster() && listener());
}
