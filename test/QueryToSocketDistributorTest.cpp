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

// Hack to allow ASSERT_*() macros to work with ASYNC_TEST
#define return co_return

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateThrowsWhenFinished) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  co_await queryToSocketDistributor.signalEnd();
  EXPECT_THROW(co_await queryToSocketDistributor.addQueryStatusUpdate("Abc"),
               ad_utility::Exception);
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, signalEndRunsCleanup) {
  bool executed = false;
  QueryToSocketDistributor queryToSocketDistributor{ioContext,
                                                    [&]() { executed = true; }};
  EXPECT_FALSE(executed);
  co_await queryToSocketDistributor.signalEnd();
  EXPECT_TRUE(executed);
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, destructorRunsCleanup) {
  uint32_t counter = 0;
  {
    QueryToSocketDistributor queryToSocketDistributor{ioContext,
                                                      [&]() { counter++; }};
    EXPECT_EQ(counter, 0);
  }
  EXPECT_EQ(counter, 1);
  // Make sure this is interpreted as a coroutine
  co_return;
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, doubleSignalEndThrowsError) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  co_await queryToSocketDistributor.signalEnd();
  EXPECT_THROW(co_await queryToSocketDistributor.signalEnd(),
               ad_utility::Exception);
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, signalEndWakesUpListeners) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  auto listener = [&]() -> net::awaitable<void> {
    waiting = true;
    EXPECT_EQ(nullptr,
              co_await queryToSocketDistributor.waitForNextDataPiece(0));
  };

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(waiting);
    co_await queryToSocketDistributor.signalEnd();
  };
  co_await (listener() && broadcaster());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateWakesUpListeners) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  auto listener = [&]() -> net::awaitable<void> {
    waiting = true;
    auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
    EXPECT_THAT(result, Pointee("Abc"s));
  };

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(waiting);
    co_await queryToSocketDistributor.addQueryStatusUpdate("Abc");
  };
  co_await (listener() && broadcaster());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, listeningBeforeStartWorks) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  auto listener = [&]() -> net::awaitable<void> {
    waiting = true;
    auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
    EXPECT_THAT(result, Pointee("Abc"s));
  };

  auto broadcaster = [&]() -> net::awaitable<void> {
    // Ensure correct order of execution
    ASSERT_TRUE(waiting);
    co_await queryToSocketDistributor.addQueryStatusUpdate("Abc");
  };
  co_await (listener() && broadcaster());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateBeforeListenersWorks) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  bool doneAdding = false;

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
    auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
    EXPECT_THAT(result, Pointee("Abc"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(1);
    EXPECT_THAT(result, Pointee("Def"s));
  };
  co_await (broadcaster() && listener());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, signalEndDoesNotPreventConsumptionOfRest) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, []() {}};
  bool waiting = false;
  bool doneAdding = false;

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
    auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
    EXPECT_THAT(result, Pointee("Abc"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(1);
    EXPECT_THAT(result, Pointee("Def"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(2);
    ASSERT_FALSE(result);
  };
  co_await (broadcaster() && listener());
}
