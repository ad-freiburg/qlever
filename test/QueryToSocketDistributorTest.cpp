//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/experimental/awaitable_operators.hpp>

#include "util/AsyncTestHelpers.h"
#include "util/Exception.h"
#include "util/TransparentFunctors.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace net = boost::asio;

using ad_utility::websocket::QueryToSocketDistributor;
using namespace boost::asio::experimental::awaitable_operators;
using namespace std::string_literals;

using ::testing::Pointee;
static constexpr auto noop = ad_utility::noop;

// Hack to allow ASSERT_*() macros to work with ASYNC_TEST
#define return co_return

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateThrowsWhenFinished) {
  auto ptr = std::make_shared<QueryToSocketDistributor>(ioContext, noop);
  QueryToSocketDistributor& queryToSocketDistributor = *ptr;
  queryToSocketDistributor.signalEnd();
  EXPECT_THROW(queryToSocketDistributor.addQueryStatusUpdate("Abc"),
               ad_utility::Exception);
  co_return;
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, signalEndRunsCleanup) {
  bool executed = false;
  auto ptr = std::make_shared<QueryToSocketDistributor>(
      ioContext, [&](bool signalEndCalled) { executed = signalEndCalled; });
  QueryToSocketDistributor& queryToSocketDistributor = *ptr;
  EXPECT_FALSE(executed);
  queryToSocketDistributor.signalEnd();
  EXPECT_TRUE(executed);
  co_return;
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, destructorRunsCleanup) {
  uint32_t counter = 0;
  {
    QueryToSocketDistributor queryToSocketDistributor{ioContext,
                                                      [&](bool) { counter++; }};
    EXPECT_EQ(counter, 0);
  }
  EXPECT_EQ(counter, 1);
  // Make sure this is interpreted as a coroutine
  co_return;
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, doubleSignalEndThrowsError) {
  auto ptr = std::make_shared<QueryToSocketDistributor>(ioContext, noop);
  QueryToSocketDistributor& queryToSocketDistributor = *ptr;
  queryToSocketDistributor.signalEnd();
  EXPECT_THROW(queryToSocketDistributor.signalEnd(), ad_utility::Exception);
  co_return;
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, signalEndWakesUpListeners) {
  auto ptr = std::make_shared<QueryToSocketDistributor>(ioContext, noop);
  QueryToSocketDistributor& queryToSocketDistributor = *ptr;
  auto impl = [&]() -> net::awaitable<void> {
    bool waiting = false;
    auto listener = [&]() -> net::awaitable<void> {
      waiting = true;
      EXPECT_EQ(nullptr,
                co_await queryToSocketDistributor.waitForNextDataPiece(0));
    };

    auto broadcaster = [&]() -> net::awaitable<void> {
      // Ensure correct order of execution
      ASSERT_TRUE(waiting);
      queryToSocketDistributor.signalEnd();
      co_return;
    };
    co_await (listener() && broadcaster());
  };
  co_await net::co_spawn(queryToSocketDistributor.strand(), impl,
                         net::use_awaitable);
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateWakesUpListeners) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, noop};
  auto impl = [&]() -> net::awaitable<void> {
    bool waiting = false;
    auto listener = [&]() -> net::awaitable<void> {
      waiting = true;
      auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
      EXPECT_THAT(result, Pointee("Abc"s));
    };

    auto broadcaster = [&]() -> net::awaitable<void> {
      // Ensure correct order of execution
      ASSERT_TRUE(waiting);
      queryToSocketDistributor.addQueryStatusUpdate("Abc");
      co_return;
    };
    co_await (listener() && broadcaster());
  };
  co_await net::co_spawn(queryToSocketDistributor.strand(), impl,
                         net::use_awaitable);
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, listeningBeforeStartWorks) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, noop};
  auto impl = [&]() -> net::awaitable<void> {
    bool waiting = false;
    auto listener = [&]() -> net::awaitable<void> {
      waiting = true;
      auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
      EXPECT_THAT(result, Pointee("Abc"s));
    };

    auto broadcaster = [&]() -> net::awaitable<void> {
      // Ensure correct order of execution
      ASSERT_TRUE(waiting);
      queryToSocketDistributor.addQueryStatusUpdate("Abc");
      co_return;
    };
    co_await (listener() && broadcaster());
  };
  co_await net::co_spawn(queryToSocketDistributor.strand(), impl,
                         net::use_awaitable);
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, addQueryStatusUpdateBeforeListenersWorks) {
  QueryToSocketDistributor queryToSocketDistributor{ioContext, noop};

  auto impl = [&]() -> net::awaitable<void> {
    queryToSocketDistributor.addQueryStatusUpdate("Abc");
    queryToSocketDistributor.addQueryStatusUpdate("Def");
    auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
    EXPECT_THAT(result, Pointee("Abc"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(1);
    EXPECT_THAT(result, Pointee("Def"s));
  };
  co_await net::co_spawn(queryToSocketDistributor.strand(), impl,
                         net::use_awaitable);
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, signalEndDoesNotPreventConsumptionOfRest) {
  auto ptr = std::make_shared<QueryToSocketDistributor>(ioContext, noop);
  QueryToSocketDistributor& queryToSocketDistributor = *ptr;
  queryToSocketDistributor.addQueryStatusUpdate("Abc");
  queryToSocketDistributor.addQueryStatusUpdate("Def");
  queryToSocketDistributor.signalEnd();

  auto impl = [&]() -> net::awaitable<void> {
    auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
    EXPECT_THAT(result, Pointee("Abc"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(1);
    EXPECT_THAT(result, Pointee("Def"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(2);
    ASSERT_FALSE(result);
  };
  co_await (net::co_spawn(queryToSocketDistributor.strand(), impl,
                          net::use_awaitable));
}

// _____________________________________________________________________________

ASYNC_TEST(QueryToSocketDistributor, fullConsumptionAfterSignalEndWorks) {
  auto ptr = std::make_shared<QueryToSocketDistributor>(ioContext, noop);
  QueryToSocketDistributor& queryToSocketDistributor = *ptr;

  queryToSocketDistributor.addQueryStatusUpdate("Abc");
  queryToSocketDistributor.addQueryStatusUpdate("Def");
  queryToSocketDistributor.signalEnd();

  auto impl = [&]() -> net::awaitable<void> {
    auto result = co_await queryToSocketDistributor.waitForNextDataPiece(0);
    EXPECT_THAT(result, Pointee("Abc"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(1);
    EXPECT_THAT(result, Pointee("Def"s));
    result = co_await queryToSocketDistributor.waitForNextDataPiece(2);
    EXPECT_FALSE(result);
  };
  co_await (net::co_spawn(queryToSocketDistributor.strand(), impl,
                          net::use_awaitable));
}
