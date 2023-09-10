//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include "util/http/websocket/QueryHub.h"

using ad_utility::websocket::QueryHub;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::QueryToSocketDistributor;
namespace net = boost::asio;

template <typename Func>
void testRunner(Func func) {
  net::io_context ioContext;
  QueryHub queryHub{ioContext};
  bool sanityFlag = false;

  auto lambdaWrapper = [&]() -> net::awaitable<void> {
    co_await func(queryHub);
    sanityFlag = true;
  };

  net::co_spawn(ioContext, lambdaWrapper(), net::detached);
  ioContext.run();
  EXPECT_TRUE(sanityFlag);
}

TEST(QueryHub, simulateLifecycleWithoutListeners) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId = QueryId::idFromString("abc");
    std::weak_ptr<QueryToSocketDistributor> observer =
        co_await queryHub.createOrAcquireDistributor(queryId, true);
    EXPECT_TRUE(observer.expired());
  });
}

TEST(QueryHub, simulateLifecycleWithExclusivelyListeners) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId = QueryId::idFromString("abc");
    std::weak_ptr<QueryToSocketDistributor> observer;
    {
      auto distributor1 =
          co_await queryHub.createOrAcquireDistributor(queryId, false);
      auto distributor2 =
          co_await queryHub.createOrAcquireDistributor(queryId, false);
      EXPECT_EQ(distributor1, distributor2);
      observer = distributor1;
    }
    EXPECT_TRUE(observer.expired());
  });
}

TEST(QueryHub, simulateLifecycleWithSubsequentProviders) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId = QueryId::idFromString("abc");
    std::weak_ptr<QueryToSocketDistributor> observer1;
    std::weak_ptr<QueryToSocketDistributor> observer2;
    {
      auto distributor1 =
          co_await queryHub.createOrAcquireDistributor(queryId, true);
      auto distributor2 =
          co_await queryHub.createOrAcquireDistributor(queryId, true);
      EXPECT_NE(distributor1, distributor2);

      observer1 = distributor1;
      observer2 = distributor2;
    }
    EXPECT_TRUE(observer1.expired());
    EXPECT_TRUE(observer2.expired());
  });
}

TEST(QueryHub, simulateStandardLifecycle) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId = QueryId::idFromString("abc");
    std::weak_ptr<QueryToSocketDistributor> observer;
    {
      auto distributor1 =
          co_await queryHub.createOrAcquireDistributor(queryId, false);
      auto distributor2 =
          co_await queryHub.createOrAcquireDistributor(queryId, true);
      auto distributor3 =
          co_await queryHub.createOrAcquireDistributor(queryId, false);
      EXPECT_EQ(distributor1, distributor2);
      EXPECT_EQ(distributor2, distributor3);

      observer = distributor1;
    }
    EXPECT_TRUE(observer.expired());
  });
}

TEST(QueryHub, verifySlowListenerDoesNotPreventCleanup) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId = QueryId::idFromString("abc");
    auto distributor1 =
        co_await queryHub.createOrAcquireDistributor(queryId, false);
    {
      auto distributor2 =
          co_await queryHub.createOrAcquireDistributor(queryId, true);
      EXPECT_EQ(distributor1, distributor2);
    }
    EXPECT_NE(distributor1,
              co_await queryHub.createOrAcquireDistributor(queryId, true));
    EXPECT_NE(distributor1,
              co_await queryHub.createOrAcquireDistributor(queryId, false));
  });
}

TEST(QueryHub, verifyDistributorIsStartedWithProvider) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId = QueryId::idFromString("abc");
    auto distributor =
        co_await queryHub.createOrAcquireDistributor(queryId, true);
    EXPECT_TRUE(co_await distributor->signalStartIfNotStarted());
  });
}

TEST(QueryHub, verifyDistributorIsNotStartedWithListener) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId = QueryId::idFromString("abc");
    auto distributor =
        co_await queryHub.createOrAcquireDistributor(queryId, false);
    EXPECT_FALSE(co_await distributor->signalStartIfNotStarted());
  });
}

TEST(QueryHub, simulateLifecycleWithDifferentQueryIds) {
  testRunner([](auto& queryHub) -> net::awaitable<void> {
    QueryId queryId1 = QueryId::idFromString("abc");
    QueryId queryId2 = QueryId::idFromString("def");
    auto distributor1 =
        co_await queryHub.createOrAcquireDistributor(queryId1, true);
    auto distributor2 =
        co_await queryHub.createOrAcquireDistributor(queryId2, true);
    auto distributor3 =
        co_await queryHub.createOrAcquireDistributor(queryId1, true);
    auto distributor4 =
        co_await queryHub.createOrAcquireDistributor(queryId2, true);
    EXPECT_NE(distributor1, distributor2);
    EXPECT_NE(distributor1, distributor3);
    EXPECT_NE(distributor1, distributor4);
    EXPECT_NE(distributor2, distributor3);
    EXPECT_NE(distributor2, distributor4);
    EXPECT_NE(distributor3, distributor4);
  });
}
