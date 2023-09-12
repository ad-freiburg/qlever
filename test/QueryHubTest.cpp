//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/AsyncTestHelpers.h"
#include "util/http/websocket/QueryHub.h"

using ad_utility::websocket::QueryHub;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::QueryToSocketDistributor;

ASYNC_TEST(QueryHub, simulateLifecycleWithoutListeners) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  std::weak_ptr<QueryToSocketDistributor> observer =
      co_await queryHub.createOrAcquireDistributor(queryId, true);
  EXPECT_TRUE(observer.expired());
}

ASYNC_TEST(QueryHub, simulateLifecycleWithExclusivelyListeners) {
  QueryHub queryHub{ioContext};
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
}

ASYNC_TEST(QueryHub, simulateLifecycleWithSubsequentProviders) {
  QueryHub queryHub{ioContext};
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
}

ASYNC_TEST(QueryHub, simulateStandardLifecycle) {
  QueryHub queryHub{ioContext};
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
}

ASYNC_TEST(QueryHub, verifySlowListenerDoesNotPreventCleanup) {
  QueryHub queryHub{ioContext};
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
}

ASYNC_TEST(QueryHub, verifyDistributorIsStartedWithProvider) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  auto distributor =
      co_await queryHub.createOrAcquireDistributor(queryId, true);
  EXPECT_TRUE(co_await distributor->signalStartIfNotStarted());
}

ASYNC_TEST(QueryHub, verifyDistributorIsNotStartedWithListener) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  auto distributor =
      co_await queryHub.createOrAcquireDistributor(queryId, false);
  EXPECT_FALSE(co_await distributor->signalStartIfNotStarted());
}

ASYNC_TEST(QueryHub, simulateLifecycleWithDifferentQueryIds) {
  QueryHub queryHub{ioContext};
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
}
