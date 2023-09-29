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
  auto distributor =
      co_await queryHub.createOrAcquireDistributorForSending(queryId);
  std::weak_ptr<QueryToSocketDistributor> observer = distributor;
  distributor.reset();
  EXPECT_TRUE(observer.expired());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryHub, simulateLifecycleWithExclusivelyListeners) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  std::weak_ptr<const QueryToSocketDistributor> observer;
  {
    auto distributor1 =
        co_await queryHub.createOrAcquireDistributorForReceiving(queryId);
    auto distributor2 =
        co_await queryHub.createOrAcquireDistributorForReceiving(queryId);
    EXPECT_EQ(distributor1, distributor2);
    observer = distributor1;
    // Shared pointers will be destroyed here
  }
  EXPECT_TRUE(observer.expired());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryHub, simulateStandardLifecycle) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  std::weak_ptr<const QueryToSocketDistributor> observer;
  {
    auto distributor1 =
        co_await queryHub.createOrAcquireDistributorForReceiving(queryId);
    auto distributor2 =
        co_await queryHub.createOrAcquireDistributorForSending(queryId);
    auto distributor3 =
        co_await queryHub.createOrAcquireDistributorForReceiving(queryId);
    EXPECT_EQ(distributor1, distributor2);
    EXPECT_EQ(distributor2, distributor3);

    observer = distributor1;
    // Shared pointers will be destroyed here
  }
  EXPECT_TRUE(observer.expired());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryHub, verifySlowListenerDoesNotPreventCleanup) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  auto distributor1 =
      co_await queryHub.createOrAcquireDistributorForReceiving(queryId);
  {
    auto distributor2 =
        co_await queryHub.createOrAcquireDistributorForSending(queryId);
    EXPECT_EQ(distributor1, distributor2);
    co_await distributor2->signalEnd();
  }
  EXPECT_NE(distributor1,
            co_await queryHub.createOrAcquireDistributorForSending(queryId));
  EXPECT_NE(distributor1,
            co_await queryHub.createOrAcquireDistributorForReceiving(queryId));
}

// _____________________________________________________________________________

ASYNC_TEST(QueryHub, simulateLifecycleWithDifferentQueryIds) {
  QueryHub queryHub{ioContext};
  QueryId queryId1 = QueryId::idFromString("abc");
  QueryId queryId2 = QueryId::idFromString("def");
  auto distributor1 =
      co_await queryHub.createOrAcquireDistributorForSending(queryId1);
  auto distributor2 =
      co_await queryHub.createOrAcquireDistributorForSending(queryId2);
  auto distributor3 =
      co_await queryHub.createOrAcquireDistributorForReceiving(queryId1);
  auto distributor4 =
      co_await queryHub.createOrAcquireDistributorForReceiving(queryId2);
  EXPECT_NE(distributor1, distributor2);
  EXPECT_EQ(distributor1, distributor3);
  EXPECT_NE(distributor1, distributor4);
  EXPECT_NE(distributor2, distributor3);
  EXPECT_EQ(distributor2, distributor4);
  EXPECT_NE(distributor3, distributor4);
}
