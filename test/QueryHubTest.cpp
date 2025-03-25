//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/AsyncTestHelpers.h"
#include "util/http/websocket/QueryHub.h"

using ad_utility::websocket::QueryHub;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::QueryToSocketDistributor;

ASIO_TEST(QueryHub, simulateLifecycleWithoutListeners) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  auto distributor = queryHub.createOrAcquireDistributorForSending(queryId);
  std::weak_ptr<QueryToSocketDistributor> observer = distributor;
  distributor.reset();
  EXPECT_TRUE(observer.expired());
}

// _____________________________________________________________________________
ASIO_TEST(QueryHub, simulateLifecycleWithExclusivelyListeners) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  std::weak_ptr<const QueryToSocketDistributor> observer;
  {
    auto distributor1 =
        queryHub.createOrAcquireDistributorForReceiving(queryId);
    auto distributor2 =
        queryHub.createOrAcquireDistributorForReceiving(queryId);
    EXPECT_EQ(distributor1, distributor2);
    observer = distributor1;
    // Shared pointers will be destroyed here
  }
  EXPECT_TRUE(observer.expired());
}

// _____________________________________________________________________________
ASIO_TEST(QueryHub, simulateStandardLifecycle) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  std::weak_ptr<const QueryToSocketDistributor> observer;
  {
    auto distributor1 =
        queryHub.createOrAcquireDistributorForReceiving(queryId);
    auto distributor2 = queryHub.createOrAcquireDistributorForSending(queryId);
    auto distributor3 =
        queryHub.createOrAcquireDistributorForReceiving(queryId);
    EXPECT_EQ(distributor1, distributor2);
    EXPECT_EQ(distributor2, distributor3);

    observer = distributor1;
    // Shared pointers will be destroyed here
  }
  EXPECT_TRUE(observer.expired());
}

// _____________________________________________________________________________
ASIO_TEST(QueryHub, verifySlowListenerDoesNotPreventCleanup) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");
  auto distributor1 = queryHub.createOrAcquireDistributorForReceiving(queryId);
  {
    auto distributor2 = queryHub.createOrAcquireDistributorForSending(queryId);
    EXPECT_EQ(distributor1, distributor2);
    distributor2->signalEnd();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  auto senderNewId = queryHub.createOrAcquireDistributorForSending(queryId);
  EXPECT_NE(distributor1, senderNewId);
  distributor1.reset();
  // The `distributor1` is referring to an old query with the same ID, so
  // resetting it should not impact the registry.
  EXPECT_EQ(senderNewId,
            queryHub.createOrAcquireDistributorForReceiving(queryId));
}

// _____________________________________________________________________________

ASIO_TEST(QueryHub, simulateLifecycleWithDifferentQueryIds) {
  QueryHub queryHub{ioContext};
  QueryId queryId1 = QueryId::idFromString("abc");
  QueryId queryId2 = QueryId::idFromString("def");
  auto distributor1 = queryHub.createOrAcquireDistributorForSending(queryId1);
  auto distributor2 = queryHub.createOrAcquireDistributorForSending(queryId2);
  auto distributor3 = queryHub.createOrAcquireDistributorForReceiving(queryId1);
  auto distributor4 = queryHub.createOrAcquireDistributorForReceiving(queryId2);
  EXPECT_NE(distributor1, distributor2);
  EXPECT_EQ(distributor1, distributor3);
  EXPECT_NE(distributor1, distributor4);
  EXPECT_NE(distributor2, distributor3);
  EXPECT_EQ(distributor2, distributor4);
  EXPECT_NE(distributor3, distributor4);
}

// _____________________________________________________________________________

namespace ad_utility::websocket {

// Ensures that nothing is scheduled when QueryHub is already destroyed
ASYNC_TEST(QueryHub, verifyNoOpOnDestroyedQueryHub) {
  std::unique_ptr<QueryHub> queryHub = std::make_unique<QueryHub>(ioContext);
  auto distributor = queryHub->createOrAcquireDistributorForSending(
      QueryId::idFromString("abc"));
  queryHub.reset();

  distributor->signalEnd();
  co_await net::post(net::use_awaitable);
  // Ensure no scheduled tasks left to run
  EXPECT_EQ(ioContext.poll(), 0);
}

// _____________________________________________________________________________

// Ensures that a scheduled task does not modify the map after destruction
// of the QueryHub.
ASIO_TEST(QueryHub, verifyNoOpOnDestroyedQueryHubAfterSchedule) {
  std::weak_ptr<QueryHub::MapType> distributorMap;
  std::unique_ptr<QueryHub> queryHub = std::make_unique<QueryHub>(ioContext);
  auto distributor = queryHub->createOrAcquireDistributorForSending(
      QueryId::idFromString("abc"));
  distributor->signalEnd();
  distributorMap = std::weak_ptr{queryHub->socketDistributors_};
  // Destroy object
  queryHub.reset();

  EXPECT_TRUE(distributorMap.expired());
  // Ensure 1 scheduled task left to run
  EXPECT_EQ(ioContext.poll(), 1);
}

// _____________________________________________________________________________

ASIO_TEST(QueryHub, verifyNoErrorWhenQueryIdMissing) {
  QueryHub queryHub{ioContext};
  auto queryId = QueryId::idFromString("abc");
  auto distributor = queryHub.createOrAcquireDistributorForSending(queryId);
  // Under normal conditions this would happen when
  // `createOrAcquireDistributorForSending` is called after the reference count
  // of the previous distributor with the same id reached zero, but before the
  // destructor running the cleanup could remove the entry. Because this edge
  // case is very hard to simulate under real conditions (dependent on
  // scheduling of the operating system) we just fake it here to check the
  // behaviour we desire.
  queryHub.socketDistributors_->wlock()->clear();
  EXPECT_NO_THROW(distributor->signalEnd());
  EXPECT_TRUE(queryHub.socketDistributors_->wlock()->empty());
}

}  // namespace ad_utility::websocket

// _____________________________________________________________________________

ASIO_TEST(QueryHub, ensureOnlyOneSenderCanExist) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");

  [[maybe_unused]] auto distributor =
      queryHub.createOrAcquireDistributorForSending(queryId);
  EXPECT_THROW(queryHub.createOrAcquireDistributorForSending(queryId),
               ad_utility::Exception);
}
