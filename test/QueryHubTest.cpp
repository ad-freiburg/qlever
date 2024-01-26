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
  auto senderNewId =
      co_await queryHub.createOrAcquireDistributorForSending(queryId);
  EXPECT_NE(distributor1, senderNewId);
  distributor1.reset();
  // The `distributor1` is referring to an old query with the same ID, so
  // resetting it should not impact the registry.
  EXPECT_EQ(senderNewId,
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

// _____________________________________________________________________________

namespace ad_utility::websocket {

// Ensures that nothing is scheduled when QueryHub is already destroyed
ASYNC_TEST(QueryHub, verifyNoOpOnDestroyedQueryHub) {
  {
    std::unique_ptr<QueryHub> queryHub = std::make_unique<QueryHub>(ioContext);
    auto distributor = co_await queryHub->createOrAcquireDistributorForSending(
        QueryId::idFromString("abc"));
    std::atomic_flag blocker = false;
    std::atomic_flag started = false;
    // Add task to "block" execution
    net::post(queryHub->globalStrand_, [&blocker, &started]() {
      started.test_and_set();
      started.notify_all();
      blocker.wait(false);
    });
    ad_utility::JThread thread{
        [&ioContext]() { EXPECT_EQ(1, ioContext.poll_one()); }};
    // Destroy object
    queryHub.reset();

    // Wait for thread to start
    started.wait(false);

    co_await distributor->signalEnd();

    // unblock global strand
    blocker.test_and_set();
    blocker.notify_all();
  }
  // Ensure no scheduled tasks left to run
  EXPECT_EQ(ioContext.poll(), 0);
}

// _____________________________________________________________________________

// Ensures that a scheduled task does not modify the map after destruction
// of the QueryHub.
ASYNC_TEST(QueryHub, verifyNoOpOnDestroyedQueryHubAfterSchedule) {
  std::weak_ptr<QueryHub::MapType> distributorMap;
  {
    std::unique_ptr<QueryHub> queryHub = std::make_unique<QueryHub>(ioContext);
    auto distributor = co_await queryHub->createOrAcquireDistributorForSending(
        QueryId::idFromString("abc"));
    std::atomic_flag blocker = false;
    std::atomic_flag started = false;
    // Add task to "block" execution
    net::post(queryHub->globalStrand_, [&blocker, &started]() {
      started.test_and_set();
      started.notify_all();
      blocker.wait(false);
    });
    ad_utility::JThread thread{
        [&ioContext]() { EXPECT_EQ(1, ioContext.poll_one()); }};

    // Wait for thread to start
    started.wait(false);

    co_await distributor->signalEnd();
    distributorMap = queryHub->socketDistributors_.getWeak();
    // Destroy object
    queryHub.reset();

    // unblock global strand
    blocker.test_and_set();
    blocker.notify_all();
  }
  // Ensure 1 scheduled task left to run
  EXPECT_EQ(ioContext.poll(), 1);
  EXPECT_TRUE(distributorMap.expired());
}

// _____________________________________________________________________________

ASYNC_TEST(QueryHub, verifyNoErrorWhenQueryIdMissing) {
  QueryHub queryHub{ioContext};
  auto queryId = QueryId::idFromString("abc");
  auto distributor =
      co_await queryHub.createOrAcquireDistributorForSending(queryId);
  // Under normal conditions this would happen when
  // `createOrAcquireDistributorForSending` is called after the reference count
  // of the previous distributor with the same id reached zero, but before the
  // destructor running the cleanup could remove the entry. Because this edge
  // case is very hard to simulate under real conditions (dependent on
  // scheduling of the operating system) we just fake it here to check the
  // behaviour we desire.
  queryHub.socketDistributors_.get().clear();
  EXPECT_NO_THROW(co_await distributor->signalEnd());
  EXPECT_TRUE(queryHub.socketDistributors_.get().empty());
}

// _____________________________________________________________________________

ASYNC_TEST_N(QueryHub, testCorrectReschedulingForEmptyPointerOnDestruct, 2) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");

  auto distributor =
      co_await queryHub.createOrAcquireDistributorForReceiving(queryId);
  std::weak_ptr<const QueryToSocketDistributor> comparison = distributor;

  co_await net::dispatch(
      net::bind_executor(queryHub.globalStrand_, net::use_awaitable));
  net::post(
      ioContext,
      std::packaged_task{[distributor = std::move(distributor)]() mutable {
        // Invoke destructor while the strand of
        // queryHub is still in use
        distributor.reset();
      }})
      .get();

  distributor = co_await queryHub.createOrAcquireDistributorInternalUnsafe(
      queryId, false);
  EXPECT_FALSE(!comparison.owner_before(distributor) &&
               !distributor.owner_before(comparison));

  // First run the cleanupCall of the initial distributor which now sees a
  // non-expired pointer, which is therefore not to be erased.
  co_await net::post(ioContext, net::use_awaitable);
}

}  // namespace ad_utility::websocket

// _____________________________________________________________________________

ASYNC_TEST(QueryHub, ensureOnlyOneSenderCanExist) {
  QueryHub queryHub{ioContext};
  QueryId queryId = QueryId::idFromString("abc");

  [[maybe_unused]] auto distributor =
      co_await queryHub.createOrAcquireDistributorForSending(queryId);
  EXPECT_THROW(co_await queryHub.createOrAcquireDistributorForSending(queryId),
               ad_utility::Exception);
}
