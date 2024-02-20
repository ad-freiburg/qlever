//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/AsyncTestHelpers.h"
#include "util/http/websocket/QueryHub.h"
#include "util/http/websocket/UpdateFetcher.h"

using ad_utility::websocket::QueryHub;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::UpdateFetcher;

using namespace std::string_literals;

using ::testing::Pointee;

ASYNC_TEST(UpdateFetcher, checkIndexIncrements) {
  QueryId queryId = QueryId::idFromString("abc");
  QueryHub queryHub{ioContext};
  UpdateFetcher updateFetcher{queryHub, queryId};

  auto distributor = queryHub.createOrAcquireDistributorForSending(queryId);
  distributor->addQueryStatusUpdate("1");
  distributor->addQueryStatusUpdate("2");

  auto impl = [&]() -> net::awaitable<void> {
    auto payload = co_await updateFetcher.waitForEvent();
    EXPECT_THAT(payload, Pointee("1"s));
    payload = co_await updateFetcher.waitForEvent();
    EXPECT_THAT(payload, Pointee("2"s));
  };
  co_await net::co_spawn(updateFetcher.strand(), impl, net::use_awaitable);
}
