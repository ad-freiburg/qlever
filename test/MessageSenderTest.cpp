//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/experimental/awaitable_operators.hpp>

#include "util/AsyncTestHelpers.h"
#include "util/http/websocket/MessageSender.h"
#include "util/http/websocket/QueryHub.h"

using ad_utility::websocket::MessageSender;
using ad_utility::websocket::OwningQueryId;
using ad_utility::websocket::QueryHub;
using ad_utility::websocket::QueryRegistry;

using namespace boost::asio::experimental::awaitable_operators;
using namespace std::string_literals;

using ::testing::Pointee;
using ::testing::VariantWith;

ASYNC_TEST(MessageSender, destructorCallsSignalEnd) {
  QueryRegistry queryRegistry;
  OwningQueryId queryId = queryRegistry.uniqueId();
  QueryHub queryHub{ioContext};

  auto distributor = co_await queryHub.createOrAcquireDistributorForReceiving(
      queryId.toQueryId());

  co_await MessageSender::create(std::move(queryId), queryHub);

  net::deadline_timer timer{ioContext, boost::posix_time::seconds(2)};

  auto result = co_await (distributor->waitForNextDataPiece(0) ||
                          timer.async_wait(net::use_awaitable));

  using PayloadType = std::shared_ptr<const std::string>;

  EXPECT_THAT(result, VariantWith<PayloadType>(PayloadType{}));
}

// _____________________________________________________________________________

ASYNC_TEST(MessageSender, callingOperatorBroadcastsPayload) {
  QueryRegistry queryRegistry;
  OwningQueryId queryId = queryRegistry.uniqueId();
  QueryHub queryHub{ioContext};

  auto distributor = co_await queryHub.createOrAcquireDistributorForReceiving(
      queryId.toQueryId());

  auto updateWrapper =
      co_await MessageSender::create(std::move(queryId), queryHub);

  updateWrapper("Still");
  updateWrapper("Dre");

  net::deadline_timer timer{ioContext, boost::posix_time::seconds(2)};

  auto result = co_await (distributor->waitForNextDataPiece(0) ||
                          timer.async_wait(net::use_awaitable));

  using PayloadType = std::shared_ptr<const std::string>;

  EXPECT_THAT(result, VariantWith<PayloadType>(Pointee("Still"s)));

  result = co_await (distributor->waitForNextDataPiece(1) ||
                     timer.async_wait(net::use_awaitable));

  using PayloadType = std::shared_ptr<const std::string>;

  EXPECT_THAT(result, VariantWith<PayloadType>(Pointee("Dre"s)));
}
