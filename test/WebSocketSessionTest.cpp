//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/beast/core/flat_buffer.hpp>

#include "util/AsyncTestHelpers.h"
#include "util/BeastTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/http/websocket/MessageSender.h"
#include "util/http/websocket/WebSocketSession.h"

using ad_utility::websocket::QueryHub;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::QueryRegistry;
using ad_utility::websocket::WebSocketSession;
namespace net = boost::asio;
namespace http = boost::beast::http;
using net::ip::tcp;
using namespace boost::asio::experimental::awaitable_operators;
using ::testing::HasSubstr;

// _____________________________________________________________________________

http::request<http::string_body> withPath(std::string_view path) {
  http::request<http::string_body> request{};
  request.target(path);
  return request;
}

// _____________________________________________________________________________

auto toBuffer(std::string_view view) {
  return net::const_buffer{view.data(), view.size()};
}

// Common implementation for the test cases below:
// Run the `clientLogic` on the `executor` via `co_spawn` and `co_await` the
// server logic. Note that the client logic and the server logic are run
// separately, meaning that they can't be cancelled and both have to run to
// completion on their own.
net::awaitable<void> runTest(auto executor, net::awaitable<void> serverLogic,
                             net::awaitable<void> clientLogic) {
  auto fut = std::async(std::launch::async, [&]() {
    net::co_spawn(executor, std::move(clientLogic), net::use_future).get();
  });
  EXPECT_NO_THROW(co_await std::move(serverLogic));
  while (true) {
    auto status = fut.wait_for(std::chrono::milliseconds(0));
    if (status == std::future_status::ready) {
      EXPECT_NO_THROW(fut.get());
      break;
    }
    auto exec = co_await net::this_coro::executor;
    net::steady_timer timer{exec, std::chrono::milliseconds(10)};
    co_await timer.async_wait(net::use_awaitable);
  }
}

// _____________________________________________________________________________
TEST(WebSocketSession, EnsureCorrectPathAcceptAndRejectBehaviour) {
  auto testFunc = WebSocketSession::getErrorResponseIfPathIsInvalid;

  EXPECT_EQ(std::nullopt, testFunc(withPath("/watch/some-id")));
  EXPECT_EQ(std::nullopt, testFunc(withPath("/watch/😇")));

  auto assertNotFoundResponse = [](auto response) {
    ASSERT_TRUE(response.has_value());
    EXPECT_EQ(response->result(), http::status::not_found);
  };

  assertNotFoundResponse(testFunc(withPath("")));
  assertNotFoundResponse(testFunc(withPath("/")));
  assertNotFoundResponse(testFunc(withPath("/watch")));
  assertNotFoundResponse(testFunc(withPath("/watch/")));
  assertNotFoundResponse(testFunc(withPath("/watch//")));
  assertNotFoundResponse(testFunc(withPath("/watch///")));
  assertNotFoundResponse(testFunc(withPath("/watch/trailing-slash/")));
  assertNotFoundResponse(testFunc(withPath("/other-prefix/some-id")));
}

// _____________________________________________________________________________

struct WebSocketTestContainer {
  net::strand<net::io_context::executor_type> strand_;
  std::unique_ptr<QueryHub> queryHub_;
  QueryRegistry registry_;
  tcp::socket server_;
  tcp::socket client_;
  QueryHub& queryHub() { return *queryHub_; }

  net::awaitable<void> serverLogic(auto&& completionToken) {
    boost::beast::tcp_stream stream{std::move(server_)};
    boost::beast::flat_buffer buffer;
    http::request<http::string_body> request;
    co_await http::async_read(stream, buffer, request, completionToken);
    co_await WebSocketSession::handleSession(queryHub(), registry_, request,
                                             std::move(stream.socket()));
  }

  auto serverLogic() { return serverLogic(net::use_awaitable); }
};

net::awaitable<WebSocketTestContainer> createTestContainer(
    net::io_context& ioContext) {
  auto strand = net::make_strand(ioContext);
  WebSocketTestContainer container{
      strand, std::make_unique<QueryHub>(ioContext), QueryRegistry{},
      tcp::socket{strand}, tcp::socket{strand}};
  co_await connect(container.server_, container.client_);
  co_return std::move(container);
}

// _____________________________________________________________________________

// Hack to allow ASSERT_*() macros to work with ASYNC_TEST
#define return co_return

ASYNC_TEST(WebSocketSession, verifySessionEndsOnClientCloseWhileTransmitting) {
  auto c = co_await createTestContainer(ioContext);

  auto distributor = c.queryHub().createOrAcquireDistributorForSending(
      QueryId::idFromString("some-id"));

  distributor->addQueryStatusUpdate("my-event");

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{
        std::move(c.client_)};
    boost::beast::flat_buffer buffer;
    co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());
    co_await webSocket.async_read(buffer, net::use_awaitable);
    ASSERT_TRUE(webSocket.got_text());
    auto data = buffer.data();
    std::string_view view{static_cast<const char*>(data.data()), data.size()};
    EXPECT_EQ(view, "my-event");
    co_await webSocket.async_close(boost::beast::websocket::close_code::normal,
                                   net::use_awaitable);
  };

  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifySessionEndsOnClientClose) {
  auto c = co_await createTestContainer(ioContext);

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{
        std::move(c.client_)};
    boost::beast::flat_buffer buffer;
    co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());
    co_await webSocket.async_close(boost::beast::websocket::close_code::normal,
                                   net::use_awaitable);
  };

  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifySessionEndsWhenServerIsDoneSending) {
  auto c = co_await createTestContainer(ioContext);

  auto distributor = c.queryHub().createOrAcquireDistributorForSending(
      QueryId::idFromString("some-id"));

  distributor->addQueryStatusUpdate("my-event");

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{
        std::move(c.client_)};
    boost::beast::flat_buffer buffer;
    co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());
    co_await webSocket.async_read(buffer, net::use_awaitable);
    ASSERT_TRUE(webSocket.got_text());
    auto data = buffer.data();
    std::string_view view{static_cast<const char*>(data.data()), data.size()};
    EXPECT_EQ(view, "my-event");
    // Expect socket will be closed
    distributor->signalEnd();
    EXPECT_THROW(co_await webSocket.async_read(buffer, net::use_awaitable),
                 boost::system::system_error);
  };

  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifyCancelStringTriggersCancellation) {
  auto c = co_await createTestContainer(ioContext);

  auto queryId = c.registry_.uniqueIdFromString("some-id", "my-query");
  ASSERT_TRUE(queryId.has_value());
  auto cancellationHandle =
      c.registry_.getCancellationHandle(queryId->toQueryId());

  auto controllerActions = [&]() -> net::awaitable<void> {
    constexpr auto clientTimeout = std::chrono::milliseconds{50};
    boost::beast::websocket::stream<tcp::socket> webSocket{
        std::move(c.client_)};
    co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());

    EXPECT_FALSE(cancellationHandle->isCancelled());

    // Wrong keyword should be ignored
    co_await webSocket.async_write(toBuffer("other"), net::use_awaitable);

    // Give server some time to process cancellation request
    net::steady_timer timer{c.strand_, clientTimeout};
    co_await timer.async_wait(net::use_awaitable);

    EXPECT_FALSE(cancellationHandle->isCancelled());

    co_await webSocket.async_write(toBuffer("cancel"), net::use_awaitable);

    // Give server some time to process cancellation request
    timer.expires_after(clientTimeout);
    co_await timer.async_wait(net::use_awaitable);

    EXPECT_TRUE(cancellationHandle->isCancelled());
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        cancellationHandle->throwIfCancelled(), HasSubstr("manually cancelled"),
        ad_utility::CancellationException);

    // Cancel should not close connection immediately
    EXPECT_TRUE(webSocket.is_open());

    {
      // Trigger connection close by creating and destroying message sender
      ad_utility::websocket::MessageSender{std::move(queryId).value(),
                                           c.queryHub()};
    }

    // Expect socket will be closed
    boost::beast::flat_buffer buffer;
    EXPECT_THROW(co_await webSocket.async_read(buffer, net::use_awaitable),
                 boost::system::system_error);
  };
  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifyWrongExecutorConfigThrows) {
  auto runTestWithDummyClient =
      [&](WebSocketTestContainer& c,
          net::awaitable<void> serverLogic) -> net::awaitable<void> {
    auto clientLogic = [&]() -> net::awaitable<void> {
      boost::beast::websocket::stream<tcp::socket> webSocket{
          std::move(c.client_)};
      try {
        co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                           net::use_awaitable);
        co_await webSocket.async_close(
            boost::beast::websocket::close_code::normal, net::use_awaitable);
      } catch (...) {
        // Ignore any exceptions here, they are expected because we test
        // scenarios which interrupt the websocket connection abruptly
      }
    };
    auto serverLogicTestWrapper = [&]() -> net::awaitable<void> {
      EXPECT_THROW(co_await std::move(serverLogic), ad_utility::Exception);
    };
    co_await (serverLogicTestWrapper() && clientLogic());
  };

  {
    auto c = co_await createTestContainer(ioContext);
    co_await runTestWithDummyClient(c, c.serverLogic());
  }
  auto otherStrand = net::make_strand(ioContext);
  {
    auto c = co_await createTestContainer(ioContext);
    co_await runTestWithDummyClient(
        c, net::co_spawn(c.strand_,
                         c.serverLogic(net::bind_executor(otherStrand,
                                                          net::use_awaitable)),
                         net::use_awaitable));
  }
  {
    auto c = co_await createTestContainer(ioContext);
    co_await runTestWithDummyClient(
        c, net::co_spawn(otherStrand, c.serverLogic(), net::use_awaitable));
  }
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifyCancelOnCloseStringTriggersCancellation) {
  auto c = co_await createTestContainer(ioContext);

  auto queryId = c.registry_.uniqueIdFromString("some-id", "my-query");
  ASSERT_TRUE(queryId.has_value());
  auto cancellationHandle =
      c.registry_.getCancellationHandle(queryId->toQueryId());

  auto controllerActions = [&]() -> net::awaitable<void> {
    constexpr auto clientTimeout = std::chrono::milliseconds{50};
    boost::beast::websocket::stream<tcp::socket> webSocket{
        std::move(c.client_)};
    co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());

    EXPECT_FALSE(cancellationHandle->isCancelled());

    // Wrong keyword should be ignored
    co_await webSocket.async_write(toBuffer("other"), net::use_awaitable);

    EXPECT_FALSE(cancellationHandle->isCancelled());

    co_await webSocket.async_write(toBuffer("cancel_on_close"),
                                   net::use_awaitable);

    EXPECT_FALSE(cancellationHandle->isCancelled());

    // Wrong keyword should be ignored
    co_await webSocket.async_write(toBuffer("other2"), net::use_awaitable);

    // Give server some time to process request
    net::steady_timer timer{c.strand_, clientTimeout};
    co_await timer.async_wait(net::use_awaitable);

    EXPECT_FALSE(cancellationHandle->isCancelled());

    co_await webSocket.async_close(boost::beast::websocket::close_code::normal,
                                   net::use_awaitable);

    // Give server some time to process cancellation request
    timer.expires_after(clientTimeout);
    co_await timer.async_wait(net::use_awaitable);

    EXPECT_TRUE(cancellationHandle->isCancelled());
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        cancellationHandle->throwIfCancelled(), HasSubstr("manually cancelled"),
        ad_utility::CancellationException);

    {
      // Trigger connection close by creating and destroying message sender
      ad_utility::websocket::MessageSender{std::move(queryId).value(),
                                           c.queryHub()};
      co_await net::post(net::use_awaitable);
    }

    // Expect socket will be closed
    boost::beast::flat_buffer buffer;
    EXPECT_THROW(co_await webSocket.async_read(buffer, net::use_awaitable),
                 boost::system::system_error);
  };
  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifyWithoutClientActionNoCancelDoesHappen) {
  auto c = co_await createTestContainer(ioContext);

  auto queryId = c.registry_.uniqueIdFromString("some-id", "my-query");
  ASSERT_TRUE(queryId.has_value());
  auto cancellationHandle =
      c.registry_.getCancellationHandle(queryId->toQueryId());

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{
        std::move(c.client_)};
    co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());

    EXPECT_FALSE(cancellationHandle->isCancelled());

    // Wrong keyword should be ignored
    co_await webSocket.async_write(toBuffer("other"), net::use_awaitable);
  };

  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
  EXPECT_FALSE(cancellationHandle->isCancelled());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifyCancelStringDoesNotThrowWithoutHandle) {
  auto c = co_await createTestContainer(ioContext);

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{
        std::move(c.client_)};
    co_await webSocket.async_handshake("localhost", "/watch/does-not-exist",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());

    co_await webSocket.async_write(toBuffer("cancel"), net::use_awaitable);

    co_await webSocket.async_close(boost::beast::websocket::close_code::normal,
                                   net::use_awaitable);
  };

  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession,
           verifyCancelOnCloseStringDoesNotThrowWithoutHandle) {
  auto c = co_await createTestContainer(ioContext);

  auto controllerActions = [&]() -> net::awaitable<void> {
    try {
      boost::beast::websocket::stream<tcp::socket> webSocket{
          std::move(c.client_)};
      co_await webSocket.async_handshake("localhost", "/watch/does-not-exist",
                                         net::use_awaitable);
      ASSERT_TRUE(webSocket.is_open());

      co_await webSocket.async_write(toBuffer("cancel_on_close"),
                                     net::use_awaitable);

      co_await webSocket.async_close(
          boost::beast::websocket::close_code::normal, net::use_awaitable);
    } catch (const std::exception& e) {
      throw;
    }
  };
  co_await runTest(c.strand_, c.serverLogic(), controllerActions());
}
