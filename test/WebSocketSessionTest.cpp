//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/beast/core/flat_buffer.hpp>

#include "util/AsyncTestHelpers.h"
#include "util/BeastTestHelpers.h"
#include "util/http/websocket/WebSocketSession.h"

using ad_utility::websocket::QueryHub;
using ad_utility::websocket::QueryId;
using ad_utility::websocket::WebSocketSession;
namespace net = boost::asio;
namespace http = boost::beast::http;
using net::ip::tcp;
using namespace boost::asio::experimental::awaitable_operators;

http::request<http::string_body> withPath(std::string_view path) {
  http::request<http::string_body> request{};
  request.target(path);
  return request;
}

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

// Hack to allow ASSERT_*() macros to work with ASYNC_TEST
#define return co_return

ASYNC_TEST(WebSocketSession, verifySessionEndsOnClientCloseWhileTransmitting) {
  QueryHub queryHub{ioContext};

  auto distributor = co_await queryHub.createOrAcquireDistributorForSending(
      QueryId::idFromString("some-id"));

  co_await distributor->addQueryStatusUpdate("my-event");

  tcp::socket server{ioContext};
  tcp::socket client{ioContext};
  co_await connect(server, client);

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{std::move(client)};
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

  auto serverLogic = [&]() -> net::awaitable<void> {
    boost::beast::tcp_stream stream{std::move(server)};
    boost::beast::flat_buffer buffer;
    http::request<http::string_body> request;
    co_await http::async_read(stream, buffer, request, net::use_awaitable);
    co_await WebSocketSession::handleSession(queryHub, request,
                                             std::move(stream.socket()));
  };

  co_await (serverLogic() && controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifySessionEndsOnClientClose) {
  QueryHub queryHub{ioContext};

  tcp::socket server{ioContext};
  tcp::socket client{ioContext};
  co_await connect(server, client);

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{std::move(client)};
    boost::beast::flat_buffer buffer;
    co_await webSocket.async_handshake("localhost", "/watch/some-id",
                                       net::use_awaitable);
    ASSERT_TRUE(webSocket.is_open());
    co_await webSocket.async_close(boost::beast::websocket::close_code::normal,
                                   net::use_awaitable);
  };

  auto serverLogic = [&]() -> net::awaitable<void> {
    boost::beast::tcp_stream stream{std::move(server)};
    boost::beast::flat_buffer buffer;
    http::request<http::string_body> request;
    co_await http::async_read(stream, buffer, request, net::use_awaitable);
    co_await WebSocketSession::handleSession(queryHub, request,
                                             std::move(stream.socket()));
  };

  co_await (serverLogic() && controllerActions());
}

// _____________________________________________________________________________

ASYNC_TEST(WebSocketSession, verifySessionEndsWhenServerIsDoneSending) {
  QueryHub queryHub{ioContext};

  auto distributor = co_await queryHub.createOrAcquireDistributorForSending(
      QueryId::idFromString("some-id"));

  co_await distributor->addQueryStatusUpdate("my-event");

  tcp::socket server{ioContext};
  tcp::socket client{ioContext};
  co_await connect(server, client);

  auto controllerActions = [&]() -> net::awaitable<void> {
    boost::beast::websocket::stream<tcp::socket> webSocket{std::move(client)};
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
    co_await distributor->signalEnd();
    EXPECT_THROW(co_await webSocket.async_read(buffer, net::use_awaitable),
                 boost::system::system_error);
  };

  auto serverLogic = [&]() -> net::awaitable<void> {
    boost::beast::tcp_stream stream{std::move(server)};
    boost::beast::flat_buffer buffer;
    http::request<http::string_body> request;
    co_await http::async_read(stream, buffer, request, net::use_awaitable);
    co_await WebSocketSession::handleSession(queryHub, request,
                                             std::move(stream.socket()));
  };

  co_await (serverLogic() && controllerActions());
}
