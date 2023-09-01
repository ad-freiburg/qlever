//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketManager.h"

#include <ctre/ctre.h>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <optional>

#include "util/http/HttpUtils.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/UpdateFetcher.h"
#include "util/http/websocket/WebSocketTracker.h"

namespace ad_utility::websocket {
using namespace boost::asio::experimental::awaitable_operators;

net::awaitable<void> WebSocketManager::handleClientCommands(websocket& ws) {
  beast::flat_buffer buffer;

  while (ws.is_open()) {
    co_await ws.async_read(buffer, net::use_awaitable);
    // TODO<RobinTF> replace with cancellation process
    ws.text(ws.got_text());
    co_await ws.async_write(buffer.data(), net::use_awaitable);
    buffer.clear();
  }
}

net::awaitable<void> WebSocketManager::waitForServerEvents(
    websocket& ws, const QueryId& queryId, StrandType& socketStrand) {
  UpdateFetcher updateFetcher{queryId, webSocketTracker_, socketStrand};
  while (ws.is_open()) {
    auto json = co_await updateFetcher.waitForEvent();
    if (json == nullptr) {
      if (ws.is_open()) {
        co_await ws.async_close(beast::websocket::close_code::normal,
                                net::use_awaitable);
      }
      break;
    }
    ws.text(true);
    co_await ws.async_write(net::buffer(json->data(), json->length()),
                            net::use_awaitable);
  }
}

// Extracts the query id from a URL path. Returns the empty string when invalid.
std::string extractQueryId(std::string_view path) {
  auto match = ctre::match<"/watch/([^/?]+)">(path);
  if (match) {
    return match.get<1>().to_string();
  }
  return "";
}

net::awaitable<void> WebSocketManager::connectionLifecycle(
    tcp::socket socket, http::request<http::string_body> request) {
  auto queryIdString = extractQueryId(request.target());
  AD_CORRECTNESS_CHECK(!queryIdString.empty());
  QueryId queryId = QueryId::idFromString(queryIdString);
  websocket ws{std::move(socket)};

  try {
    ws.set_option(beast::websocket::stream_base::timeout::suggested(
        beast::role_type::server));

    co_await ws.async_accept(request, boost::asio::use_awaitable);

    auto strand = net::make_strand(ws.get_executor());

    // Experimental operators, see
    // https://www.boost.org/doc/libs/1_81_0/doc/html/boost_asio/overview/composition/cpp20_coroutines.html
    // for more information
    co_await (
        net::co_spawn(strand, waitForServerEvents(ws, queryId, strand),
                      net::use_awaitable) &&
        net::co_spawn(strand, handleClientCommands(ws), net::use_awaitable));

  } catch (boost::system::system_error& error) {
    if (error.code() == beast::websocket::error::closed) {
      co_return;
    }
    // There was an unexpected error, rethrow
    throw;
  }

  // In case an unexpected exception is thrown, close the connection gracefully.
  if (ws.is_open()) {
    co_await ws.async_close(beast::websocket::close_code::internal_error,
                            net::use_awaitable);
  }
}

net::awaitable<void> WebSocketManager::manageConnection(
    tcp::socket socket, http::request<http::string_body> request) {
  auto executor = socket.get_executor();
  return net::co_spawn(
      executor, connectionLifecycle(std::move(socket), std::move(request)),
      net::use_awaitable);
}

WebSocketTracker& WebSocketManager::getWebSocketTracker() {
  return webSocketTracker_;
}

// TODO<C++23> use std::expected<void, ErrorResponse>
std::optional<http::response<http::string_body>>
WebSocketManager::getErrorResponseIfPathIsInvalid(
    const http::request<http::string_body>& request) {
  auto path = request.target();

  if (extractQueryId(path).empty()) {
    return ad_utility::httpUtils::createNotFoundResponse(
        "No WebSocket on this path", request);
  }
  return std::nullopt;
}
}  // namespace ad_utility::websocket
