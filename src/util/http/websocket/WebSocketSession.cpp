//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketSession.h"

#include <ctre/ctre.h>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <optional>

#include "util/http/HttpUtils.h"
#include "util/http/websocket/QueryHub.h"
#include "util/http/websocket/QueryId.h"
#include "util/http/websocket/UpdateFetcher.h"

namespace ad_utility::websocket {
using namespace boost::asio::experimental::awaitable_operators;

// Extracts the query id from a URL path. Returns the empty string when invalid.
std::string extractQueryId(std::string_view path) {
  auto match = ctre::match<"/watch/([^/?]+)">(path);
  if (match) {
    return match.get<1>().to_string();
  }
  return "";
}

// _____________________________________________________________________________

net::awaitable<void> WebSocketSession::handleClientCommands() {
  beast::flat_buffer buffer;

  while (ws_.is_open()) {
    co_await ws_.async_read(buffer, net::use_awaitable);
    // TODO<RobinTF> replace with cancellation process
    ws_.text(ws_.got_text());
    co_await ws_.async_write(buffer.data(), net::use_awaitable);
    buffer.clear();
  }
}

// _____________________________________________________________________________

net::awaitable<void> WebSocketSession::waitForServerEvents() {
  while (ws_.is_open()) {
    auto json = co_await updateFetcher_.waitForEvent();
    if (json == nullptr) {
      if (ws_.is_open()) {
        // This will cause the loop within `handleClientCommands` to terminate
        // by provoking an exception
        co_await ws_.async_close(beast::websocket::close_code::normal,
                                 net::use_awaitable);
      }
      break;
    }
    ws_.text(true);
    co_await ws_.async_write(net::buffer(json->data(), json->length()),
                             net::use_awaitable);
  }
}

// _____________________________________________________________________________

net::awaitable<void> WebSocketSession::acceptAndWait(
    const http::request<http::string_body>& request) {
  try {
    ws_.set_option(beast::websocket::stream_base::timeout::suggested(
        beast::role_type::server));

    co_await ws_.async_accept(request, boost::asio::use_awaitable);

    auto strand = net::make_strand(ws_.get_executor());

    co_await net::dispatch(strand, net::use_awaitable);

    // Experimental operators, see
    // https://www.boost.org/doc/libs/1_81_0/doc/html/boost_asio/overview/composition/cpp20_coroutines.html
    // for more information
    co_await (waitForServerEvents() && handleClientCommands());

  } catch (boost::system::system_error& error) {
    // Gracefully end if socket was closed
    if (error.code() == beast::websocket::error::closed ||
        error.code() == net::error::operation_aborted) {
      co_return;
    }
    // There was an unexpected error, rethrow
    throw;
  }
}

// _____________________________________________________________________________

net::awaitable<void> WebSocketSession::handleSession(
    QueryHub& queryHub, const http::request<http::string_body>& request,
    tcp::socket socket) {
  auto queryIdString = extractQueryId(request.target());
  AD_CORRECTNESS_CHECK(!queryIdString.empty());
  UpdateFetcher fetcher{queryHub, QueryId::idFromString(queryIdString)};
  WebSocketSession webSocketSession{std::move(fetcher), std::move(socket)};
  co_await webSocketSession.acceptAndWait(request);
}
// _____________________________________________________________________________

// TODO<C++23> use std::expected<void, ErrorResponse>
std::optional<http::response<http::string_body>>
WebSocketSession::getErrorResponseIfPathIsInvalid(
    const http::request<http::string_body>& request) {
  auto path = request.target();

  if (extractQueryId(path).empty()) {
    return ad_utility::httpUtils::createNotFoundResponse(
        "No WebSocket on this path", request);
  }
  return std::nullopt;
}
}  // namespace ad_utility::websocket
