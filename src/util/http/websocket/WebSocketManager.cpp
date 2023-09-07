//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketManager.h"

#include <ctre/ctre.h>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <optional>

#include "util/http/HttpUtils.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/QueryHub.h"
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

QueryId WebSocketManager::extractFromRequest(
    const http::request<http::string_body>& request) {
  auto queryIdString = extractQueryId(request.target());
  AD_CORRECTNESS_CHECK(!queryIdString.empty());
  return QueryId::idFromString(queryIdString);
}

// _____________________________________________________________________________

net::awaitable<void> WebSocketManager::handleClientCommands() {
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

net::awaitable<void> WebSocketManager::waitForServerEvents() {
  while (ws_.is_open()) {
    auto json = co_await updateFetcher_.waitForEvent();
    if (json == nullptr) {
      if (ws_.is_open()) {
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

net::awaitable<void> WebSocketManager::connectionLifecycle() {
  try {
    ws_.set_option(beast::websocket::stream_base::timeout::suggested(
        beast::role_type::server));

    co_await ws_.async_accept(request_, boost::asio::use_awaitable);

    auto strand = net::make_strand(ws_.get_executor());

    co_await net::dispatch(strand, net::use_awaitable);

    // Experimental operators, see
    // https://www.boost.org/doc/libs/1_81_0/doc/html/boost_asio/overview/composition/cpp20_coroutines.html
    // for more information
    co_await (waitForServerEvents() && handleClientCommands());

  } catch (boost::system::system_error& error) {
    if (error.code() == beast::websocket::error::closed) {
      co_return;
    }
    // There was an unexpected error, rethrow
    throw;
  }

  // In case an unexpected exception is thrown, close the connection gracefully.
  if (ws_.is_open()) {
    co_await ws_.async_close(beast::websocket::close_code::internal_error,
                             net::use_awaitable);
  }
}

// _____________________________________________________________________________

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
