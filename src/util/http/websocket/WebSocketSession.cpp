//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketSession.h"

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <ctre-unicode.hpp>
#include <optional>

#include "util/Algorithm.h"
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

bool WebSocketSession::tryToCancelQuery() const {
  if (auto cancellationHandle =
          queryRegistry_.getCancellationHandle(queryId_)) {
    cancellationHandle->cancel(CancellationState::MANUAL);

    return true;
  }
  return false;
}

// _____________________________________________________________________________

net::awaitable<void> WebSocketSession::handleClientCommands() {
  beast::flat_buffer buffer;

  while (ws_.is_open()) {
    co_await ws_.async_read(buffer, net::use_awaitable);
    if (ws_.got_text()) {
      auto data = buffer.data();
      std::string_view dataAsString{static_cast<char*>(data.data()),
                                    data.size()};
      if (dataAsString == "cancel_on_close") {
        cancelOnClose_ = true;
      } else if (dataAsString == "cancel" && tryToCancelQuery()) {
        break;
      }
    }
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

    co_await ws_.async_accept(request, net::use_awaitable);

    // Experimental operators, see
    // https://www.boost.org/doc/libs/1_81_0/doc/html/boost_asio/overview/composition/cpp20_coroutines.html
    // for more information
    co_await (waitForServerEvents() && handleClientCommands());

  } catch (boost::system::system_error& error) {
    if (cancelOnClose_) {
      tryToCancelQuery();
    }
    static const std::array<boost::system::error_code, 4> allowedCodes{
        beast::websocket::error::closed, net::error::operation_aborted,
        net::error::eof, net::error::connection_reset};
    // Gracefully end if socket was closed
    if (ad_utility::contains(allowedCodes, error.code())) {
      co_return;
    }
    // There was an unexpected error, rethrow
    throw;
  }
}

// _____________________________________________________________________________

net::awaitable<void> WebSocketSession::handleSession(
    QueryHub& queryHub, const QueryRegistry& queryRegistry,
    const http::request<http::string_body>& request, tcp::socket socket) {
  // Make sure access to new websocket is on a strand and therefore thread safe
  auto executor = co_await net::this_coro::executor;
  auto strandExecutor =
      executor.target<net::strand<net::io_context::executor_type>>();
  AD_CONTRACT_CHECK(strandExecutor);
  AD_CONTRACT_CHECK(strandExecutor->running_in_this_thread());
  AD_CONTRACT_CHECK(socket.get_executor() == *strandExecutor);

  auto queryIdString = extractQueryId(request.target());
  AD_CORRECTNESS_CHECK(!queryIdString.empty());
  auto queryId = QueryId::idFromString(queryIdString);
  UpdateFetcher fetcher{queryHub, queryId};
  WebSocketSession webSocketSession{std::move(fetcher), std::move(socket),
                                    queryRegistry, std::move(queryId)};
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
