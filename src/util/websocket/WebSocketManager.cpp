//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketManager.h"

#include <absl/cleanup/cleanup.h>
#include <ctre/ctre.h>

#include <boost/asio/experimental/awaitable_operators.hpp>
#include <mutex>
#include <optional>

#include "util/MultiMap.h"
#include "util/http/HttpUtils.h"
#include "util/websocket/Common.h"

namespace ad_utility::websocket {
using namespace boost::asio::experimental::awaitable_operators;

void WebSocketManager::registerCallback(const QueryId& queryId,
                                        WebSocketId webSocketId,
                                        QueryUpdateCallback callback) {
  std::lock_guard lock1{activeWebSocketsMutex};
  // make sure websocket has not been shut down right before acquiring the lock
  if (containsKeyValuePair(activeWebSockets, queryId, webSocketId)) {
    std::lock_guard lock2{listeningWebSocketsMutex};
    // Ensure last callback has been fired, otherwise co_await will wait
    // indefinitely
    AD_CORRECTNESS_CHECK(!listeningWebSockets.contains(webSocketId));
    listeningWebSockets[webSocketId] = std::move(callback);
  }
}

// ONLY CALL THIS IF YOU HOLD the listeningWebSocketsMutex!!!
void WebSocketManager::fireCallbackAndRemoveIfPresentNoLock(
    WebSocketId webSocketId, SharedPayloadAndTimestamp payload) {
  if (listeningWebSockets.contains(webSocketId)) {
    listeningWebSockets.at(webSocketId)(std::move(payload));
    listeningWebSockets.erase(webSocketId);
  }
}

void WebSocketManager::fireCallbackAndRemoveIfPresent(
    WebSocketId webSocketId, SharedPayloadAndTimestamp payload) {
  std::lock_guard lock{listeningWebSocketsMutex};
  fireCallbackAndRemoveIfPresentNoLock(webSocketId, std::move(payload));
}

bool WebSocketManager::fireAllCallbacksForQuery(
    const QueryId& queryId, SharedPayloadAndTimestamp payload) {
  std::lock_guard lock1{activeWebSocketsMutex};
  std::lock_guard lock2{listeningWebSocketsMutex};
  auto range = activeWebSockets.equal_range(queryId);
  size_t counter = 0;
  for (auto it = range.first; it != range.second; it++) {
    fireCallbackAndRemoveIfPresentNoLock(it->second, payload);
    counter++;
  }
  return counter < activeWebSockets.count(queryId);
}

void WebSocketManager::enableWebSocket(WebSocketId webSocketId,
                                       const QueryId& queryId) {
  std::lock_guard lock{activeWebSocketsMutex};
  activeWebSockets.insert({queryId, webSocketId});
}

void WebSocketManager::disableWebSocket(
    WebSocketId webSocketId, const QueryId& queryId,
    ad_utility::query_state::QueryStateManager& queryStateManager) {
  bool noListenersLeft;
  {
    std::lock_guard lock{activeWebSocketsMutex};
    removeKeyValuePair(activeWebSockets, queryId, webSocketId);
    fireCallbackAndRemoveIfPresent(webSocketId, nullptr);
    noListenersLeft = !activeWebSockets.contains(queryId);
  }
  if (noListenersLeft) {
    queryStateManager.clearQueryInfo(queryId);
  }
}

// Based on https://stackoverflow.com/a/69285751
template <typename CompletionToken>
auto WebSocketManager::waitForEvent(
    const QueryId& queryId, WebSocketId webSocketId,
    common::Timestamp lastUpdate,
    ad_utility::query_state::QueryStateManager& queryStateManager,
    CompletionToken&& token) {
  auto initiate = [this, lastUpdate, &queryStateManager]<typename Handler>(
                      Handler&& self, const QueryId& queryId,
                      WebSocketId webSocketId) mutable {
    auto callback = [self = std::make_shared<Handler>(std::forward<Handler>(
                         self))](SharedPayloadAndTimestamp snapshot) {
      (*self)(std::move(snapshot));
    };
    auto potentialUpdate =
        queryStateManager.getIfUpdatedSince(queryId, lastUpdate);
    if (potentialUpdate != nullptr) {
      callback(std::move(potentialUpdate));
    } else {
      registerCallback(queryId, webSocketId, std::move(callback));
    }
  };
  return net::async_initiate<CompletionToken, void(SharedPayloadAndTimestamp)>(
      initiate, token, queryId, webSocketId);
}

net::awaitable<void> WebSocketManager::handleClientCommands(
    websocket& ws, WebSocketId webSocketId, const QueryId& queryId,
    ad_utility::query_state::QueryStateManager& queryStateManager) {
  absl::Cleanup cleanup{[this, webSocketId, queryId, &queryStateManager]() {
    disableWebSocket(webSocketId, queryId, queryStateManager);
  }};
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
    websocket& ws, WebSocketId webSocketId, const QueryId& queryId,
    ad_utility::query_state::QueryStateManager& queryStateManager) {
  common::Timestamp lastUpdate = std::chrono::steady_clock::now();
  while (ws.is_open()) {
    auto update = co_await waitForEvent(queryId, webSocketId, lastUpdate,
                                        queryStateManager, net::use_awaitable);
    if (update == nullptr) {
      if (ws.is_open()) {
        co_await ws.async_close(beast::websocket::close_code::normal,
                                net::use_awaitable);
      }
      break;
    }
    ws.text(true);
    const auto& json = update->payload;
    lastUpdate = update->updateMoment;
    co_await ws.async_write(net::buffer(json.data(), json.length()),
                            net::use_awaitable);
  }
}

net::awaitable<void> WebSocketManager::connectionLifecycle(
    tcp::socket socket, http::request<http::string_body> request,
    ad_utility::query_state::QueryStateManager& queryStateManager) {
  auto match = ctre::match<"/watch/([^/?]+)">(request.target());
  AD_CORRECTNESS_CHECK(match);
  QueryId queryId = QueryId::idFromString(match.get<1>().to_string());
  websocket ws{std::move(socket)};

  try {
    ws.set_option(beast::websocket::stream_base::timeout::suggested(
        beast::role_type::server));

    co_await ws.async_accept(request, boost::asio::use_awaitable);

    WebSocketId webSocketId{ws};
    enableWebSocket(webSocketId, queryId);
    auto strand = net::make_strand(ws.get_executor());

    // experimental operators
    co_await (net::co_spawn(strand,
                            waitForServerEvents(ws, webSocketId, queryId,
                                                queryStateManager),
                            net::use_awaitable) &&
              net::co_spawn(strand,
                            handleClientCommands(ws, webSocketId, queryId,
                                                 queryStateManager),
                            net::use_awaitable));

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
    tcp::socket socket, http::request<http::string_body> request,
    ad_utility::query_state::QueryStateManager& queryStateManager) {
  auto executor = socket.get_executor();
  return net::co_spawn(
      executor,
      connectionLifecycle(std::move(socket), std::move(request),
                          queryStateManager),
      net::use_awaitable);
}

std::optional<http::response<http::string_body>> checkPathIsValid(
    const http::request<http::string_body>& request) {
  auto path = request.target();

  if (ctre::match<"/watch/[^/?]+">(path)) {
    return std::nullopt;
  }
  return ad_utility::httpUtils::createNotFoundResponse(
      "No WebSocket on this path", request);
}
}  // namespace ad_utility::websocket
