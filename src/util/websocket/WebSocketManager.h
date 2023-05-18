//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

#include <boost/beast/websocket.hpp>

#include "util/http/beast.h"
#include "util/websocket/Common.h"
#include "util/websocket/QueryState.h"
namespace ad_utility::query_state {
class QueryStateManager;
};

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket::common::SharedPayloadAndTimestamp;
using websocket = beast::websocket::stream<tcp::socket>;
using common::SharedPayloadAndTimestamp;

class WebSocketId {
  const void* address_;

 public:
  // Generate unique webSocketIds based on the address of the passed reference.
  // Make sure you're not moving the socket anywhere!
  explicit WebSocketId(const websocket& webSocket) : address_{&webSocket} {}

  constexpr bool operator==(const WebSocketId&) const noexcept = default;

  template <typename H>
  friend H AbslHashValue(H h, const WebSocketId& c) {
    return H::combine(std::move(h), c.address_);
  }
};

using QueryUpdateCallback = std::function<void(SharedPayloadAndTimestamp)>;

class WebSocketManager {
  std::mutex activeWebSocketsMutex{};
  absl::btree_multimap<QueryId, WebSocketId> activeWebSockets{};
  // Note there may be active websockets that are not currently listening
  // because of concurrency.
  std::mutex listeningWebSocketsMutex{};
  absl::flat_hash_map<WebSocketId, QueryUpdateCallback> listeningWebSockets{};

  void registerCallback(const QueryId& queryId, WebSocketId webSocketId,
                        QueryUpdateCallback callback);

  // ONLY CALL THIS IF YOU HOLD the listeningWebSocketsMutex!!!
  void fireCallbackAndRemoveIfPresentNoLock(WebSocketId webSocketId,
                                            SharedPayloadAndTimestamp payload);

  void fireCallbackAndRemoveIfPresent(WebSocketId webSocketId,
                                      SharedPayloadAndTimestamp payload);

  void enableWebSocket(WebSocketId webSocketId, const QueryId& queryId);

  void disableWebSocket(
      WebSocketId webSocketId, const QueryId& queryId,
      ad_utility::query_state::QueryStateManager& queryStateManager);

  template <typename CompletionToken>
  auto waitForEvent(
      const QueryId& queryId, WebSocketId webSocketId,
      common::Timestamp lastUpdate,
      ad_utility::query_state::QueryStateManager& queryStateManager,
      CompletionToken&& token);

  net::awaitable<void> handleClientCommands(
      websocket& ws, WebSocketId webSocketId, const QueryId& queryId,
      ad_utility::query_state::QueryStateManager& queryStateManager);

  net::awaitable<void> waitForServerEvents(
      websocket& ws, WebSocketId webSocketId, const QueryId& queryId,
      ad_utility::query_state::QueryStateManager& queryStateManager);

  net::awaitable<void> connectionLifecycle(
      tcp::socket socket, http::request<http::string_body> request,
      ad_utility::query_state::QueryStateManager& queryStateManager);

 public:
  net::awaitable<void> manageConnection(
      tcp::socket socket, http::request<http::string_body> request,
      ad_utility::query_state::QueryStateManager& queryStateManager);
  // Returns true if there are other active connections that do no currently
  // wait.
  bool fireAllCallbacksForQuery(
      const QueryId& queryId,
      SharedPayloadAndTimestamp runtimeInformationSnapshot);
};

std::optional<http::response<http::string_body>> checkPathIsValid(
    const http::request<http::string_body>& request);
};  // namespace ad_utility::websocket
