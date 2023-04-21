#include "WebSocketManager.h"

#include <boost/asio/experimental/awaitable_operators.hpp>

#include <absl/container/flat_hash_map.h>
#include <absl/container/btree_map.h>
#include <absl/cleanup/cleanup.h>

#include <iostream>
#include <optional>
#include <mutex>

#include "../MultiMap.h"
#include "./Common.h"
#include "./QueryState.h"

namespace ad_utility::websocket {
using namespace boost::asio::experimental::awaitable_operators;
using websocket = beast::websocket::stream<tcp::socket>;
using common::RuntimeInformationSnapshot;

typedef uint32_t WebSocketId;
// We need a copy here due to the async nature of the lifetime of those objects
// TODO evaluate if using a shared const ptr is more efficient here
typedef std::function<void(std::optional<RuntimeInformationSnapshot>)> QueryUpdateCallback;


static std::mutex activeWebSocketsMutex{};
static absl::btree_multimap<QueryId, WebSocketId> activeWebSockets{};
// Note there may be active websockets that are not currently listening
// because of concurrency.
static std::mutex listeningWebSocketsMutex{};
static absl::flat_hash_map<WebSocketId, QueryUpdateCallback> listeningWebSockets{};

static std::atomic<WebSocketId> webSocketIdCounter{0};

void registerCallback(WebSocketId webSocketId, QueryUpdateCallback callback) {
  std::lock_guard lock1{activeWebSocketsMutex};
  AD_CORRECTNESS_CHECK(activeWebSockets.count(webSocketId) != 0);
  std::lock_guard lock2{listeningWebSocketsMutex};
  AD_CORRECTNESS_CHECK(listeningWebSockets.count(webSocketId) == 0);
  listeningWebSockets[webSocketId] = std::move(callback);
}

// ONLY CALL THIS IF YOU HOLD the listeningWebSocketsMutex!!!
void fireCallbackAndRemoveIfPresentNoLock(WebSocketId webSocketId, std::optional<RuntimeInformationSnapshot> runtimeInformation) {
  if (listeningWebSockets.count(webSocketId) != 0) {
    listeningWebSockets.at(webSocketId)(std::move(runtimeInformation));
    listeningWebSockets.erase(webSocketId);
  }
}

void fireCallbackAndRemoveIfPresent(WebSocketId webSocketId, std::optional<RuntimeInformationSnapshot> runtimeInformation) {
    std::lock_guard lock{listeningWebSocketsMutex};
    fireCallbackAndRemoveIfPresentNoLock(webSocketId, std::move(runtimeInformation));
}

bool fireAllCallbacksForQuery(QueryId queryId, RuntimeInformationSnapshot runtimeInformationSnapshot) {
  std::lock_guard lock1{activeWebSocketsMutex};
  std::lock_guard lock2{listeningWebSocketsMutex};
  auto range = activeWebSockets.equal_range(queryId);
  size_t counter = 0;
  for (auto it = range.first; it != range.second; it++) {
    fireCallbackAndRemoveIfPresentNoLock(it->second, runtimeInformationSnapshot);
    counter++;
  }
  return counter < activeWebSockets.count(queryId);
}

void enableWebSocket(WebSocketId webSocketId, QueryId queryId) {
  std::lock_guard lock{activeWebSocketsMutex};
  activeWebSockets.insert({webSocketId, queryId});
}

void disableWebSocket(WebSocketId webSocketId, QueryId queryId) {
  bool noListenersLeft;
  {
    std::lock_guard lock{activeWebSocketsMutex};
    removeKeyValuePair(activeWebSockets, queryId, webSocketId);
    fireCallbackAndRemoveIfPresent(webSocketId, std::nullopt);
    noListenersLeft = activeWebSockets.count(queryId) == 0;
  }
  if (noListenersLeft) {
    query_state::clearQueryInfo(queryId);
  }
}

// Based on https://stackoverflow.com/a/69285751
template<typename CompletionToken>
auto waitForEvent(QueryId queryId, common::TimeStamp lastUpdate, CompletionToken&& token) {
  auto initiate = [lastUpdate]<typename Handler>(Handler&& self, QueryId queryId) mutable {
    auto callback = [self = std::make_shared<Handler>(std::forward<Handler>(self)), queryId](std::optional<RuntimeInformationSnapshot> snapshot) {
      (*self)(snapshot);
    };
    auto potentialUpdate = query_state::getIfUpdatedSince(queryId, lastUpdate);
    if (potentialUpdate.has_value()) {
      callback(*potentialUpdate);
    } else {
      registerCallback(queryId, std::move(callback));
    }
  };
  // TODO match this to QueryUpdateCallback
  // TODO don't pass token directly and instead wrap it inside "bind_cancellation_slot"
  //  to remove the callback in case it gets cancelled
  return net::async_initiate<CompletionToken, void(std::optional<RuntimeInformationSnapshot>)>(
      initiate, token, queryId
  );
}

net::awaitable<void> handleClientCommands(websocket& ws, WebSocketId webSocketId, QueryId queryId) {
  absl::Cleanup cleanup{[webSocketId, queryId]() {
    // TODO is this properly cleaned up when this coroutine is cancelled?
    disableWebSocket(webSocketId, queryId);
  }};
  beast::flat_buffer buffer;

  while (ws.is_open()) {
    co_await ws.async_read(buffer, net::use_awaitable);
    // TODO replace with cancellation process
    ws.text(ws.got_text());
    co_await ws.async_write(buffer.data(), net::use_awaitable);
    buffer.clear();
  }
}

net::awaitable<void> waitForServerEvents(websocket& ws, WebSocketId webSocketId, QueryId queryId) {
  common::TimeStamp lastUpdate = std::chrono::steady_clock::now();
  while (ws.is_open()) {
    auto update = co_await waitForEvent(queryId, lastUpdate, net::use_awaitable);
    if (!update.has_value()) {
      if (ws.is_open()) {
        co_await ws.async_close(beast::websocket::close_code::normal, net::use_awaitable);
      }
      break;
    }
    ws.text(true);
    std::string json = update->runtimeInformation.toString();
    lastUpdate = update->updateMoment;
    co_await ws.async_write(net::buffer(json.data(), json.length()), net::use_awaitable);
  }
  // TODO remove
  std::cout << "End of server events" << std::endl;
}

net::awaitable<void> connectionLifecycle(tcp::socket socket, http::request<http::string_body> request) {
  // TODO extract queryId from request and don't accept connection if invalid
  QueryId queryId = 0;
  websocket ws{std::move(socket)};

  try {
    // TODO set suggested settings as described here?
    // https://www.boost.org/doc/libs/master/libs/beast/example/websocket/server/awaitable/websocket_server_awaitable.cpp
    co_await ws.async_accept(request, boost::asio::use_awaitable);

    // Generate unique webSocketIds until it overflows and hopefully the old
    // ids are gone by then.
    WebSocketId webSocketId = webSocketIdCounter++;
    enableWebSocket(webSocketId, queryId);
    auto strand = net::make_strand(ws.get_executor());

    // experimental operators
    co_await (net::co_spawn(strand, waitForServerEvents(ws, webSocketId, queryId), net::use_awaitable)
              && net::co_spawn(strand, handleClientCommands(ws, webSocketId, queryId), net::use_awaitable));

    // TODO can this happen?
    if (ws.is_open()) {
      co_await ws.async_close(beast::websocket::close_code::normal, net::use_awaitable);
    }

  } catch (boost::system::system_error& error) {
    if (error.code() != beast::websocket::error::closed) {
      // There was an unexpected error, rethrow
      throw;
    }
    // TODO check if there are errors where the socket needs to be closed explicitly
  }
}

net::awaitable<void> manageConnection(tcp::socket socket, http::request<http::string_body> request) {
  auto executor = socket.get_executor();
  return net::co_spawn(executor, connectionLifecycle(std::move(socket), std::move(request)), net::use_awaitable);
}
}
