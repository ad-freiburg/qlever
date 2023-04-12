#include "WebSocketManager.h"

#include <boost/asio/experimental/awaitable_operators.hpp>

#include <absl/container/flat_hash_map.h>
#include <absl/container/btree_map.h>
#include <absl/cleanup/cleanup.h>

#include <iostream>

namespace ad_utility::websocket {
using namespace boost::asio::experimental::awaitable_operators;
using websocket = beast::websocket::stream<tcp::socket>;

typedef uint32_t QueryId;
typedef uint32_t WebSocketId;
typedef std::function<void(bool)> QueryUpdateCallback;

template<typename Container>
void removeKeyValuePair(Container& container, typename Container::key_type key, typename Container::mapped_type value) {
  auto iterPair = container.equal_range(key);

  for (auto it = iterPair.first; it != iterPair.second; ++it) {
    if (it->second == value) {
      container.erase(it);
      break;
    }
  }
}

static absl::btree_multimap<QueryId, WebSocketId> activeWebSockets{};
// Note there may be active websockets that are not currently listening
// because of concurrency.
static absl::flat_hash_map<WebSocketId, QueryUpdateCallback> listeningWebSockets{};

static std::atomic<WebSocketId> webSocketIdCounter{0};

void registerCallback(WebSocketId webSocketId, QueryUpdateCallback callback) {
  // TODO lock here for activeWebSockets
  // TODO assert websocket is in activeWebSockets
  // TODO lock here for listeningWebSockets
  // TODO assert no callback active
  listeningWebSockets[webSocketId] = std::move(callback);
}

void fireCallbackAndRemoveIfPresent(WebSocketId webSocketId, bool abort) {
  // TODO lock here for listeningWebSockets
  if (listeningWebSockets.count(webSocketId) != 0) {
    listeningWebSockets.at(webSocketId)(abort);
    listeningWebSockets.erase(webSocketId);
  }
}

void enableWebSocket(WebSocketId webSocketId, QueryId queryId) {
  // TODO lock here for activeWebSockets
  activeWebSockets.insert({webSocketId, queryId});
}

void disableWebSocket(WebSocketId webSocketId, QueryId queryId) {
  // TODO lock here for activeWebSockets
  removeKeyValuePair(activeWebSockets, queryId, webSocketId);
  fireCallbackAndRemoveIfPresent(webSocketId, true);
}

// Based on https://stackoverflow.com/a/69285751
template<typename CompletionToken>
auto waitForEvent(QueryId queryId, CompletionToken&& token) {
  auto initiate = []<typename Handler>(Handler&& self, QueryId queryId) mutable {
    // TODO check if update ready, otherwise register callback
    registerCallback(queryId, [self = std::make_shared<Handler>(std::forward<Handler>(self))](bool abort) {
      (*self)(abort);
    });
  };
  // TODO match this to QueryUpdateCallback
  // TODO don't pass token directly and instead wrap it inside "bind_cancellation_slot"
  //  to remove the callback in case it gets cancelled
  return net::async_initiate<CompletionToken, void(bool)>(
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
    ws.text(ws.got_text());
    co_await ws.async_write(buffer.data(), net::use_awaitable);
    buffer.clear();
  }
}

net::awaitable<void> waitForServerEvents(websocket& ws, WebSocketId webSocketId, QueryId queryId) {
  std::string testData = "Abc";

  while (ws.is_open()) {
    bool abort = co_await waitForEvent(queryId, net::use_awaitable);
    if (abort) {
      break;
    }
    ws.text(true);
    co_await ws.async_write(net::buffer(testData.data(), testData.length()), net::use_awaitable);
  }
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


    co_await ws.async_close(beast::websocket::close_code::normal, net::use_awaitable);

  } catch (boost::system::system_error& error) {
    if (error.code() != beast::websocket::error::closed) {
      // There was an unexpected error, rethrow
      // TODO does this move make sense?
      throw std::move(error);
    }
    // TODO check if there are errors where the socket needs to be closed explicitly
  }
}

net::awaitable<void> manageConnection(tcp::socket socket, http::request<http::string_body> request) {
  auto executor = socket.get_executor();
  return net::co_spawn(executor, connectionLifecycle(std::move(socket), std::move(request)), net::use_awaitable);
}
};

