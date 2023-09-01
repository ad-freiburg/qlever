//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once
#include <absl/container/flat_hash_map.h>

#include <boost/beast/websocket.hpp>
#include <vector>

#include "util/http/beast.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/EphemeralWaitingList.h"
#include "util/http/websocket/QueryToSocketDistributor.h"
#include "util/http/websocket/WebSocketTracker.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket = beast::websocket::stream<tcp::socket>;

/// Central class to keep track of all currently connected websockets
/// to provide them with status updates.
class WebSocketManager {
  net::io_context& ioContext_;
  net::strand<net::io_context::executor_type> socketStrand_;
  WebSocketTracker webSocketTracker_;

  /// Loop that waits for input from the client
  net::awaitable<void> handleClientCommands(websocket&);

  /// Loop that waits for an update of the given query to occur and send it
  /// to the client once it happens
  net::awaitable<void> waitForServerEvents(websocket&, const QueryId&);

  /// Accepts the websocket connection and "blocks" for the lifetime of the
  /// websocket connection
  net::awaitable<void> connectionLifecycle(tcp::socket,
                                           http::request<http::string_body>);

 public:
  /// Constructs an instance of this class using the provided io context
  /// reference to create a thread-safe strand for all cross-thread operations.
  /// Any instance needs to be destructed before the io context.
  explicit WebSocketManager(net::io_context& ioContext)
      : ioContext_{ioContext},
        socketStrand_(net::make_strand(ioContext)),
        webSocketTracker_{ioContext_, socketStrand_} {}

  /// The main interface for this class. The HTTP server is supposed to check
  /// if an HTTP request is a websocket upgrade request and delegate further
  /// handling to this method if it is the case.
  net::awaitable<void> manageConnection(tcp::socket,
                                        http::request<http::string_body>);

  WebSocketTracker& getWebSocketTracker();
};

/// Helper function to provide a proper error response if the provided URL
/// path is not accepted by the server.
std::optional<http::response<http::string_body>> getErrorResponseIfPathIsValid(
    const http::request<http::string_body>&);
};  // namespace ad_utility::websocket
