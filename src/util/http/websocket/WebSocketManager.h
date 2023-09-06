//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <boost/beast/websocket.hpp>

#include "util/http/beast.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/QueryToSocketDistributor.h"
#include "util/http/websocket/WebSocketTracker.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket = beast::websocket::stream<tcp::socket>;
using StrandType = net::strand<net::any_io_executor>;

/// Central class to keep track of all currently connected websockets
/// to provide them with status updates.
class WebSocketManager {
  WebSocketTracker webSocketTracker_;

  /// Loop that waits for input from the client
  net::awaitable<void> handleClientCommands(websocket&);

  /// Loop that waits for an update of the given query to occur and send it
  /// to the client once it happens
  net::awaitable<void> waitForServerEvents(websocket&, const QueryId&,
                                           StrandType&);

 public:
  /// Constructs an instance of this class using the provided io context
  /// reference to pass it to the wrapped `WebSocketTracker`
  explicit WebSocketManager(net::io_context& ioContext)
      : webSocketTracker_{ioContext} {}

  /// The main interface for this class. The HTTP server is supposed to check
  /// if an HTTP request is a websocket upgrade request and delegate further
  /// handling to this method if it is the case. It then accepts the websocket
  /// connection and "blocks" for the lifetime of it.
  net::awaitable<void> connectionLifecycle(tcp::socket,
                                           http::request<http::string_body>);

  /// Get a reference to the wrapped `WebSocketTracker`.
  WebSocketTracker& getWebSocketTracker();

  /// Helper function to provide a proper error response if the provided URL
  /// path is not accepted by the server.
  static std::optional<http::response<http::string_body>>
  getErrorResponseIfPathIsInvalid(const http::request<http::string_body>&);
};
};  // namespace ad_utility::websocket
