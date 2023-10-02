//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <boost/beast/websocket.hpp>

#include "util/http/beast.h"
#include "util/http/websocket/QueryHub.h"
#include "util/http/websocket/QueryId.h"
#include "util/http/websocket/UpdateFetcher.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::QueryId;
using websocket = beast::websocket::stream<tcp::socket>;

/// Class to manage the lifecycle of a single websocket.
class WebSocketSession {
  UpdateFetcher updateFetcher_;
  websocket ws_;

  /// Wait for input from the client in a loop. Processing will happen in a
  /// future version of Qlever.
  net::awaitable<void> handleClientCommands();

  /// Wait for updates of the given query and send them to the client when they
  /// occur in a loop.
  net::awaitable<void> waitForServerEvents();

  /// Accept the websocket handshake and delegate further handling to
  /// `waitForServerEvents` and `handleClientCommands`
  net::awaitable<void> acceptAndWait(const http::request<http::string_body>&);

  /// Construct an instance of this class
  WebSocketSession(UpdateFetcher updateFetcher, tcp::socket socket)
      : updateFetcher_{std::move(updateFetcher)}, ws_{std::move(socket)} {}

 public:
  /// The main interface for this class. The HTTP server is supposed to check
  /// if an HTTP request is a websocket upgrade request and delegate further
  /// handling to this method if it is the case. It then accepts the websocket
  /// connection and "blocks" for the lifetime of it. Make sure to call this
  /// coroutine on an explicit strand, and make sure the passed socket's
  /// executor is that same strand, otherwise there may be race conditions.
  static net::awaitable<void> handleSession(
      QueryHub& queryHub, const http::request<http::string_body>& request,
      tcp::socket socket);
  /// Helper function to provide a proper error response if the provided URL
  /// path is not accepted by the server.
  static std::optional<http::response<http::string_body>>
  getErrorResponseIfPathIsInvalid(const http::request<http::string_body>&);
};
};  // namespace ad_utility::websocket
