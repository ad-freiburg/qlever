//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <boost/beast/websocket.hpp>

#include "util/http/beast.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/QueryHub.h"
#include "util/http/websocket/UpdateFetcher.h"

namespace ad_utility::websocket {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using boost::asio::ip::tcp;
using websocket::common::QueryId;
using websocket = beast::websocket::stream<tcp::socket>;

/// Class to manage the lifecycle of a single websocket. Single-use only.
class WebSocketSession {
  UpdateFetcher updateFetcher_;
  http::request<http::string_body> request_;
  websocket ws_;

  /// Loop that waits for input from the client
  net::awaitable<void> handleClientCommands();

  /// Loop that waits for an update of the given query to occur and send it
  /// to the client once it happens
  net::awaitable<void> waitForServerEvents();

  /// Accepts the websocket handshake and delegates further handling to
  /// `waitForServerEvents` and `handleClientCommands`
  net::awaitable<void> acceptAndWait();

  /// Constructs an instance of this class
  WebSocketSession(UpdateFetcher updateFetcher,
                   http::request<http::string_body> request, tcp::socket socket)
      : updateFetcher_{std::move(updateFetcher)},
        request_(std::move(request)),
        ws_{std::move(socket)} {}

 public:
  /// The main interface for this class. The HTTP server is supposed to check
  /// if an HTTP request is a websocket upgrade request and delegate further
  /// handling to this method if it is the case. It then accepts the websocket
  /// connection and "blocks" for the lifetime of it.
  static net::awaitable<void> handleSession(
      QueryHub& queryHub, http::request<http::string_body> request,
      tcp::socket socket);
  /// Helper function to provide a proper error response if the provided URL
  /// path is not accepted by the server.
  static std::optional<http::response<http::string_body>>
  getErrorResponseIfPathIsInvalid(const http::request<http::string_body>&);
};
};  // namespace ad_utility::websocket
