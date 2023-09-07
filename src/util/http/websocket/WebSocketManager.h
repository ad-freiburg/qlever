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
class WebSocketManager {
  QueryId queryId_;
  UpdateFetcher updateFetcher_;
  http::request<http::string_body> request_;
  websocket ws_;

  /// Loop that waits for input from the client
  net::awaitable<void> handleClientCommands();

  /// Loop that waits for an update of the given query to occur and send it
  /// to the client once it happens
  net::awaitable<void> waitForServerEvents();

  /// Helper method to create a QueryId based on a given request.
  static QueryId extractFromRequest(const http::request<http::string_body>&);

 public:
  /// Constructs an instance of this class
  explicit WebSocketManager(QueryHub& queryHub,
                            http::request<http::string_body> request,
                            tcp::socket socket)
      : queryId_{extractFromRequest(request)},
        updateFetcher_{queryId_, queryHub},
        request_(std::move(request)),
        ws_{std::move(socket)} {}

  /// The main interface for this class. The HTTP server is supposed to check
  /// if an HTTP request is a websocket upgrade request and delegate further
  /// handling to this method if it is the case. It then accepts the websocket
  /// connection and "blocks" for the lifetime of it.
  net::awaitable<void> connectionLifecycle();

  /// Helper function to provide a proper error response if the provided URL
  /// path is not accepted by the server.
  static std::optional<http::response<http::string_body>>
  getErrorResponseIfPathIsInvalid(const http::request<http::string_body>&);
};
};  // namespace ad_utility::websocket
