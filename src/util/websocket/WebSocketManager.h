#pragma once

#include "../http/beast.h"
#include <boost/beast/websocket.hpp>

#include "./Common.h"

namespace ad_utility::websocket {
  namespace net = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using boost::asio::ip::tcp;
  using websocket::common::QueryId;
  using websocket::common::RuntimeInformationSnapshot;

  net::awaitable<void> manageConnection(tcp::socket socket, http::request<http::string_body> request);
  // Returns true if there are other active connections that do no currently wait.
  bool fireAllCallbacksForQuery(const QueryId& queryId, RuntimeInformationSnapshot runtimeInformationSnapshot);

  std::optional<http::response<http::string_body>> checkPathIsValid(const http::request<http::string_body>& request);
};
