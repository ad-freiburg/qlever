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
  void fireAllCallbacksForQuery(QueryId queryId, RuntimeInformationSnapshot runtimeInformationSnapshot);
};
