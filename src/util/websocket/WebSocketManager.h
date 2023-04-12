#pragma once

#include "../http/beast.h"
#include <boost/beast/websocket.hpp>

namespace ad_utility::websocket {
  namespace net = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using boost::asio::ip::tcp;

  net::awaitable<void> manageConnection(tcp::socket socket, http::request<http::string_body> request);
};
