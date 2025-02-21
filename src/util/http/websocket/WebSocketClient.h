// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#pragma once

#include <gtest/gtest_prod.h>
#include <util/http/beast.h>

#include <boost/beast/ssl.hpp>
#include <functional>
#include <thread>

#include "util/http/HttpUtils.h"

namespace ad_utility::websocket {
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

template <typename StreamType>
class WebSocketClientImpl {
 public:
  using MessageHandler = std::function<void(const std::string&)>;

  WebSocketClientImpl(const std::string& host, const std::string& port,
                      const std::string& target);

  ~WebSocketClientImpl() { close(); }

  void start();

  void setMessageHandler(MessageHandler handler) { messageHandler_ = handler; }

  void close();

 private:
  void asyncResolve();

  void asyncConnect(tcp::resolver::results_type results);

  template <typename T = StreamType>
  std::enable_if_t<std::is_same_v<T, beast::ssl_stream<tcp::socket>>, void>
  asyncSSLHandshake();

  static ssl::context getSslContext() {
    ssl::context ctx(ssl::context::sslv23_client);
    ctx.set_verify_mode(ssl::verify_none);
    return ctx;
  }

  void asyncWebsocketHandshake();

  void readMessages();

  net::io_context ioc_;
  std::thread ioThread_;

  net::ip::tcp::resolver resolver_;

  std::optional<ssl::context> ctx_;
  websocket::stream<StreamType> ws_;

  beast::flat_buffer buffer_;

  std::string host_;
  std::string port_;
  std::string target_;
  MessageHandler messageHandler_;
};

using HttpWebSocketClient = WebSocketClientImpl<tcp::socket>;
using HttpsWebSocketClient =
    WebSocketClientImpl<beast::ssl_stream<tcp::socket>>;

using WebSocketVariant = std::variant<std::unique_ptr<HttpWebSocketClient>,
                                      std::unique_ptr<HttpsWebSocketClient>>;

WebSocketVariant getRuntimeInfoClient(
    const ad_utility::httpUtils::Url& url, const std::string& target,
    std::function<void(const std::string&)> msgHandler);
}  // namespace ad_utility::websocket
