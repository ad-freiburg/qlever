// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#pragma once
#include <gtest/gtest_prod.h>

#include <boost/beast/ssl.hpp>
#include <functional>

#include "util/http/HttpUtils.h"
#include "util/http/beast.h"

namespace ad_utility::websocket {
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

/*
 * Class managing a WebSocketClient.
 * Calling the start function initializes the WebSocket connection in a
 * background thread. All incoming messages are passed to the `msgHandler_`
 * callback function.
 */
template <typename StreamType>
class WebSocketClientImpl {
  using MessageHandlerFunction = std::function<void(const std::string&)>;

 public:
  WebSocketClientImpl(std::string_view hostname, std::string_view port,
                      std::string_view target,
                      MessageHandlerFunction msgHandler);

  // Destructor gracefully closes the connection.
  ~WebSocketClientImpl() { close(); }

  // Start the websocket connection.
  void start();

  // Closes the connection.
  void close();

 private:
  // The following methods are used to setup the websocket-connection.
  void asyncResolve();

  void asyncConnect(tcp::resolver::results_type results);

  template <typename T = StreamType>
  std::enable_if_t<std::is_same_v<T, beast::ssl_stream<tcp::socket>>, void>
  asyncSSLHandshake();

  void asyncWebsocketHandshake();

  // Read incoming messages and handle them with the `msgHandler_`.
  void readMessages();

  // Stream and related objects.
  net::io_context ioContext_;
  net::ip::tcp::resolver resolver_;
  std::optional<ssl::context> sslContext_;
  websocket::stream<StreamType> stream_;
  JThread ioThread_;

  // Buffer for incoming messages.
  beast::flat_buffer buffer_;

  // Url of the endpoint.
  std::string host_;
  std::string port_;
  std::string target_;

  // Handler function to handle received messages in `readMessages()`.
  MessageHandlerFunction msgHandler_;

  // Connection indicator for thread-safe testing.
  std::atomic<bool> isConnected_{false};

  FRIEND_TEST(WebSocketClient, HttpConnection);
  FRIEND_TEST(WebSocketClient, ReadMessages);
};

using HttpWebSocketClient = WebSocketClientImpl<tcp::socket>;
using HttpsWebSocketClient =
    WebSocketClientImpl<beast::ssl_stream<tcp::socket>>;

using WebSocketClientVariant =
    std::variant<std::unique_ptr<HttpWebSocketClient>,
                 std::unique_ptr<HttpsWebSocketClient>>;

// Convenience function to get a WebSocketClient handling all incoming messages
// with the given `msgHandler_`.
WebSocketClientVariant getWebSocketClient(
    const ad_utility::httpUtils::Url& url, const std::string& target,
    const std::function<void(const std::string&)>& msgHandler);

// Concatenates two url paths, expects both of them to start with a '/'.
std::string concatUrlPaths(std::string_view a, std::string_view b);

}  // namespace ad_utility::websocket
