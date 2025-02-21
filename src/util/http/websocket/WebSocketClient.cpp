// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include "WebSocketClient.h"

#include <utility>
#include <variant>

#include "util/Log.h"

namespace ad_utility::websocket {
// ____________________________________________________________________________
template <>
HttpWebSocketClient::WebSocketClientImpl(const std::string& host,
                                         const std::string& port,
                                         const std::string& target)
    : ioc_(),
      resolver_(ioc_),
      ws_(ioc_),
      host_(host),
      port_(port),
      target_(target) {}

// ____________________________________________________________________________
template <>
HttpsWebSocketClient::WebSocketClientImpl(const std::string& host,
                                          const std::string& port,
                                          const std::string& target)
    : ioc_(),
      resolver_(ioc_),
      ctx_(getSslContext()),
      ws_(ioc_, ctx_.value()),
      host_(host),
      port_(port),
      target_(target) {}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::start() {
  asyncResolve();
  ioThread_ = std::thread([this]() { ioc_.run(); });
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::close() {
  if (!ws_.is_open()) {
    ws_.async_close(
        websocket::close_code::normal, [this](beast::error_code ec) {
          if (ec) {
            LOG(ERROR) << "WebSocketClient: " << ec.message() << '\n';
          }
          ioc_.stop();
        });
  }

  if (ioThread_.joinable()) ioThread_.join();
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::asyncResolve() {
  resolver_.async_resolve(
      host_, port_,
      [this](beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) {
          LOG(ERROR) << "WebSocketClient: " << ec.message() << "\n";
          return;
        }
        asyncConnect(results);
      });
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::asyncConnect(
    tcp::resolver::results_type results) {
  constexpr bool isSSL =
      std::is_same_v<StreamType, beast::ssl_stream<tcp::socket>>;
  auto getLowest = [&]() -> decltype(auto) {
    if constexpr (isSSL) {
      return ws_.next_layer().next_layer();
    } else {
      return ws_.next_layer();
    }
  };

  net::async_connect(
      getLowest(), results, [this](beast::error_code ec, tcp::endpoint) {
        if (ec) {
          LOG(ERROR) << "WebSocketClient: " << ec.message() << '\n';
        }
        LOG(INFO) << "WebSocketClient connected to " << host_ << ":" << port_
                  << '\n';
        if constexpr (isSSL) {
          asyncSSLHandshake();
        } else {
          asyncWebsocketHandshake();
        }
      });
}

// ____________________________________________________________________________
template <typename StreamType>
template <typename T>
std::enable_if_t<std::is_same_v<T, beast::ssl_stream<tcp::socket>>, void>
WebSocketClientImpl<StreamType>::asyncSSLHandshake() {
  ws_.next_layer().async_handshake(
      ssl::stream_base::client, [this](beast::error_code ec) {
        if (ec) {
          std::cerr << "SSL handshake error: " << ec.message() << " "
                    << ec.value() << " " << ec.category().name() << "\n";
          return;
        }
        asyncWebsocketHandshake();
      });
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::asyncWebsocketHandshake() {
  // TODO<unex>: The WebsocketHandshake currently does not work with SSL.
  if constexpr (std::is_same_v<StreamType, beast::ssl_stream<tcp::socket>>) {
    ws_.set_option(
        websocket::stream_base::decorator([this](websocket::request_type& req) {
          req.set(beast::http::field::host, host_);
          req.set(beast::http::field::upgrade, "websocket");
          req.set(beast::http::field::connection, "Upgrade");
          req.set(beast::http::field::sec_websocket_version, "13");
          req.set(beast::http::field::sec_websocket_key, "8J+koQ==");
        }));
  }
  ws_.async_handshake(host_, target_, [this](beast::error_code ec) {
    if (ec) {
      LOG(ERROR) << "WebSocket handshake error: " << ec.message() << '\n';
      return;
    }
    readMessages();
  });
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::readMessages() {
  auto on_read = [this](beast::error_code ec, std::size_t bytes_transferred) {
    if (ec == websocket::error::closed) {
      return;
    }

    if (ec) {
      LOG(ERROR) << "Read error: " << ec.message() << '\n';
      return;
    }

    std::string msg = beast::buffers_to_string(buffer_.data());
    buffer_.consume(bytes_transferred);

    if (messageHandler_) {
      messageHandler_(msg);
    }

    readMessages();
  };

  ws_.async_read(buffer_, on_read);
}

template class WebSocketClientImpl<tcp::socket>;
template class WebSocketClientImpl<beast::ssl_stream<tcp::socket>>;

WebSocketVariant getRuntimeInfoClient(
    const ad_utility::httpUtils::Url& url, const std::string& target,
    std::function<void(const std::string&)> msgHandler) {
  auto getClient = [&]<typename WsType>() {
    WebSocketVariant c =
        std::make_unique<WsType>(url.host(), url.port(), target);
    std::visit(
        [&](auto& client) {
          client->setMessageHandler(msgHandler);
          client->start();
        },
        c);
    return c;
  };

  if (url.protocol() == ad_utility::httpUtils::Url::Protocol::HTTP) {
    return getClient.operator()<HttpWebSocketClient>();
  } else {
    return getClient.operator()<HttpWebSocketClient>();
  }
}
}  // namespace ad_utility::websocket
