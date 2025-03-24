// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include "WebSocketClient.h"

#include <variant>

#include "util/Log.h"
#include "util/TypeIdentity.h"

namespace ad_utility::websocket {

// ____________________________________________________________________________
template <>
HttpWebSocketClient::WebSocketClientImpl(std::string_view hostname,
                                         std::string_view port,
                                         std::string_view target)
    : ioContext_(),
      resolver_(ioContext_),
      stream_(ioContext_),
      host_(hostname),
      port_(port),
      target_(target) {}

// ____________________________________________________________________________
template <>
HttpsWebSocketClient::WebSocketClientImpl(std::string_view hostname,
                                          std::string_view port,
                                          std::string_view target)
    : ioContext_(),
      resolver_(ioContext_),
      sslContext_([]() {
        ssl::context ctx(ssl::context::sslv23_client);
        ctx.set_verify_mode(ssl::verify_none);
        return ctx;
      }()),
      stream_(ioContext_, sslContext_.value()),
      host_(hostname),
      port_(port),
      target_(target) {}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::start() {
  asyncResolve();
  ioThread_ = JThread{[this]() { ioContext_.run(); }};
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::close() {
  if (isConnected_) {
    net::post(ioContext_, [this]() {
      stream_.async_close(
          websocket::close_code::normal, [this](beast::error_code ec) {
            if (ec) {
              LOG(ERROR) << "WebSocketClient: " << ec.message() << '\n';
            }
            ioContext_.stop();
          });
    });
  } else {
    ioContext_.stop();
  }
  if (ioThread_.joinable()) ioThread_.join();
  isConnected_ = false;
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
  auto getLowestLayer = [&]() -> decltype(auto) {
    if constexpr (isSSL) {
      return stream_.next_layer().next_layer();
    } else {
      return stream_.next_layer();
    }
  };

  net::async_connect(
      getLowestLayer(), results, [this](beast::error_code ec, tcp::endpoint) {
        if (ec) {
          LOG(ERROR) << "WebSocketClient: " << ec.message() << '\n';
          return;
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
  // Set SNI Hostname (many hosts need this to handshake successfully)
  if (!SSL_set_tlsext_host_name(stream_.next_layer().native_handle(),
                                host_.c_str()))
    throw beast::system_error(
        beast::error_code(static_cast<int>(::ERR_get_error()),
                          net::error::get_ssl_category()),
        "Failed to set SNI Hostname");
  stream_.next_layer().async_handshake(
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
  stream_.async_handshake(
      absl::StrCat(host_, ":", port_), target_, [this](beast::error_code ec) {
        if (ec) {
          LOG(ERROR) << "WebSocket handshake error: " << ec.message() << '\n';
          return;
        }
        // Websocket handshake successful.
        // Currently this is hardcoded to immediately start reading messages,
        // keeping the `ioContext_` running.
        isConnected_ = true;
        readMessages();
      });
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::readMessages() {
  auto onRead = [this](beast::error_code ec, std::size_t bytes_transferred) {
    if (ec == websocket::error::closed || ec == ssl::error::stream_truncated) {
      return;
    }
    if (ec) {
      LOG(ERROR) << "Read error: " << ec.message() << '\n';
      return;
    }

    std::string msg = beast::buffers_to_string(buffer_.data());
    buffer_.consume(bytes_transferred);
    if (msgHandler_) {
      msgHandler_(msg);
    }
    readMessages();
  };

  stream_.async_read(buffer_, onRead);
}

template class WebSocketClientImpl<tcp::socket>;
template class WebSocketClientImpl<beast::ssl_stream<tcp::socket>>;

// ____________________________________________________________________________
std::string concatUrlPaths(std::string_view a, std::string_view b) {
  AD_CONTRACT_CHECK(a.starts_with('/') && b.starts_with('/'));
  return absl::StrCat((a.back() == '/' ? a.substr(0, a.size() - 1) : a), b);
}

// ____________________________________________________________________________
WebSocketClientVariant getWebSocketClient(
    const ad_utility::httpUtils::Url& url, const std::string& webSocketPath,
    const std::function<void(const std::string&)>& msgHandler) {
  using namespace ad_utility::use_type_identity;
  auto getClient = [&]<typename T>(TI<T>) -> WebSocketClientVariant {
    auto client = std::make_unique<T>(
        url.host(), url.port(), concatUrlPaths(url.target(), webSocketPath));
    client->setMessageHandler(msgHandler);
    client->start();
    return client;
  };

  return url.protocol() == ad_utility::httpUtils::Url::Protocol::HTTP
             ? getClient(ti<HttpWebSocketClient>)
             : getClient(ti<HttpsWebSocketClient>);
}
}  // namespace ad_utility::websocket
