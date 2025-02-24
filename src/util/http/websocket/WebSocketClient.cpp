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
HttpWebSocketClient::WebSocketClientImpl(const ad_utility::httpUtils::Url& url,
                                         const std::string& webSocketPath)
    : ioContext_(),
      resolver_(ioContext_),
      stream_(ioContext_),
      url_(url),
      target_(concatUrlPaths(url.target(), webSocketPath)) {}

// ____________________________________________________________________________
template <>
HttpsWebSocketClient::WebSocketClientImpl(const ad_utility::httpUtils::Url& url,
                                          const std::string& webSocketPath)
    : ioContext_(),
      resolver_(ioContext_),
      sslContext_([]() {
        ssl::context ctx(ssl::context::sslv23_client);
        ctx.set_verify_mode(ssl::verify_none);
        return ctx;
      }()),
      stream_(ioContext_, sslContext_.value()),
      url_(url),
      target_(concatUrlPaths(url.target(), webSocketPath)) {}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::start() {
  asyncResolve();
  ioThread_ = JThread{[this]() { ioContext_.run(); }};
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::close() {
  if (stream_.is_open()) {
    stream_.async_close(
        websocket::close_code::normal, [this](beast::error_code ec) {
          if (ec) {
            LOG(ERROR) << "WebSocketClient: " << ec.message() << '\n';
          }
          ioContext_.stop();
        });
  }
  if (ioThread_.joinable()) ioThread_.join();
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::asyncResolve() {
  resolver_.async_resolve(
      url_.host(), url_.port(),
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
        }
        LOG(INFO) << "WebSocketClient connected to " << url_.host() << ":"
                  << url_.port() << '\n';
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
                                url_.host().c_str()))
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
      absl::StrCat(url_.host(), ":", url_.port()), target_,
      [this](beast::error_code ec) {
        if (ec) {
          LOG(ERROR) << "WebSocket handshake error: " << ec.message() << '\n';
          return;
        }
        // Websocket handshake successful.
        // Currently this is hardcoded to immediately start reading messages,
        // keeping the `ioContext_` running.
        readMessages();
      });
}

// ____________________________________________________________________________
template <typename StreamType>
void WebSocketClientImpl<StreamType>::readMessages() {
  AD_CONTRACT_CHECK(stream_.is_open());
  auto onRead = [this](beast::error_code ec, std::size_t bytes_transferred) {
    if (ec == websocket::error::closed) {
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
WebSocketVariant getRuntimeInfoClient(
    const ad_utility::httpUtils::Url& url, const std::string& webSocketPath,
    const std::function<void(const std::string&)>& msgHandler) {
  using namespace ad_utility::use_type_identity;
  auto getClient = [&]<typename T>(TI<T>) {
    WebSocketVariant c = std::make_unique<T>(url, webSocketPath);
    std::visit(
        [&](auto& client) {
          client->setMessageHandler(msgHandler);
          client->start();
        },
        c);
    return c;
  };

  return url.protocol() == ad_utility::httpUtils::Url::Protocol::HTTP
             ? getClient(ti<HttpWebSocketClient>)
             : getClient(ti<HttpsWebSocketClient>);
}
}  // namespace ad_utility::websocket
