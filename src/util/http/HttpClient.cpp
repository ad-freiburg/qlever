// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "util/http/HttpClient.h"

#include <sstream>
#include <string>

#include "absl/strings/str_cat.h"
#include "util/http/HttpUtils.h"
#include "util/http/beast.h"

namespace beast = boost::beast;
namespace ssl = boost::asio::ssl;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;

// Implemented using Boost.Beast, code apapted from
// https://www.boost.org/doc/libs/master/libs/beast/example/http/client/sync/http_client_sync.cpp
// https://www.boost.org/doc/libs/master/libs/beast/example/http/client/sync-ssl/http_client_sync_ssl.cpp

// ____________________________________________________________________________
template <typename StreamType>
HttpClientImpl<StreamType>::HttpClientImpl(const std::string& host,
                                           const std::string& port) {
  // IMPORTANT implementation note: Although we need only `stream_` later, it
  // is important that we also keep `io_context_` and `ssl_context_` alive.
  // Otherwise, we get a nasty and non-deterministic segmentation fault when
  // the destructor of `stream_` is called. It seems that `resolver` does not
  // have to stay alive.
  if constexpr (std::is_same_v<StreamType, beast::tcp_stream>) {
    tcp::resolver resolver{io_context_};
    stream_ = std::make_unique<StreamType>(io_context_);
    auto const results = resolver.resolve(host, port);
    stream_->connect(results);
  } else {
    static_assert(std::is_same_v<StreamType, ssl::stream<tcp::socket>>,
                  "StreamType must be either boost::beast::tcp_stream or "
                  "boost::asio::ssl::stream<boost::asio::ip::tcp::socket>");
    ssl_context_ = std::make_unique<ssl::context>(ssl::context::sslv23_client);
    ssl_context_->set_verify_mode(ssl::verify_none);
    tcp::resolver resolver{io_context_};
    stream_ = std::make_unique<StreamType>(io_context_, *ssl_context_);
    if (!SSL_set_tlsext_host_name(stream_->native_handle(), host.c_str())) {
      boost::system::error_code ec{static_cast<int>(::ERR_get_error()),
                                   boost::asio::error::get_ssl_category()};
      throw boost::system::system_error{ec};
    }
    auto const results = resolver.resolve(host, port);
    boost::asio::connect(stream_->next_layer(), results.begin(), results.end());
    stream_->handshake(ssl::stream_base::client);
  }
}

// ____________________________________________________________________________
template <typename StreamType>
HttpClientImpl<StreamType>::~HttpClientImpl() noexcept(false) {
  boost::system::error_code ec;
  if constexpr (std::is_same_v<StreamType, beast::tcp_stream>) {
    stream_->socket().shutdown(tcp::socket::shutdown_both, ec);
    // `not_connected happens sometimes, so don't bother reporting it.
    if (ec && ec != beast::errc::not_connected) {
      if (std::uncaught_exceptions() == 0) {
        throw beast::system_error{ec};
      }
    }
  } else {
    static_assert(std::is_same_v<StreamType, ssl::stream<tcp::socket>>,
                  "StreamType must be either boost::beast::tcp_stream or "
                  "boost::asio::ssl::stream<boost::asio::ip::tcp::socket>");
    stream_->shutdown(ec);
    // Gracefully close the stream. The `if` part is explained on
    // http://stackoverflow.com/questions/25587403/boost-asio-ssl-async-shutdown-always-finishes-with-an-error
    if (ec == boost::asio::error::eof) {
      ec.assign(0, ec.category());
    }
    if (ec) {
      if (std::uncaught_exceptions() == 0) {
        throw boost::system::system_error{ec};
      }
    }
  }
}

// ____________________________________________________________________________
template <typename StreamType>
std::istringstream HttpClientImpl<StreamType>::sendRequest(
    const boost::beast::http::verb& method, const std::string& host,
    const std::string& target, const std::string& requestBody,
    const std::string& contentTypeHeader, const std::string& acceptHeader) {
  // Check that we have a stream (obtained via a call to `openStream` above).
  if (!stream_) {
    throw std::runtime_error("Trying to send request without connection");
  }

  // Set up the request.
  http::request<http::string_body> request;
  request.method(method);
  request.target(target);
  request.set(http::field::host, host);
  request.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
  request.set(http::field::accept, acceptHeader);
  request.set(http::field::content_type, contentTypeHeader);
  request.set(http::field::content_length, std::to_string(requestBody.size()));
  request.body() = requestBody;

  // Send the request, receive the response (unlimited body size), and return
  // the body as a `std::istringstream`.
  http::write(*stream_, request);
  beast::flat_buffer buffer;
  http::response_parser<http::dynamic_body> response_parser;
  response_parser.body_limit((std::numeric_limits<std::uint64_t>::max)());
  http::read(*stream_, buffer, response_parser);
  std::istringstream responseBody(
      beast::buffers_to_string(response_parser.get().body().data()));
  return responseBody;
}

// Explicit instantiations for HTTP and HTTPS, see the bottom of `HttpClient.h`.
template class HttpClientImpl<beast::tcp_stream>;
template class HttpClientImpl<ssl::stream<tcp::socket>>;
