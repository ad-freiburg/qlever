// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "util/http/HttpClient.h"

#include <absl/strings/str_cat.h>

#include <sstream>
#include <string>

#include "global/Constants.h"
#include "util/AsioHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/beast.h"

namespace beast = boost::beast;
namespace ssl = boost::asio::ssl;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;
using ad_utility::httpUtils::Url;

// Implemented using Boost.Beast, code apapted from
// https://www.boost.org/doc/libs/master/libs/beast/example/http/client/sync/http_client_sync.cpp
// https://www.boost.org/doc/libs/master/libs/beast/example/http/client/sync-ssl/http_client_sync_ssl.cpp

// ____________________________________________________________________________
template <typename StreamType>
HttpClientImpl<StreamType>::HttpClientImpl(std::string_view host,
                                           std::string_view port) {
  // IMPORTANT implementation note: Although we need only `stream_` later, it
  // is important that we also keep `io_context_` and `ssl_context_` alive.
  // Otherwise, we get a nasty and non-deterministic segmentation fault when
  // the destructor of `stream_` is called. It seems that `resolver` does not
  // have to stay alive.
  if constexpr (std::is_same_v<StreamType, beast::tcp_stream>) {
    tcp::resolver resolver{ioContext_};
    stream_ = std::make_unique<StreamType>(ioContext_);
    auto const results = resolver.resolve(host, port);
    stream_->connect(results);
  } else {
    static_assert(std::is_same_v<StreamType, ssl::stream<tcp::socket>>,
                  "StreamType must be either boost::beast::tcp_stream or "
                  "boost::asio::ssl::stream<boost::asio::ip::tcp::socket>");
    ssl_context_ = std::make_unique<ssl::context>(ssl::context::sslv23_client);
    ssl_context_->set_verify_mode(ssl::verify_none);
    tcp::resolver resolver{ioContext_};
    stream_ = std::make_unique<StreamType>(ioContext_, *ssl_context_);
    if (!SSL_set_tlsext_host_name(stream_->native_handle(),
                                  std::string{host}.c_str())) {
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
HttpClientImpl<StreamType>::~HttpClientImpl() {
  // We are closing the HTTP connection and destroying the client. So it is
  // neither required nor possible in a safe way to report errors from a
  // destructor and we can simply ignore the error codes.
  [[maybe_unused]] boost::system::error_code ec;

  // Close the stream. Ignore the value of `ec` because we don't care about
  // possible errors (we are done with the connection anyway, and there
  // is no simple and safe way to report errors from a destructor).
  if constexpr (std::is_same_v<StreamType, beast::tcp_stream>) {
    stream_->socket().shutdown(tcp::socket::shutdown_both, ec);
  } else {
    static_assert(std::is_same_v<StreamType, ssl::stream<tcp::socket>>,
                  "StreamType must be either boost::beast::tcp_stream or "
                  "boost::asio::ssl::stream<boost::asio::ip::tcp::socket>");
    stream_->shutdown(ec);
  }
}

// ____________________________________________________________________________
template <typename StreamType>
cppcoro::generator<std::span<std::byte>>
HttpClientImpl<StreamType>::sendRequest(
    const boost::beast::http::verb& method, std::string_view host,
    std::string_view target, ad_utility::SharedCancellationHandle handle,
    std::string_view requestBody, std::string_view contentTypeHeader,
    std::string_view acceptHeader) {
  // Check that we have a stream (created in the constructor).
  AD_CORRECTNESS_CHECK(stream_);

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

  auto wait = [this, &handle]<typename T>(
                  net::awaitable<T> awaitable,
                  ad_utility::source_location loc =
                      ad_utility::source_location::current()) -> T {
    return ad_utility::runAndWaitForAwaitable(
        ad_utility::interruptible(std::move(awaitable), handle, std::move(loc)),
        ioContext_);
  };

  // Send the request, receive the response (unlimited body size), and return
  // the body as a `std::istringstream`.
  wait(http::async_write(*stream_, request, net::use_awaitable));
  beast::flat_buffer buffer;
  http::response_parser<http::buffer_body> responseParser;
  responseParser.body_limit(std::numeric_limits<std::uint64_t>::max());
  wait(http::async_read_header(*stream_, buffer, responseParser,
                               net::use_awaitable));
  std::string aggregateBuffer;
  while (!responseParser.is_done()) {
    std::array<std::byte, 4096> staticBuffer;
    responseParser.get().body().data = staticBuffer.data();
    responseParser.get().body().size = staticBuffer.size();

    wait(http::async_read_some(*stream_, buffer, responseParser,
                               net::use_awaitable));
    size_t remainingBytes = responseParser.get().body().size;
    co_yield std::span{staticBuffer}.first(staticBuffer.size() -
                                           remainingBytes);
  }
}

// ____________________________________________________________________________
template <typename StreamType>
http::response<http::string_body>
HttpClientImpl<StreamType>::sendWebSocketHandshake(
    const boost::beast::http::verb& method, std::string_view host,
    std::string_view target) {
  // Check that we have a stream (created in the constructor).
  AD_CORRECTNESS_CHECK(stream_);

  // Set up the request.
  http::request<http::string_body> request;
  request.method(method);
  request.target(target);
  // See
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Protocol_upgrade_mechanism
  // for more information on the websocket handshake mechanism.
  request.set(http::field::host, host);
  request.set(http::field::upgrade, "websocket");
  request.set(http::field::connection, "Upgrade");
  request.set(http::field::sec_websocket_version, "13");
  request.set(http::field::sec_websocket_key, "8J+koQ==");

  http::write(*stream_, request);

  http::response<http::string_body> response;

  beast::flat_buffer buffer;
  http::read(*stream_, buffer, response);

  return response;
}

// Explicit instantiations for HTTP and HTTPS, see the bottom of
// `HttpClient.h`.
template class HttpClientImpl<beast::tcp_stream>;
template class HttpClientImpl<ssl::stream<tcp::socket>>;

// ____________________________________________________________________________
cppcoro::generator<std::span<std::byte>> sendHttpOrHttpsRequest(
    const ad_utility::httpUtils::Url& url,
    ad_utility::SharedCancellationHandle handle,
    const boost::beast::http::verb& method, std::string_view requestData,
    std::string_view contentTypeHeader, std::string_view acceptHeader) {
  auto sendRequest = [&]<typename Client>() {
    return
        [](const ad_utility::httpUtils::Url& url,
           ad_utility::SharedCancellationHandle handle,
           const boost::beast::http::verb& method, std::string_view requestData,
           std::string_view contentTypeHeader, std::string_view acceptHeader)
            -> cppcoro::generator<std::span<std::byte>> {
          Client client{url.host(), url.port()};
          auto generator = client.sendRequest(method, url.host(), url.target(),
                                              std::move(handle), requestData,
                                              contentTypeHeader, acceptHeader);
          // Don't return directly, to keep the client object alive.
          for (auto bytes : generator) {
            co_yield bytes;
          }
        }(url, std::move(handle), method, requestData, contentTypeHeader,
            acceptHeader);
  };
  if (url.protocol() == Url::Protocol::HTTP) {
    return sendRequest.operator()<HttpClient>();
  } else {
    AD_CORRECTNESS_CHECK(url.protocol() == Url::Protocol::HTTPS);
    return sendRequest.operator()<HttpsClient>();
  }
}
