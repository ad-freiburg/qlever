// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "util/http/HttpClient.h"

#include <absl/strings/str_cat.h>

#include <boost/url/url.hpp>
#include <string>

#include "global/Constants.h"
#include "util/AsioHelpers.h"
#include "util/TypeIdentity.h"
#include "util/http/HttpUtils.h"
#include "util/http/beast.h"

namespace beast = boost::beast;
namespace ssl = boost::asio::ssl;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;
using ad_utility::httpUtils::Url;
using Proxy = ad_utility::httpProxy::Proxy;

// Implemented using Boost.Beast, code adapted from
// https://www.boost.org/doc/libs/master/libs/beast/example/http/client/sync/http_client_sync.cpp
// https://www.boost.org/doc/libs/master/libs/beast/example/http/client/sync-ssl/http_client_sync_ssl.cpp

namespace {
// Ask `proxy` to open a tunnel to `host`:`port` over the already connected
// `socket`, using the `CONNECT` method, and check that it agreed. Throws if the
// proxy refuses. Must be called before the TLS handshake, so that the handshake
// happens with the target host and not with the proxy.
//
// Note: this is a free function and not a member of `HttpClientImpl`, because
// the explicit instantiation for the plain-HTTP case at the bottom of this file
// would instantiate all members, and `beast::tcp_stream` has no `next_layer()`.
//
// TODO: Like the `connect` and `handshake` calls in the constructor below,
// this exchange is blocking and has no timeout, so a proxy that accepts the
// TCP connection but never answers blocks the calling thread indefinitely
// (and out of reach of the cancellation handle). The whole connection setup
// should eventually get a deadline.
void establishProxyTunnel(tcp::socket& socket, const Proxy& proxy,
                          std::string_view host, std::string_view port) {
  // The target of a `CONNECT` request is in authority form, that is
  // `host:port` with the port always explicit (RFC 9110, 9.3.6).
  std::string authority = absl::StrCat(host, ":", port);
  http::request<http::empty_body> request{http::verb::connect, authority, 11};
  request.set(http::field::host, authority);
  request.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
  if (!proxy.authorization_.empty()) {
    request.set(http::field::proxy_authorization, proxy.authorization_);
  }
  // Note: deliberately no `prepare_payload()`, which would add a
  // `Content-Length: 0` that some proxies reject on a `CONNECT`.
  http::write(socket, request);

  beast::flat_buffer buffer;
  http::response_parser<http::empty_body> responseParser;
  // A successful response to `CONNECT` has neither a body nor a
  // `Content-Length`, because everything after the header belongs to the
  // tunnel. We must therefore tell the parser not to expect a body; otherwise
  // it would read until the connection is closed and the tunnel would never be
  // established.
  responseParser.skip(true);
  http::read(socket, buffer, responseParser);

  const auto& response = responseParser.get();
  unsigned status = response.result_int();
  if (status < 200 || status >= 300) {
    throw std::runtime_error(absl::StrCat(
        "The HTTP proxy <", proxy.host_, ":", proxy.port_,
        "> refused to open a tunnel to <", authority,
        "> and responded with: ", status, " ", toStd(response.reason()),
        status == 407 ? ". Note that credentials for the proxy can be given as "
                        "part of the proxy URL, as in "
                        "`http_proxy=http://user:password@proxy:3128`"
                      : ""));
  }

  // Any bytes that arrived after the response header already belong to the
  // tunneled TLS stream, and there is no way to hand a pre-filled buffer to
  // `ssl::stream`. Proxies do not send anything before the client speaks, so
  // this should never happen; fail loudly rather than corrupt the handshake.
  if (buffer.size() != 0) {
    throw std::runtime_error(absl::StrCat(
        "The HTTP proxy <", proxy.host_, ":", proxy.port_, "> sent ",
        buffer.size(),
        " unexpected byte(s) directly after its response to the CONNECT "
        "request, which QLever cannot forward into the TLS session"));
  }
}
}  // namespace

// ____________________________________________________________________________
template <typename StreamType>
HttpClientImpl<StreamType>::HttpClientImpl(std::string_view host,
                                           std::string_view port,
                                           const std::optional<Proxy>& proxy) {
  // With a proxy, the TCP connection goes to the proxy, and it is the proxy
  // that connects to `host`:`port` on our behalf. Everything below is otherwise
  // identical to the direct case, except for the `CONNECT` tunnel for HTTPS.
  std::string_view connectHost = proxy.has_value() ? proxy->host_ : host;
  std::string_view connectPort = proxy.has_value() ? proxy->port_ : port;

  // IMPORTANT implementation note: Although we need only `stream_` later, it
  // is important that we also keep `io_context_` and `ssl_context_` alive.
  // Otherwise, we get a nasty and non-deterministic segmentation fault when
  // the destructor of `stream_` is called. It seems that `resolver` does not
  // have to stay alive.
  if constexpr (std::is_same_v<StreamType, beast::tcp_stream>) {
    tcp::resolver resolver{ioContext_};
    stream_ = std::make_unique<StreamType>(ioContext_);
    auto const results = resolver.resolve(connectHost, connectPort);
    stream_->connect(results);
    // No tunnel needed: a plain HTTP request is relayed by the proxy based on
    // the absolute form of its request target, see `absoluteFormTarget`. The
    // proxy credentials, however, have to go on every relayed request, see
    // `sendRequest`.
    if (proxy.has_value()) {
      proxyAuthorization_ = proxy->authorization_;
    }
  } else {
    static_assert(std::is_same_v<StreamType, ssl::stream<tcp::socket>>,
                  "StreamType must be either boost::beast::tcp_stream or "
                  "boost::asio::ssl::stream<boost::asio::ip::tcp::socket>");
    ssl_context_ = std::make_unique<ssl::context>(ssl::context::sslv23_client);
    ssl_context_->set_verify_mode(ssl::verify_none);
    tcp::resolver resolver{ioContext_};
    stream_ = std::make_unique<StreamType>(ioContext_, *ssl_context_);
    // Note that the SNI host is the target host, also when going through a
    // proxy: the TLS session is with the target, the proxy only forwards bytes.
    if (!SSL_set_tlsext_host_name(stream_->native_handle(),
                                  std::string{host}.c_str())) {
      boost::system::error_code ec{static_cast<int>(::ERR_get_error()),
                                   boost::asio::error::get_ssl_category()};
      throw boost::system::system_error{ec};
    }
    auto const results = resolver.resolve(connectHost, connectPort);
    boost::asio::connect(stream_->next_layer(), results.begin(), results.end());
    if (proxy.has_value()) {
      establishProxyTunnel(stream_->next_layer(), proxy.value(), host, port);
    }
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
HttpOrHttpsResponse HttpClientImpl<StreamType>::sendRequest(
    std::unique_ptr<HttpClientImpl<StreamType>> client,
    const boost::beast::http::verb& method, std::string_view host,
    std::string_view target, ad_utility::SharedCancellationHandle handle,
    std::string_view requestBody, std::string_view contentTypeHeader,
    std::string_view acceptHeader) {
  // Check that the client pointer is valid.
  AD_CORRECTNESS_CHECK(client);
  // Check that we have a stream (created in the constructor).
  AD_CORRECTNESS_CHECK(client->stream_);

  // Set up the request.
  http::request<http::string_body> request;
  request.method(method);
  request.target(target);
  request.set(http::field::host, host);
  // A relayed plain HTTP request carries the proxy credentials on every
  // request; the proxy consumes this header and does not forward it (RFC 9110,
  // 11.7.2). For HTTPS the credentials are sent on the `CONNECT` request
  // instead, see `establishProxyTunnel`.
  if (!client->proxyAuthorization_.empty()) {
    request.set(http::field::proxy_authorization, client->proxyAuthorization_);
  }
  request.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
  request.set(http::field::accept, acceptHeader);
  request.set(http::field::content_type, contentTypeHeader);
  request.set(http::field::content_length, std::to_string(requestBody.size()));
  request.body() = requestBody;

  auto wait = [&client, &handle](
                  auto awaitable,
                  ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) ->
      typename decltype(awaitable)::value_type {
        static_assert(
            ad_utility::isInstantiation<decltype(awaitable), net::awaitable>);
        return ad_utility::runAndWaitForAwaitable(
            ad_utility::interruptible(std::move(awaitable), handle,
                                      std::move(loc)),
            client->ioContext_);
      };

  // Send the request, receive the response (unlimited body size).
  wait(http::async_write(*(client->stream_), request, net::use_awaitable));
  beast::flat_buffer buffer;
  auto responseParser =
      std::make_unique<http::response_parser<http::buffer_body>>();
  responseParser->body_limit(boost::none);
  wait(http::async_read_header(*(client->stream_), buffer, *responseParser,
                               net::use_awaitable));

  const auto status = responseParser->get().result();
  const std::string contentType =
      responseParser->get()[http::field::content_type];
  const std::string location = responseParser->get()[http::field::location];

  auto getBody = [](std::unique_ptr<HttpClientImpl<StreamType>> client,
                    std::unique_ptr<http::response_parser<http::buffer_body>>
                        responseParser,
                    beast::flat_buffer buffer,
                    ad_utility::SharedCancellationHandle handle)
      -> cppcoro::generator<ql::span<std::byte>> {
    while (!responseParser->is_done()) {
      std::array<std::byte, 4096> staticBuffer;
      responseParser->get().body().data = staticBuffer.data();
      responseParser->get().body().size = staticBuffer.size();
      ad_utility::runAndWaitForAwaitable(
          ad_utility::interruptible(
              http::async_read_some(*(client->stream_), buffer, *responseParser,
                                    net::use_awaitable),
              handle, AD_CURRENT_SOURCE_LOC()),
          client->ioContext_);
      size_t remainingBytes = responseParser->get().body().size;
      co_yield ql::span{staticBuffer}.first(staticBuffer.size() -
                                            remainingBytes);
    }
  };

  return {.status_ = status,
          .contentType_ = contentType,
          .location_ = location,
          .body_ = getBody(std::move(client), std::move(responseParser),
                           std::move(buffer), std::move(handle))};
}

// ____________________________________________________________________________
template <typename StreamType>
http::response<http::string_body>
HttpClientImpl<StreamType>::sendWebSocketHandshake(const http::verb& method,
                                                   std::string_view host,
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
HttpOrHttpsResponse sendHttpOrHttpsRequest(
    const ad_utility::httpUtils::Url& url,
    ad_utility::SharedCancellationHandle handle, const http::verb& method,
    std::string_view requestData, std::string_view contentTypeHeader,
    std::string_view acceptHeader, size_t maxRedirects) {
  return sendHttpOrHttpsRequestWithProxy(
      url, std::move(handle), method, requestData, contentTypeHeader,
      acceptHeader, maxRedirects, ad_utility::httpProxy::globalProxy());
}

// ____________________________________________________________________________
HttpOrHttpsResponse sendHttpOrHttpsRequestWithProxy(
    const ad_utility::httpUtils::Url& url,
    ad_utility::SharedCancellationHandle handle, const http::verb& method,
    std::string_view requestData, std::string_view contentTypeHeader,
    std::string_view acceptHeader, size_t maxRedirects,
    const std::optional<Proxy>& proxy) {
  auto sendRequest = [&](const Url& currentUrl,
                         auto ti) -> HttpOrHttpsResponse {
    using Client = typename decltype(ti)::type;
    auto client =
        std::make_unique<Client>(currentUrl.host(), currentUrl.port(), proxy);
    // A plain HTTP request that is relayed by a proxy has to name its target in
    // absolute form, so that the proxy knows where to relay it to. For HTTPS
    // the connection is tunneled through the proxy, which then never sees the
    // request, so the target stays in origin form.
    std::string target = currentUrl.target();
    if constexpr (std::is_same_v<Client, HttpClient>) {
      if (proxy.has_value()) {
        target = ad_utility::httpProxy::absoluteFormTarget(
            currentUrl.host(), currentUrl.port(), target);
      }
    }
    return Client::sendRequest(std::move(client), method, currentUrl.host(),
                               target, handle, requestData, contentTypeHeader,
                               acceptHeader);
  };

  using namespace ad_utility::use_type_identity;

  // Follow redirects up to the limit specified by maxRedirects.
  boost::url currentUrl = boost::url{url.asString()};
  size_t redirectCount = 0;
  while (redirectCount <= maxRedirects) {
    HttpOrHttpsResponse response;
    if (currentUrl.scheme() == "http") {
      response = sendRequest(Url{currentUrl.c_str()}, ti<HttpClient>);
    } else {
      AD_CORRECTNESS_CHECK(currentUrl.scheme() == "https");
      response = sendRequest(Url{currentUrl.c_str()}, ti<HttpsClient>);
    }

    // Check if the response is a redirect (301, 302, 307, 308), we don't modify
    // the request type to GET for status codes 301 and 302, which would be the
    // behavior of browsers.
    using enum http::status;
    bool isRedirect =
        ad_utility::contains(std::array{moved_permanently, found,
                                        temporary_redirect, permanent_redirect},
                             response.status_);
    if (!isRedirect) {
      return response;
    }

    // If it is a redirect, but there is no `Location` header, we cannot
    // proceed and throw an error.
    if (response.location_.empty()) {
      throw std::runtime_error(absl::StrCat(
          "HTTP request to <", currentUrl.c_str(),
          "> responded with redirect status code: ", response.status_,
          " but no Location header was provided"));
    }

    // Follow the redirect and properly resolve relative URLs.
    boost::system::result<boost::url_view> relativeUrl =
        boost::urls::parse_uri_reference(response.location_);
    if (relativeUrl.has_error()) {
      throw std::runtime_error(absl::StrCat(
          "The HTTP request to '", currentUrl.c_str(),
          "' responded with redirect status code: ", response.status_,
          " but the Location header value '", response.location_,
          "' could not be parsed: ", relativeUrl.error().message()));
    }
    auto result = currentUrl.resolve(relativeUrl.value());
    AD_CORRECTNESS_CHECK(!result.has_error(), "Error while resolving URL:",
                         result.error().message());
    redirectCount++;
  }

  // If the loop exited because we exceeded the maximum number of redirects,
  // throw a corresponding error.
  throw std::runtime_error(absl::StrCat("HTTP request to <", url.asString(),
                                        "> exceeded maximum redirect limit of ",
                                        maxRedirects));
}
#endif
