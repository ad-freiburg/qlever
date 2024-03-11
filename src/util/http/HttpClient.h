// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

// IMPORTANT: The header `util/HttpServer/beast.h` redefines some variables
// (important for our code) which are also defined in some of the ´<boost/...>´
// headers. The header `util/HttpServer/beast.h` should therefore always come
// first. Otherwise, there will be "redefinition" warnings when compiling and
// possible segmentation faults at runtime.
//
// TODO: It goes without saying that we should try to get rid of this anomaly as
// soon as possible. We should avoid redefining variables in the beast code, the
// order of the includes should not matter, and it should certainly not cause
// segmentation faults.

#include <span>
#include <string>

#include "util/CancellationHandle.h"
#include "util/Generator.h"
#include "util/http/HttpUtils.h"
#include "util/http/beast.h"

// A class for basic communication with a remote server via HTTP or HTTPS. For
// now, contains functionality for setting up a connection, sending one or
// several GET or POST requests (and getting the response), and closing the
// connection.
//
// The `StreamType` determines whether the protocol used will be HTTP or HTTPS,
// see the two instantiations `HttpCLient` and `HttpsClient` below.
template <typename StreamType>
class HttpClientImpl {
 public:
  // The constructor sets up the connection to the client.
  HttpClientImpl(std::string_view host, std::string_view port);

  // The destructor closes the connection.
  ~HttpClientImpl();

  // Send a request (the first argument must be either `http::verb::get` or
  // `http::verb::post`) and return the body of the reponse (possibly very
  // large) as an `cppcoro::generator<std::string_view>`. The same connection
  // can be used for multiple requests in a row.
  cppcoro::generator<std::span<std::byte>> sendRequest(
      const boost::beast::http::verb& method, std::string_view host,
      std::string_view target, ad_utility::SharedCancellationHandle handle,
      std::string_view requestBody = "",
      std::string_view contentTypeHeader = "text/plain",
      std::string_view acceptHeader = "text/plain");

  // Simple way to establish a websocket connection
  boost::beast::http::response<boost::beast::http::string_body>
  sendWebSocketHandshake(const boost::beast::http::verb& method,
                         std::string_view host, std::string_view target);

 private:
  // The connection stream and associated objects. See the implementation of
  // `openStream` for why we need all of them, and not just `stream_`.
  boost::asio::io_context ioContext_;
  // For some reason this work guard is required when no threads are attached
  // immediately.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      workGuard_ = boost::asio::make_work_guard(ioContext_);
  std::unique_ptr<boost::asio::ssl::context> ssl_context_;
  std::unique_ptr<StreamType> stream_;
};

// Instantiation for HTTP.
using HttpClient = HttpClientImpl<boost::beast::tcp_stream>;

// Instantiation for HTTPS.
using HttpsClient =
    HttpClientImpl<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>>;

// Global convenience function for sending a request (default: GET) to the given
// URL and obtaining the result as a `cppcoro::generator<std::span<std::byte>>`.
// The protocol (HTTP or HTTPS) is chosen automatically based on the URL. The
// `requestBody` is the payload sent for POST requests (default: empty).
cppcoro::generator<std::span<std::byte>> sendHttpOrHttpsRequest(
    const ad_utility::httpUtils::Url& url,
    ad_utility::SharedCancellationHandle handle,
    const boost::beast::http::verb& method = boost::beast::http::verb::get,
    std::string_view postData = "",
    std::string_view contentTypeHeader = "text/plain",
    std::string_view acceptHeader = "text/plain");
