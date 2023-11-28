//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_HTTPUTILS_H
#define QLEVER_HTTPUTILS_H

#include <string>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "util/AsyncStream.h"
#include "util/CompressorStream.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"
#include "util/http/beast.h"
#include "util/http/streamable_body.h"
#include "util/json.h"
#include "util/stream_generator.h"

/// Several utilities for using/customizing the HttpServer template from
/// HttpServer.h

namespace ad_utility::httpUtils {
namespace beast = boost::beast;    // from <boost/beast.hpp>
namespace http = beast::http;      // from <boost/beast/http.hpp>
namespace net = boost::asio;       // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;  // from <boost/asio/ip/tcp.hpp>
namespace streams = ad_utility::streams;
using ad_utility::httpUtils::httpStreams::streamable_body;

// Simple URL class that parses the components from a given URL string and
// provides "getters" for them (some of the components are implicit, such as the
// port, so these are not really getters). For example, the components of the
// URL https://qlever.cs.uni-freiburg.de/api/wikidata are:
//
// protocol: HTTPS
// host:     qlever.cs.uni-freiburg.de
// port:     443 (implicit)
// target:   /api/wikidata .
//
class Url {
 public:
  enum class Protocol { HTTP, HTTPS };

 private:
  Protocol protocol_;
  std::string host_;
  std::string port_;
  std::string target_;

 public:
  // Construct from given URL.
  explicit Url(std::string_view url);
  // The protocol: one of Protocol::HTTP or Protocol::HTTPS.
  Protocol protocol() const { return protocol_; }
  // The host; this is always a substring of the given URL.
  const std::string& host() const { return host_; }
  // The port; inferred from the protocol if not specified explicitly (80 for
  // HTTP, 443 for HTTPS).
  const std::string& port() const { return port_; }
  // The target; this is a substring of the given URL, except when it's empty in
  // the URL, then it's "/".
  const std::string& target() const { return target_; }
  // The protocol as string.
  std::string_view protocolAsString() const {
    return protocol_ == Protocol::HTTP ? "http" : "https";
  }
  // The whole URL as a string again (with explicit port).
  std::string asString() const {
    return absl::StrCat(protocolAsString(), "://", host_, ":", port_, target_);
  }
};

// A concept for http::request
template <typename T>
static constexpr bool isHttpRequest = false;

template <typename Body, typename Fields>
static constexpr bool isHttpRequest<http::request<Body, Fields>> = true;

template <typename T>
concept HttpRequest = isHttpRequest<T>;

/**
 * @brief Create a http::response from a string, which will become the body
 * @param body The body of the response
 * @param status The http status.
 * @param request The request to which the response belongs.
 * @param mediaType The media type of the response.
 * @return A http::response<http::string_body> which is ready to be sent.
 */
auto createHttpResponseFromString(std::string body, http::status status,
                                  const HttpRequest auto& request,
                                  MediaType mediaType) {
  http::response<http::string_body> response{status, request.version()};
  response.set(http::field::content_type, toString(mediaType));
  response.keep_alive(request.keep_alive());
  response.body() = std::move(body);
  // Set Content-Length and Transfer-Encoding.
  response.prepare_payload();
  return response;
}

/// Create a HttpResponse from a string with status 200 OK. Otherwise behaves
/// the same as createHttpResponseFromString.
static auto createOkResponse(std::string text, const HttpRequest auto& request,
                             MediaType mediaType) {
  return createHttpResponseFromString(std::move(text), http::status::ok,
                                      request, mediaType);
}

/// Assign the generator to the body of the response. If a supported
/// compression is specified in the request, this method is applied to the
/// body and the corresponding response headers are set.
static void setBody(http::response<streamable_body>& response,
                    const HttpRequest auto& request,
                    streams::stream_generator&& generator) {
  using ad_utility::content_encoding::CompressionMethod;

  CompressionMethod method =
      ad_utility::content_encoding::getCompressionMethodForRequest(request);
  auto asyncGenerator = streams::runStreamAsync(std::move(generator), 100);
  if (method != CompressionMethod::NONE) {
    response.body() =
        streams::compressStream(std::move(asyncGenerator), method);
    ad_utility::content_encoding::setContentEncodingHeaderForCompressionMethod(
        method, response);
  } else {
    response.body() = std::move(asyncGenerator);
  }
}

/// Create a HttpResponse from a stream_generator with status 200 OK.
static auto createOkResponse(ad_utility::streams::stream_generator&& generator,
                             const HttpRequest auto& request,
                             MediaType mediaType) {
  http::response<streamable_body> response{http::status::ok, request.version()};
  response.set(http::field::content_type, toString(mediaType));
  response.keep_alive(request.keep_alive());
  setBody(response, request, std::move(generator));
  // Set Content-Length and Transfer-Encoding.
  // Because ad_utility::httpUtils::httpStreams::streamable_body::size
  // is not defined, Content-Length will be cleared and Transfer-Encoding
  // will be set to chunked
  response.prepare_payload();
  return response;
}

/// Create a HttpResponse from a string with status 200 OK and mime type
/// "application/json". Otherwise behaves the same as
/// createHttpResponseFromString.
static auto createJsonResponse(std::string text, const auto& request,
                               http::status status = http::status::ok) {
  return createHttpResponseFromString(std::move(text), status, request,
                                      MediaType::json);
}

/// Create a HttpResponse from a json object with status 200 OK and mime type
/// "application/json".
static auto createJsonResponse(const nlohmann::json& j, const auto& request,
                               http::status status = http::status::ok) {
  // Argument `4` leads to a human-readable indentation.
  return createJsonResponse(j.dump(4), request, status);
}

/// Create a HttpResponse with status 404 Not Found. The string body will be a
/// default message including the name of the file that was not found, which
/// can be read from the request directly.
static auto createNotFoundResponse(const std::string& errorMsg,
                                   const HttpRequest auto& request) {
  return createHttpResponseFromString(errorMsg, http::status::not_found,
                                      request, MediaType::textPlain);
}

/// Create a HttpResponse with status 400 Bad Request.
static auto createBadRequestResponse(std::string body,
                                     const HttpRequest auto& request) {
  return createHttpResponseFromString(std::move(body),
                                      http::status::bad_request, request,
                                      MediaType::textPlain);
}
}  // namespace ad_utility::httpUtils

#endif  // QLEVER_HTTPUTILS_H
