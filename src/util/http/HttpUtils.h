//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_HTTPUTILS_H
#define QLEVER_HTTPUTILS_H

#include <string>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "nlohmann/json.hpp"
#include "util/AsyncStream.h"
#include "util/CompressorStream.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"
#include "util/http/MediaTypes.h"
#include "util/http/UrlParser.h"
#include "util/http/beast.h"
#include "util/http/streamable_body.h"
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

/// The components of a URL. For example, the components of the URL
/// https://qlever.cs.uni-freiburg.de/api/wikidata are:
///
/// protocol: HTTPS
/// host:     qlever.cs.uni-freiburg.de
/// port:     443 (implicit)
/// target:   /api/wikidata .
///
/// NOTE: `host` and `target` could be `std::string_view` because they are parts
/// of the given URL. However, `port` can be implicit, so we need a
/// `std::string` here (and it's not an `int` because the Beast functions ask
/// for the port as a string). Since URLs are short and we do not handle large
/// numbers of URLs, the overhead of the string copies are negligible.
struct UrlComponents {
  // Construct from given URL.
  UrlComponents(const std::string_view url);
  // Members.
  enum Protocol { HTTP, HTTPS } protocol;
  std::string host;
  std::string port;
  std::string target;
  // For testing.
  friend std::ostream& operator<<(std::ostream& os, const UrlComponents& uc) {
    return os << "UrlComponents("
              << (uc.protocol == Protocol::HTTP ? "http" : "https") << ", "
              << uc.host << ", " << uc.port << ", " << uc.target << ")";
  }
};

/// Concatenate base and path. Path must start with a '/', base may end with a
/// slash. For example, path_cat("base", "/file.txt"), path_cat("base/" ,
/// "/file.txt") both lead to "base/file.txt". Taken from the beast examples.
inline std::string path_cat(beast::string_view base, beast::string_view path) {
  if (base.empty()) return std::string(path);
  std::string result(base);
  AD_CONTRACT_CHECK(path.starts_with('/'));
  char constexpr path_separator = '/';
  if (result.back() == path_separator) result.resize(result.size() - 1);
  result.append(path.data(), path.size());
  return result;
}

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
                                  MediaType mediaType = MediaType::html) {
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
                                      request);
}

/// Create a HttpResponse with status 400 Bad Request.
static auto createBadRequestResponse(std::string body,
                                     const HttpRequest auto& request) {
  return createHttpResponseFromString(std::move(body),
                                      http::status::bad_request, request);
}

/// Create a HttpResponse with status 500 Bad Request.
static auto createServerErrorResponse(std::string message,
                                      const HttpRequest auto& request) {
  return createHttpResponseFromString(
      std::move(message), http::status::internal_server_error, request);
}

/// Create a HttpResponse for a HTTP HEAD request for a file from the
/// size of the file and the path to the file.
static auto createHeadResponse(size_t sizeOfFile, const string& path,
                               const HttpRequest auto& request) {
  http::response<http::empty_body> response{http::status::ok,
                                            request.version()};
  response.set(http::field::server, BOOST_BEAST_VERSION_STRING);
  response.set(http::field::content_type, mediaTypeForFilename(path));
  response.content_length(sizeOfFile);
  response.keep_alive(request.keep_alive());
  return response;
}

/// Create a HttpResponse for a HTTP GET request for a file.
static auto createGetResponseForFile(http::file_body::value_type&& body,
                                     const string& path,
                                     const HttpRequest auto& request) {
  auto sizeOfFile = body.size();
  // Respond to GET request
  http::response<http::file_body> response{
      std::piecewise_construct, std::make_tuple(std::move(body)),
      std::make_tuple(http::status::ok, request.version())};
  response.set(http::field::server, BOOST_BEAST_VERSION_STRING);
  response.set(http::field::content_type, mediaTypeForFilename(path));
  response.content_length(sizeOfFile);
  response.keep_alive(request.keep_alive());
  return response;
}

/// Log a beast::error_code together with an additional message.
inline void logBeastError(beast::error_code ec, char const* what) {
  LOG(ERROR) << what << ": " << ec.message() << "\n";
}

namespace detail {

// This coroutine is  part of the implementation of `makeFileServer` (see
// below). It is the actual coroutine that performs the serving of a file.
// Note: Because of the coroutine lifetime rules, the coroutine has to get the
// `documentRoot`,`whitelist`, `request` by value. According to the signature,
// the `send` action might be dangling, but when using this coroutine inside
// the `HttpServer` class template, it never is. The parameters `documentRoot`
// and `whitelist` are currently copied for every http request, if this
// becomes a performance problem, they might also become `shared_ptr`s.
boost::asio::awaitable<void> makeFileServerImpl(
    std::string documentRoot,
    std::optional<ad_utility::HashSet<std::string>> whitelist,
    HttpRequest auto request, auto&& send) {
  // Make sure we can handle the method
  if (request.method() != http::verb::get &&
      request.method() != http::verb::head) {
    throw std::runtime_error(
        "When serving files, only GET and HEAD requests are supported");
  }

  // Decode the path and check that it is absolute and contains no "..".
  auto urlPath =
      ad_utility::UrlParser::getDecodedPathAndCheck(request.target());
  if (!urlPath.has_value()) {
    throw std::runtime_error(
        absl::StrCat("Invalid URL path \"", request.target(), "\""));
  }

  // Check if the target is in the whitelist. The `target()` starts with a
  // slash, entries in the whitelist don't.
  auto urlPathWithFirstCharRemoved = urlPath.value().substr(1);
  if (whitelist.has_value() &&
      !whitelist.value().contains(urlPathWithFirstCharRemoved)) {
    throw std::runtime_error(absl::StrCat(
        "Resource \"", urlPathWithFirstCharRemoved, "\" not in whitelist"));
  }

  // Build the path to the requested file on the file system.
  std::string filesystemPath = path_cat(documentRoot, request.target());
  if (request.target().back() == '/') filesystemPath.append("index.html");

  // Attempt to open the file.
  beast::error_code errorCode;
  http::file_body::value_type body;
  body.open(filesystemPath.c_str(), beast::file_mode::scan, errorCode);

  // Handle the case where the file doesn't exist.
  if (errorCode == beast::errc::no_such_file_or_directory) {
    std::string errorMsg =
        absl::StrCat("Resource \"", request.target(), "\" not found");
    LOG(ERROR) << errorMsg << std::endl;
    co_return co_await send(createNotFoundResponse(errorMsg, request));
  }

  // Handle an unknown error.
  if (errorCode) {
    LOG(ERROR) << errorCode.message() << std::endl;
    co_return co_await send(
        createServerErrorResponse(errorCode.message(), request));
  }

  // Respond to HEAD request.
  if (request.method() == http::verb::head) {
    co_return co_await send(
        createHeadResponse(body.size(), filesystemPath, request));
  }
  // Respond to GET request.
  co_return co_await send(
      createGetResponseForFile(std::move(body), filesystemPath, request));
}
}  // namespace detail

/**
 * @brief Return a lambda which satisfies the constraints of the `HttpHandler`
 * argument in the `HttpServer` class and serves files from a specified
 * documentRoot. A typical use is `HttpServer
 * httpServer{httpUtils::makeFileServer("path_to_some_directory")};`
 * @param documentRoot The path from which files are served. May be absolute
 * or relative.
 * @param whitelist Specify a whitelist of allowed filenames (e.g.
 * `{"index.html", "style.css"}`). The default value std::nullopt means, that
 *         all files from the documentRoot may be served.
 */
inline auto makeFileServer(
    std::string documentRoot,
    std::optional<ad_utility::HashSet<std::string>> whitelist = std::nullopt) {
  // The empty path means "index.html", add this information to the whitelist.
  if (whitelist.has_value() && whitelist.value().contains("index.html")) {
    whitelist.value().insert("");
  }

  // Return a lambda that takes a HttpRequest and a Send function (see the
  // HttpServer class for details).
  // TODO<joka921> make the SendAction a concept.
  return [documentRoot = std::move(documentRoot),
          whitelist = std::move(whitelist)](
             HttpRequest auto request,
             auto&& send) -> boost::asio::awaitable<void> {
    // We need this extra indirection because the coroutine that is returned
    // might live longer than the lambda closure object which stores the
    // captures `documentRoot` and `whitelist`. For details see
    // https://devblogs.microsoft.com/oldnewthing/20211103-00/?p=105870 and
    // https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#Rcoro-capture
    return detail::makeFileServerImpl(documentRoot, whitelist,
                                      std::move(request), AD_FWD(send));
  };
}

}  // namespace ad_utility::httpUtils

#endif  // QLEVER_HTTPUTILS_H
