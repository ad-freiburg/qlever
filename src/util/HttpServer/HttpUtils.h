//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_HTTPUTILS_H
#define QLEVER_HTTPUTILS_H

#include "../StringUtils.h"
#include "../TypeTraits.h"
#include "./MediaTypes.h"
#include "./UrlParser.h"
#include "./beast.h"

/// Several utilities for using/customizing the HttpServer template from
/// HttpServer.h

namespace ad_utility::httpUtils {
namespace beast = boost::beast;    // from <boost/beast.hpp>
namespace http = beast::http;      // from <boost/beast/http.hpp>
namespace net = boost::asio;       // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;  // from <boost/asio/ip/tcp.hpp>

/// Concatenate base and path. Path must start with a '/', base may end with a
/// slash. For example, path_cat("base", "/file.txt"), path_cat("base/" ,
/// "/file.txt") both lead to "base/file.txt". Taken from the beast examples.
inline std::string path_cat(beast::string_view base, beast::string_view path) {
  if (base.empty()) return std::string(path);
  std::string result(base);
  AD_CHECK(path.starts_with('/'));
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
                             MediaType mimeType) {
  return createHttpResponseFromString(std::move(text), http::status::ok,
                                      request, mimeType);
}

/// Create a HttpResponse from a string with status 200 OK and mime type
/// "application/json". Otherwise behaves the same as
/// createHttpResponseFromString.
static auto createJsonResponse(std::string text, const auto& request) {
  return createOkResponse(std::move(text), request, MediaType::json);
}

/// Create a HttpResponse from a json object with status 200 OK and mime type
/// "application/json".
static auto createJsonResponse(const json& j, const auto& request) {
  // Argument `4` leads to a human-readable indentation.
  return createJsonResponse(j.dump(4), request);
}

/// Create a HttpResponse with status 404 Not Found. The string body will be a
/// default message including the name of the file that was not found, which can
/// be read from the request directly.
static auto createNotFoundResponse(const HttpRequest auto& request) {
  return createHttpResponseFromString("Resource \"" +
                                          std::string(request.target()) +
                                          "\" was not found on this server",
                                      http::status::not_found, request);
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

/**
 * @brief Return a lambda which satisfies the constraints of the `HttpHandler`
 * argument in the `HttpServer` class and serves files from a specified
 * documentRoot. A typical use is `HttpServer
 * httpServer{httpUtils::makeFileServer("path_to_some_directory")};`
 * @param documentRoot The path from which files are served. May be absolute or
 * relative.
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
    // Make sure we can handle the method
    if (request.method() != http::verb::get &&
        request.method() != http::verb::head) {
      co_await send(createBadRequestResponse(
          "Unknown HTTP-method, only GET and HEAD requests are supported",
          request));
      co_return;
    }

    // Decode the path and check that it is absolute and contains no "..".
    auto urlPath =
        ad_utility::UrlParser::getDecodedPathAndCheck(request.target());
    if (!urlPath.has_value()) {
      co_await send(createBadRequestResponse(
          "Invalid url path \"" + std::string{request.target()} + '"',
          request));
      co_return;
    }

    // Check if the target is in the whitelist. The `target()` starts with a
    // slash, entries in the whitelist don't.
    if (whitelist.has_value() &&
        !whitelist.value().contains(urlPath.value().substr(1))) {
      co_await send(createNotFoundResponse(request));
      co_return;
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
      co_await send(createNotFoundResponse(request));
      co_return;
    }

    // Handle an unknown error.
    if (errorCode) {
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
  };
}

};  // namespace ad_utility::httpUtils

#endif  // QLEVER_HTTPUTILS_H
