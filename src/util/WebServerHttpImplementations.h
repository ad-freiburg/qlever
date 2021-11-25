//
// Created by projector-user on 11/23/21.
//

#ifndef QLEVER_WEBSERVERHTTPIMPLEMENTATIONS_H
#define QLEVER_WEBSERVERHTTPIMPLEMENTATIONS_H

#include <re2/re2.h>
namespace beast = boost::beast;         // from <boost/beast.hpp>
namespace http = beast::http;           // from <boost/beast/http.hpp>
namespace net = boost::asio;            // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>

class HttpUtils {
 public:

  enum class MimeType {
    textHtml,
    json,
    tsv,
    csv
  };
  static std::string toString(MimeType t) {
    switch (t) {
      case MimeType::textHtml : return "text/html";
      case MimeType::json : return "application/json";
      case MimeType::tsv: return "text/tsv";
      case MimeType::csv: return "text/csv";
    }
  }
                                                    // Append an HTTP rel-path to a local filesystem path.
                                                    // The returned path is normalized for the platform.
                                                    static std::string
                                                    path_cat(
                                                        beast::string_view base,
                                                        beast::string_view path)
  {
    if(base.empty())
      return std::string(path);
    std::string result(base);
#ifdef BOOST_MSVC
    char constexpr path_separator = '\\';
    if(result.back() == path_separator)
      result.resize(result.size() - 1);
    result.append(path.data(), path.size());
    for(auto& c : result)
      if(c == '/')
        c = path_separator;
#else
    char constexpr path_separator = '/';
    if(result.back() == path_separator)
      result.resize(result.size() - 1);
    result.append(path.data(), path.size());
#endif
    return result;
  }


  // Return a reasonable mime type based on the extension of a file.
  static beast::string_view
  getMimeTypeFromFilename(beast::string_view path)
  {
    using beast::iequals;
    auto const ext = [&path]
    {
      auto const pos = path.rfind(".");
      if(pos == beast::string_view::npos)
        return beast::string_view{};
      return path.substr(pos);
    }();
    if(iequals(ext, ".htm"))  return "text/html";
    if(iequals(ext, ".html")) return "text/html";
    if(iequals(ext, ".php"))  return "text/html";
    if(iequals(ext, ".css"))  return "text/css";
    if(iequals(ext, ".txt"))  return "text/plain";
    if(iequals(ext, ".js"))   return "application/javascript";
    if(iequals(ext, ".json")) return "application/json";
    if(iequals(ext, ".xml"))  return "application/xml";
    if(iequals(ext, ".swf"))  return "application/x-shockwave-flash";
    if(iequals(ext, ".flv"))  return "video/x-flv";
    if(iequals(ext, ".png"))  return "image/png";
    if(iequals(ext, ".jpe"))  return "image/jpeg";
    if(iequals(ext, ".jpeg")) return "image/jpeg";
    if(iequals(ext, ".jpg"))  return "image/jpeg";
    if(iequals(ext, ".gif"))  return "image/gif";
    if(iequals(ext, ".bmp"))  return "image/bmp";
    if(iequals(ext, ".ico"))  return "image/vnd.microsoft.icon";
    if(iequals(ext, ".tiff")) return "image/tiff";
    if(iequals(ext, ".tif"))  return "image/tiff";
    if(iequals(ext, ".svg"))  return "image/svg+xml";
    if(iequals(ext, ".svgz")) return "image/svg+xml";
    return "application/text";
  }


  static auto createResponse(std::string text, http::status status, const auto& request, MimeType mimeType = MimeType::textHtml) {
    http::response<http::string_body> response{status, request.version()};
    response.set(http::field::content_type, toString(mimeType));
    response.keep_alive(request.keep_alive());
    response.body() = std::move(text);
    response.prepare_payload();
    return response;
  }

  static auto createOkResponse(std::string text, const auto& request, MimeType mimeType) {
    return createResponse(std::move(text), http::status::ok, request, mimeType);
  }

  static auto createJsonResponse(std::string text, const auto& request) {
    return createOkResponse(std::move(text), request, MimeType::json);
  }

  static auto createNotFoundResponse(const auto& request) {
    return createResponse("Resource \"" + std::string(request.target()) + "\" was not found on this server", http::status::not_found, request);
  }

  static auto createBadRequestResponse(std::string message, const auto& request) {
    return createResponse(std::move(message), http::status::bad_request, request);
  }

  static auto createServerErrorResponse(std::string message, const auto& request) {
    return createResponse(std::move(message), http::status::internal_server_error, request);
  }

  static auto createHeadResponse(size_t sizeOfFile, const string& path, const auto& request) {
  http::response<http::empty_body> res{http::status::ok, request.version()};
  res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
  res.set(http::field::content_type, getMimeTypeFromFilename(path));
  res.content_length(sizeOfFile);
  res.keep_alive(request.keep_alive());
  return res;
  }

  static auto createGetResponse(http::file_body::value_type&& body, const string& path, const auto& request) {
    auto sizeOfFile = body.size();
    // Respond to GET request
    http::response<http::file_body> res{
        std::piecewise_construct,
        std::make_tuple(std::move(body)),
        std::make_tuple(http::status::ok, request.version())};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, getMimeTypeFromFilename(path));
    res.content_length(sizeOfFile);
    res.keep_alive(request.keep_alive());
    return res;
  }


  // ______________________________________________________________________
  static void
  fail(beast::error_code ec, char const* what)
  {
    LOG(ERROR) << what << ": " << ec.message() << "\n";
  }

 public:
  // This function produces an HTTP response for the given
  // request. The type of the response object depends on the
  // contents of the request, so the interface requires the
  // caller to pass a generic lambda for receiving the response.
  static auto makeFileServer(std::string documentRoot) {
    return [documentRoot=std::move(documentRoot)]<typename Body, typename Allocator, typename Send>
     (
        http::request<Body, http::basic_fields<Allocator>> && req,
        Send && send) -> boost::asio::awaitable<void> {
      // Make sure we can handle the method
      if (req.method() != http::verb::get && req.method() != http::verb::head) {
        co_await send(createBadRequestResponse("Unknown HTTP-method", req));
        co_return;
      }

      // Request path must be absolute and not contain "..".
      if (req.target().empty() || req.target()[0] != '/' ||
          req.target().find("..") != beast::string_view::npos) {
        co_await send(createBadRequestResponse("Illegal request-target", req));
        co_return;
      }

      // Build the path to the requested file
      std::string path = path_cat(documentRoot, req.target());
      if (req.target().back() == '/') path.append("index.html");

      // Attempt to open the file
      beast::error_code ec;
      http::file_body::value_type body;
      body.open(path.c_str(), beast::file_mode::scan, ec);

      // Handle the case where the file doesn't exist
      if (ec == beast::errc::no_such_file_or_directory) {
        co_await send(createNotFoundResponse(req));
        co_return;
      }

      // Handle an unknown error
      if (ec) {
        co_return co_await send(createServerErrorResponse(ec.message(), req));
      }

      // Respond to HEAD request
      if (req.method() == http::verb::head) {
        co_return co_await send(createHeadResponse(body.size(), path, req));
      }
      // Respond to GET request
      co_return co_await send(createGetResponse(std::move(body), path, req));
    };
  }


};

#endif  // QLEVER_WEBSERVERHTTPIMPLEMENTATIONS_H
