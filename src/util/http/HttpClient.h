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

#include <util/http/beast.h>

#include <sstream>
#include <string>

// A class for basic communication with a remote server via HTTP or HTTPS. For
// now, contains functionality for setting up a connection, sending one or
// several GET or POST requests (and getting the response), and closing the
// connection.
template <typename StreamType>
class HttpClientImpl {
 public:
  // Set up the connection to the client. The `StreamType` determines whether it
  // is a HTTP or an HTTPS connection, see the two instantiations `HttpCLient`
  // and `HttpsClient` below.
  void openStream(const std::string& host, const std::string& port);

  // Send a request (the first argument must be either `http::verb::get` or
  // `http::verb::post`) and return the body of the reponse (possibly very
  // large) as an `std::istringstream`. The same connection can be used for
  // multiple requests in a row.
  //
  // TODO: Read and process the response in chunks. Here is a code example:
  // https://stackoverflow.com/questions/69011767/handling-large-http-response-using-boostbeast
  std::istringstream sendRequest(
      const boost::beast::http::verb& method, const std::string& host,
      const std::string& target, const std::string& requestBody = "",
      const std::string& contentTypeHeader = "text/plain",
      const std::string& acceptHeader = "text/plain");

  // Close the stream.
  void closeStream();

 private:
  // The connection stream and associated objects. See the implementation of
  // `openStream` for why we need all of them, and not just `stream_`.
  boost::asio::io_context io_context_;
  std::unique_ptr<boost::asio::ssl::context> ssl_context_;
  std::unique_ptr<StreamType> stream_;
};

// Instantiation for HTTP.
using HttpClient = HttpClientImpl<boost::beast::tcp_stream>;

// Instantiation for HTTPS.
using HttpsClient =
    HttpClientImpl<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>>;
