// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_SERVERTESTHELPERS_H_
#define QLEVER_TEST_SERVERTESTHELPERS_H_

#include <boost/beast/http.hpp>

#include "engine/Server.h"

namespace serverTestHelpers {

namespace http = boost::beast::http;

using ReqT = http::request<http::string_body>;
using ResT = std::optional<http::response<http::string_body>>;

// Test the HTTP request processing of the `Server` class.
struct SimulateHttpRequest {
  std::string indexBaseName_;

  // Given an HTTP request, apply the `Server::process` method on this request
  // and if the response is a non-streamed JSON, parse and return it. Otherwise
  // `std::nullopt` is returned.
  std::optional<nlohmann::json> operator()(const ReqT& request) const {
    boost::asio::io_context io;
    std::future<ResT> fut = co_spawn(
        io,
        [](auto request, auto indexName) -> boost::asio::awaitable<ResT> {
          // Initialize but do not start a `Server` instance on our test index.
          Server server{4321, 1, ad_utility::MemorySize::megabytes(1),
                        "accessToken"};
          server.initialize(indexName, false);

          // Simulate receiving the HTTP request.
          auto result =
              co_await server
                  .template onlyForTestingProcess<decltype(request), ResT>(
                      request);
          co_return result;
        }(request, indexBaseName_),
        boost::asio::use_future);
    io.run();
    auto response = fut.get();
    if (!response.has_value()) {
      return std::nullopt;
    }

    // Check `Content-type`: currently only `application/json` is supported.
    auto it = response.value().find(http::field::content_type);
    if (it != response.value().end()) {
      // We check `starts_with` instead of `==` because a `charset=utf-8` could
      // follow.
      if (!it->value().starts_with("application/json")) {
        return std::nullopt;
      }
    }

    // Parse the JSON body.
    return std::optional{nlohmann::json::parse(response.value().body())};
  };
};

}  // namespace serverTestHelpers

#endif  // QLEVER_TEST_SERVERTESTHELPERS_H_
