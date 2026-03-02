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
using ResT = http::response<ad_utility::httpUtils::streamable_body>;

// Test the HTTP request processing of the `Server` class.
struct SimulateHttpRequest {
  std::string indexBaseName_;

  static std::string bodyToString(
      ad_utility::httpUtils::streamable_body::value_type body) {
    // The range overload doesn't work because it takes a const Range& but
    // begin/end on the generator are not const. absl::StrJoin furthermore also
    // only accepts common iterators.
    auto respWithCommonIterators = body | ql::views::common;
    return absl::StrJoin(respWithCommonIterators.begin(),
                         respWithCommonIterators.end(), "");
  }

  // Apply `Server::process` on the given request and return the raw
  // `http::response`.
  ResT processRaw(const ReqT& request) const {
    boost::asio::io_context io;
    std::future<ResT> fut = co_spawn(
        io,
        [&io](auto request, auto indexName) -> boost::asio::awaitable<ResT> {
          // Initialize but do not start a `Server` instance on our test index.
          Server server{4321, 1, ad_utility::MemorySize::megabytes(1),
                        "accessToken"};
          server.initialize(indexName, false);
          auto queryHub = std::make_shared<ad_utility::websocket::QueryHub>(io);
          server.queryHub_ = queryHub;

          // Simulate receiving the HTTP request.
          auto result =
              co_await server
                  .template onlyForTestingProcess<decltype(request), ResT>(
                      request);
          co_return result;
        }(request, indexBaseName_),
        boost::asio::use_future);
    io.run();
    return fut.get();
  }

  // Given an HTTP request, apply the `Server::process` method on this request
  // and if the response is a JSON, parse and return it. Otherwise
  // `std::nullopt` is returned.
  std::optional<nlohmann::json> operator()(const ReqT& request) const {
    auto response = processRaw(request);

    // Check `Content-type`: currently only `application/json` is supported.
    auto it = response.find(http::field::content_type);
    if (it != response.end()) {
      // We check `starts_with` instead of `==` because a `charset=utf-8` could
      // follow.
      if (!it->value().starts_with("application/json")) {
        return std::nullopt;
      }
    }

    // Parse the JSON body.
    return std::optional{
        nlohmann::json::parse(bodyToString(std::move(response.body())))};
  }

  // Apply `Server::process` on the given request and return the body of the
  // response as a string.
  std::string processAsString(const ReqT& request) const {
    auto response = processRaw(request);
    return bodyToString(std::move(response.body()));
  }
};

}  // namespace serverTestHelpers

#endif  // QLEVER_TEST_SERVERTESTHELPERS_H_
