// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_SERVERTESTHELPERS_H_
#define QLEVER_TEST_SERVERTESTHELPERS_H_

#include <boost/beast/http.hpp>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>

#include "engine/Server.h"
#include "libqlever/Qlever.h"
#include "util/IndexTestHelpers.h"

namespace serverTestHelpers {

namespace http = boost::beast::http;

using ReqT = http::request<http::string_body>;
using ResT = http::response<ad_utility::httpUtils::streamable_body>;

// Convert the body of an `http::response` into a string. Free function so it
// can be used by both `PersistentTestServer` and the backwards-compatible
// `SimulateHttpRequest`.
inline std::string responseBodyToString(
    ad_utility::httpUtils::streamable_body::value_type body) {
  // The range overload doesn't work because it takes a const Range& but
  // begin/end on the generator are not const. absl::StrJoin furthermore also
  // only accepts common iterators.
  auto respWithCommonIterators = body | ql::views::common;
  return absl::StrJoin(respWithCommonIterators.begin(),
                       respWithCommonIterators.end(), "");
}

// Test the HTTP request processing of the `Server` class using a persistent
// `Server` instance. In contrast to `SimulateHttpRequest`, the underlying
// `Server` lives for the whole lifetime of this object. This allows executing
// multiple operations (e.g. a `SELECT` after an `UPDATE`) or inspecting the
// state of the `Server` and `DeltaTriples` after the request.
class PersistentTestServer {
  std::unique_ptr<Server> server_;

 public:
  explicit PersistentTestServer(size_t numThreads, std::string accessToken,
                                const qlever::EngineConfig& config,
                                bool noAccessCheck = false)
      : server_{std::make_unique<Server>(
            4321, numThreads, std::move(accessToken), config, noAccessCheck)} {}

  // Accessors for the `Server` and `DeltaTriples`.
  Server& server() { return *server_; }
  const Server& server() const { return *server_; }

  // Access the `DeltaTriplesManager` of the underlying `Server`, e.g. to
  // inspect the delta triples after an `INSERT DATA`/`DELETE DATA` update.
  DeltaTriplesManager& deltaTriplesManager() {
    return server_->indexAndViewsSnapshot()->index_.deltaTriplesManager();
  }
  const DeltaTriplesManager& deltaTriplesManager() const {
    return server_->indexAndViewsSnapshot()->index_.deltaTriplesManager();
  }

  // Forwards to `Server::configureQueryEventLog`.
  void configureQueryEventLog(const std::filesystem::path& path) {
    server_->configureQueryEventLog(path);
  }

  static std::string bodyToString(
      ad_utility::httpUtils::streamable_body::value_type body) {
    return responseBodyToString(std::move(body));
  }

  // Apply `Server::process` on the given request and return the raw
  // `http::response`. A fresh `io_context` and `QueryHub` are created per
  // request, but the `Server` itself persists across calls.
  ResT processRaw(const ReqT& request) {
    boost::asio::io_context io;
    // NOTE: `request`, `io`, and the `Server` are passed as arguments to the
    // coroutine (not captured), because references captured in a lambda
    // coroutine dangle after the first suspension point.
    std::future<ResT> fut = co_spawn(
        io,
        [](auto request, Server* server,
           auto& io) -> boost::asio::awaitable<ResT> {
          auto queryHub = std::make_shared<ad_utility::websocket::QueryHub>(io);
          server->queryHub_ = queryHub;

          auto result =
              co_await server
                  ->template onlyForTestingProcess<decltype(request), ResT>(
                      request);
          co_return result;
        }(request, server_.get(), io),
        boost::asio::use_future);
    io.run();
    return fut.get();
  }

  // Given an HTTP request, apply the `Server::process` method on this request
  // and if the response is a JSON, parse and return it. Otherwise
  // `std::nullopt` is returned.
  std::optional<nlohmann::json> operator()(const ReqT& request) {
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

    return std::optional{
        nlohmann::json::parse(bodyToString(std::move(response.body())))};
  }

  // Apply `Server::process` on the given request and return the body of the
  // response as a string.
  std::string processAsString(const ReqT& request) {
    auto response = processRaw(request);
    return bodyToString(std::move(response.body()));
  }
};

// Helper function creating a simple config for testing.
inline qlever::EngineConfig getDefaultConfig() {
  auto qec = ad_utility::testing::getQec("<a> <b> <c>");
  qlever::EngineConfig config;
  config.baseName_ = qec->getIndex().getOnDiskBase();
  config.memoryLimit_ = ad_utility::MemorySize::gigabytes(1);
  return config;
}

// Helper function creating a config for testing with the given base name.
inline qlever::EngineConfig getDefaultConfigWithName(std::string baseName) {
  auto config = getDefaultConfig();
  config.baseName_ = std::move(baseName);
  return config;
}

// Test the HTTP request processing of the `Server` class. Each call creates a
// fresh `Server` on the given test index, runs a single request, and discards
// it. For multiple requests that share state or for inspecting the server state
// afterwards, use `PersistentTestServer` instead.
struct SimulateHttpRequest {
  std::string indexBaseName_;
  // Optional: write the server's query start/end events to this file so a test
  // can read them back. Empty leaves the event log unconfigured.
  std::optional<std::filesystem::path> eventLogPath_ = std::nullopt;

  static std::string bodyToString(
      ad_utility::httpUtils::streamable_body::value_type body) {
    return responseBodyToString(std::move(body));
  }

  // Build a throwaway `PersistentTestServer`.
  PersistentTestServer makeServer() const {
    auto config = getDefaultConfigWithName(indexBaseName_);
    config.persistUpdates_ = false;
    PersistentTestServer server{1, "accessToken", config, false};
    if (eventLogPath_.has_value()) {
      server.configureQueryEventLog(*eventLogPath_);
    }
    return server;
  }

  // Apply `Server::process` on the given request and return the raw
  // `http::response`.
  ResT processRaw(const ReqT& request) const {
    return makeServer().processRaw(request);
  }

  // Given an HTTP request, apply the `Server::process` method on this request
  // and if the response is a JSON, parse and return it. Otherwise
  // `std::nullopt` is returned.
  std::optional<nlohmann::json> operator()(const ReqT& request) const {
    return makeServer()(request);
  }

  // Apply `Server::process` on the given request and return the body of the
  // response as a string.
  std::string processAsString(const ReqT& request) const {
    return makeServer().processAsString(request);
  }
};

}  // namespace serverTestHelpers

#endif  // QLEVER_TEST_SERVERTESTHELPERS_H_
