// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_SERVERTESTHELPERS_H_
#define QLEVER_TEST_SERVERTESTHELPERS_H_

#include <boost/beast/http.hpp>
#include <optional>
#include <string>
#include <utility>

#include "backports/filesystem.h"
#include "engine/Server.h"
#include "libqlever/Qlever.h"
#include "util/IndexTestHelpers.h"
#include "util/metrics/Metrics.h"

namespace serverTestHelpers {

namespace http = boost::beast::http;

using ReqT = http::request<http::string_body>;
using ResT = http::response<ad_utility::httpUtils::streamable_body>;

// Convert the body of an `http::response` into a string.
inline std::string responseBodyToString(
    ad_utility::httpUtils::streamable_body::value_type body) {
  // The range overload doesn't work because it takes a const Range& but
  // begin/end on the generator are not const. absl::StrJoin furthermore also
  // only accepts common iterators.
  auto respWithCommonIterators = body | ql::views::common;
  return absl::StrJoin(respWithCommonIterators.begin(),
                       respWithCommonIterators.end(), "");
}

// Test the HTTP request processing of the `Server` class. The underlying
// `Server` lives for the whole lifetime of this object, so multiple operations
// can be executed against the same server (e.g. a `SELECT` after an `UPDATE`),
// and the state of the `Server` and `DeltaTriples` can be inspected after each
// request.
class ServerForTesting {
  std::unique_ptr<Server> server_;

 public:
  explicit ServerForTesting(size_t numThreads, std::string accessToken,
                            const qlever::EngineConfig& config,
                            bool noAccessCheck = false,
                            std::shared_ptr<ad_utility::metrics::MetricsReader>
                                metricsReader = nullptr)
      : server_{std::make_unique<Server>(4321, numThreads,
                                         std::move(accessToken), config,
                                         noAccessCheck, metricsReader)} {}

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
  void configureQueryEventLog(const ql::filesystem::path& path) {
    server_->configureQueryEventLog(path);
  }

  // Apply `Server::process` on the given request and return the
  // `http::response`. A fresh `io_context` and `QueryHub` are created per
  // request, but the `Server` itself is reused across calls.
  ResT process(const ReqT& request) {
    boost::asio::io_context io;
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
};

// If the given response is a JSON (according to its `Content-type` header),
// parse its body and return it. Otherwise return `std::nullopt`.
inline std::optional<nlohmann::json> responseBodyAsJson(ResT response) {
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
      nlohmann::json::parse(responseBodyToString(std::move(response.body())))};
}

// Helper function creating a config for testing with the given base name.
inline qlever::EngineConfig getDefaultConfigWithName(std::string baseName) {
  qlever::EngineConfig config;
  config.baseName_ = std::move(baseName);
  config.memoryLimit_ = ad_utility::MemorySize::gigabytes(1);
  // Never persist updates to disk in tests (would leave files behind after the
  // test). Tests that explicitly test the persistence can override this.
  config.persistUpdates_ = false;
  return config;
}

// Helper function creating a simple config for testing.
inline qlever::EngineConfig getDefaultConfig() {
  auto qec = ad_utility::testing::getQec("<a> <b> <c>");
  return getDefaultConfigWithName(qec->getIndex().getOnDiskBase());
}

// Create a `ServerForTesting` on the test index with the given `baseName`.
// If `eventLogPath` is given, the server's query start/end events are written
// to that file.
inline ServerForTesting makeServerForTesting(
    std::string baseName,
    std::optional<ql::filesystem::path> eventLogPath = std::nullopt) {
  ServerForTesting server{1, "accessToken",
                          getDefaultConfigWithName(std::move(baseName))};
  if (eventLogPath.has_value()) {
    server.configureQueryEventLog(*eventLogPath);
  }
  return server;
}

}  // namespace serverTestHelpers

#endif  // QLEVER_TEST_SERVERTESTHELPERS_H_
