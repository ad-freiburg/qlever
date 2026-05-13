// Copyright 2025 The QLever Authors, in particular:
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <chrono>
#include <thread>

#include "backports/algorithm.h"
#include "index/InputFileServer.h"
#include "libqlever/Qlever.h"
#include "util/http/beast.h"
#include "util/jthread.h"

using namespace qlever;
using namespace testing;

namespace {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;
using ExtraHeaders = std::vector<std::pair<std::string, std::string>>;

// Return a free TCP port by binding to port 0 (which lets the OS pick a free
// port). There is a small race window between this function returning and the
// server binding to the same port.
unsigned short findFreePort() {
  net::io_context ioc;
  tcp::acceptor acceptor{ioc};
  tcp::endpoint ep{tcp::v4(), 0};
  acceptor.open(ep.protocol());
  acceptor.bind(ep);
  return acceptor.local_endpoint().port();
  // Acceptor destructor releases the port.
}

struct PostResponse {
  http::status status;
  std::string body;
};

// Synchronously POST `body` to localhost:`port`. When `contentType` is
// non-null, the Content-Type header is set; otherwise it is omitted. Any
// additional string-keyed headers can be passed via `extraHeaders`.
PostResponse syncPost(
    unsigned short port, std::string_view body,
    std::optional<std::string_view> contentType = std::nullopt,
    ExtraHeaders extraHeaders = {}) {
  net::io_context ioc;
  beast::tcp_stream stream{ioc};
  tcp::resolver resolver{ioc};
  auto const results = resolver.resolve("localhost", std::to_string(port));
  stream.connect(results);

  http::request<http::string_body> req{http::verb::post, "/", 11};
  req.set(http::field::host, "localhost");
  if (contentType.has_value()) {
    req.set(http::field::content_type, *contentType);
  }
  req.body() = std::string{body};
  for (auto& [name, value] : extraHeaders) {
    req.set(name, value);
  }
  req.prepare_payload();

  http::write(stream, req);

  beast::flat_buffer buffer;
  http::response<http::string_body> res;
  http::read(stream, buffer, res);

  beast::error_code ec;
  stream.socket().shutdown(tcp::socket::shutdown_both, ec);

  return {res.result(), res.body()};
}

// Poll the `InputFileServer` at localhost:`port` with `Can-Upload` probes
// (50 ms apart, up to `maxAttempts`) and return true as soon as the server
// replies. Returns false on timeout.
bool waitForServer(unsigned short port, int maxAttempts = 200) {
  for (int i = 0; i < maxAttempts; ++i) {
    try {
      auto r = syncPost(port, "", "text/plain", {{"Can-Upload", "1"}});
      if (r.status == http::status::ok ||
          r.status == http::status::too_many_requests) {
        return true;
      }
    } catch (...) {
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
  }
  return false;
}

// Send the `Finish-Index-Building: true` signal to localhost:`port`.
PostResponse sendFinish(unsigned short port) {
  return syncPost(port, "", std::nullopt, {{"Finish-Index-Building", "true"}});
}

// Fire a single `Can-Upload` probe to localhost:`port` and return the HTTP
// status. Returns std::nullopt if the connection fails.
std::optional<http::status> canUploadStatus(unsigned short port) {
  try {
    return syncPost(port, "", "text/plain", {{"Can-Upload", "1"}}).status;
  } catch (...) {
    return std::nullopt;
  }
}

// Call `syncPost` and assert that the response status matches `expected`.
void expectStatus(unsigned short port, std::string_view body,
                  http::status expected,
                  std::optional<std::string_view> contentType = std::nullopt,
                  ExtraHeaders extraHeaders = {}) {
  EXPECT_EQ(syncPost(port, body, contentType, std::move(extraHeaders)).status,
            expected);
}

// Owns an `InputFileServer` (zero-capacity queue) and the range returned by
// `run()`. Constructed via guaranteed copy elision so the server is never
// moved after its address is captured by the range.
struct TestServer {
  InputFileServer server;
  ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> range;
  explicit TestServer(unsigned short port)
      : server{port, 0}, range{server.run()} {}
};

// Parameters for the upload-and-query parametrized test.
struct UploadTestParams {
  std::string baseName;
  std::string body;
  std::string contentType;
  ExtraHeaders extraHeaders;
  std::string query;
  std::string expectedResult;
};

class UploadAndQueryTest : public TestWithParam<UploadTestParams> {};

}  // namespace

// ____________________________________________________________________________
// Upload RDF data in a variety of formats and verify that the triples are
// queryable after the index is built.
TEST_P(UploadAndQueryTest, Run) {
  auto [baseName, body, contentType, extraHeaders, query, expected] =
      GetParam();
  auto port = findFreePort();
  IndexBuilderConfig config;
  config.inputServerPort_ = port;
  config.baseName_ = baseName;

  std::exception_ptr buildEx;
  {
    // The JThread destructor blocks when this scope ends (after sendFinish()
    // has been called), so the index build is guaranteed to complete before
    // we run queries below.
    ad_utility::JThread buildThread{[&]() {
      try {
        Qlever::buildIndex(config);
      } catch (...) {
        buildEx = std::current_exception();
      }
    }};

    ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";
    expectStatus(port, body, http::status::ok, contentType, extraHeaders);
    EXPECT_EQ(sendFinish(port).status, http::status::ok);
  }  // buildThread joins here — index is fully built

  if (buildEx) {
    std::rethrow_exception(buildEx);
  }

  EngineConfig ec{config};
  Qlever engine{ec};
  EXPECT_EQ(engine.query(query, ad_utility::MediaType::tsv), expected);
}

INSTANTIATE_TEST_SUITE_P(
    UploadFormats, UploadAndQueryTest,
    Values(
        UploadTestParams{"InputFileServerTestTurtle",
                         "<s1> <p> <o1>. <s2> <p> <o2>.",
                         "text/turtle",
                         {},
                         "SELECT ?s WHERE { ?s <p> ?o } ORDER BY ?s",
                         "?s\n<s1>\n<s2>\n"},
        UploadTestParams{
            "InputFileServerTestNQuads",
            "<s1> <p> <o1> <g1> .\n<s2> <p> <o2> <g1> .\n",
            "application/n-quads",
            {},
            "SELECT ?s WHERE { GRAPH <g1> { ?s <p> ?o } } ORDER BY ?s",
            "?s\n<s1>\n<s2>\n"},
        UploadTestParams{"InputFileServerTestNTriples",
                         "<a> <b> <c> .\n",
                         "application/n-triples",
                         {},
                         "SELECT ?o WHERE { <a> <b> ?o }",
                         "?o\n<c>\n"},
        UploadTestParams{
            "InputFileServerTestGraphHeader",
            "<s1> <p> <o1>.",
            "text/turtle",
            {{"graph", "https://example.org/g"}},
            "SELECT ?s WHERE { GRAPH <https://example.org/g> { ?s <p> ?o } }",
            "?s\n<s1>\n"}),
    [](const auto& info) { return info.param.baseName; });

// ____________________________________________________________________________
// A Content-Type that is not one of the supported values must return 415.
TEST(InputFileServer, InvalidContentType) {
  auto port = findFreePort();
  TestServer ts{port};

  ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

  expectStatus(port, "<a> <b> <c>.", http::status::unsupported_media_type,
               "application/json");

  EXPECT_EQ(sendFinish(port).status, http::status::ok);
  for ([[maybe_unused]] auto& _ : ts.range) {
  }
}

// ____________________________________________________________________________
// A file upload without a Content-Type header must return 415.
TEST(InputFileServer, MissingContentType) {
  auto port = findFreePort();
  TestServer ts{port};

  ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

  expectStatus(port, "<a> <b> <c>.", http::status::unsupported_media_type);

  EXPECT_EQ(sendFinish(port).status, http::status::ok);
  for ([[maybe_unused]] auto& _ : ts.range) {
  }
}

// ____________________________________________________________________________
// A `Finish-Index-Building` request with a non-empty body must return 400.
TEST(InputFileServer, FinishWithNonEmptyBody) {
  auto port = findFreePort();
  TestServer ts{port};

  ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

  // Bad finish — non-empty body.
  expectStatus(port, "some-body", http::status::bad_request, "text/plain",
               {{"Finish-Index-Building", "true"}});

  // Proper finish.
  EXPECT_EQ(sendFinish(port).status, http::status::ok);
  for ([[maybe_unused]] auto& _ : ts.range) {
  }
}

// ____________________________________________________________________________
// A server with zero queue size always returns 429 for Can-Upload and upload
// requests.
TEST(InputFileServer, ZeroQueueSize) {
  auto port = findFreePort();
  TestServer ts{port};

  ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

  // Can-Upload returns 429 because the queue is never ready.
  EXPECT_EQ(canUploadStatus(port), http::status::too_many_requests);

  // File uploads return 429.
  expectStatus(port, "<a> <b> <c>.", http::status::too_many_requests,
               "text/turtle");

  // Finish the server before draining (queue.finish() makes pop() return
  // nullopt immediately, so the range is empty without blocking).
  EXPECT_EQ(sendFinish(port).status, http::status::ok);
  EXPECT_EQ(ql::ranges::distance(ts.range), 0);
}

// ____________________________________________________________________________
// Upload a body larger than the lazy-mode chunk size (1 MB) to exercise
// multi-chunk streaming. Verifies that all bytes are received without
// buffering the entire body in memory.
TEST(InputFileServer, LargeBodyMultiChunk) {
  // Build a body that exceeds 1 MB so the HTTP session sends multiple chunks.
  std::string body;
  size_t numTriples = 0;
  while (body.size() < (1u << 20) + (1u << 19)) {
    body += "<http://x.org/s" + std::to_string(numTriples) +
            "> <http://x.org/p> <http://x.org/o" + std::to_string(numTriples) +
            "> .\n";
    ++numTriples;
  }
  size_t bodySize = body.size();

  auto port = findFreePort();
  InputFileServer server{port, 1};
  auto range = server.run();

  ad_utility::JThread uploadThread{[&]() {
    ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";
    expectStatus(port, body, http::status::ok, "application/n-triples");
    EXPECT_EQ(sendFinish(port).status, http::status::ok);
  }};

  // Drain each uploaded file spec and count total bytes received.
  size_t totalBytes = 0;
  for (auto& spec : range) {
    auto buf = spec.getParallelBuffer(1 << 16);
    while (auto block = buf->getNextBlock()) {
      totalBytes += block->size();
    }
  }
  EXPECT_EQ(totalBytes, bodySize);
}

// ____________________________________________________________________________
// The `Parse-In-Parallel` request header must be honoured and passed through
// to the resulting InputFileSpecification.
TEST(InputFileServer, ParseInParallelHeader) {
  auto port = findFreePort();
  InputFileServer server{port, 1};
  auto range = server.run();

  ad_utility::JThread uploadThread{[&]() {
    ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";
    expectStatus(port, "<a> <b> <c> .\n", http::status::ok,
                 "application/n-triples", {{"Parse-In-Parallel", "true"}});
    EXPECT_EQ(sendFinish(port).status, http::status::ok);
  }};

  for (auto& spec : range) {
    EXPECT_TRUE(spec.parseInParallelSetExplicitly_);
    EXPECT_TRUE(spec.parseInParallel_);
    // Drain the buffer to allow the HTTP session to complete.
    auto buf = spec.getParallelBuffer(1 << 16);
    while (buf->getNextBlock()) {
    }
  }
}
