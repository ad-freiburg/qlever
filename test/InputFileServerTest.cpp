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

// Synchronously POST `body` with the given `contentType` to
// localhost:`port`. Any additional string-keyed headers can be passed via
// `extraHeaders`. Returns the HTTP status and the response body.
PostResponse syncPost(
    unsigned short port, std::string_view body, std::string_view contentType,
    std::vector<std::pair<std::string, std::string>> extraHeaders = {}) {
  net::io_context ioc;
  beast::tcp_stream stream{ioc};
  tcp::resolver resolver{ioc};
  auto const results = resolver.resolve("localhost", std::to_string(port));
  stream.connect(results);

  http::request<http::string_body> req{http::verb::post, "/", 11};
  req.set(http::field::host, "localhost");
  req.set(http::field::content_type, contentType);
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
  return syncPost(port, "", "text/plain", {{"Finish-Index-Building", "true"}});
}

}  // namespace

// ____________________________________________________________________________
// Build an index from turtle data uploaded in two separate requests and verify
// that all triples are queryable.
TEST(InputFileServer, TurtleUploadAndQuery) {
  auto port = findFreePort();
  IndexBuilderConfig config;
  config.inputServerPort_ = port;
  config.baseName_ = "InputFileServerTestTurtle";

  std::exception_ptr buildEx;
  {
    // Build the index in a background thread; JThread auto-joins on
    // destruction at the end of this block, ensuring the build completes
    // before we run queries.
    ad_utility::JThread buildThread{[&]() {
      try {
        Qlever::buildIndex(config);
      } catch (...) {
        buildEx = std::current_exception();
      }
    }};

    ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

    // Upload turtle data in two separate requests.
    auto r1 = syncPost(port, "<s1> <p> <o1>.", "text/turtle");
    EXPECT_EQ(r1.status, http::status::ok);

    auto r2 = syncPost(port, "<s2> <p> <o2>.", "text/turtle");
    EXPECT_EQ(r2.status, http::status::ok);

    // Signal that all data has been sent.
    EXPECT_EQ(sendFinish(port).status, http::status::ok);
  }  // buildThread joins here — index is fully built

  if (buildEx) {
    std::rethrow_exception(buildEx);
  }

  EngineConfig ec{config};
  Qlever engine{ec};

  auto res = engine.query("SELECT ?s WHERE { ?s <p> ?o } ORDER BY ?s",
                          ad_utility::MediaType::tsv);
  EXPECT_EQ(res, "?s\n<s1>\n<s2>\n");
}

// ____________________________________________________________________________
// Upload N-Quads with explicit graphs (Content-Type: application/n-quads) and
// verify that the triples can be queried within the correct named graph.
TEST(InputFileServer, NQuadsUploadAndQuery) {
  auto port = findFreePort();
  IndexBuilderConfig config;
  config.inputServerPort_ = port;
  config.baseName_ = "InputFileServerTestNQuads";

  std::exception_ptr buildEx;
  {
    ad_utility::JThread buildThread{[&]() {
      try {
        Qlever::buildIndex(config);
      } catch (...) {
        buildEx = std::current_exception();
      }
    }};

    ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

    auto r = syncPost(port,
                      "<s1> <p> <o1> <g1> .\n"
                      "<s2> <p> <o2> <g1> .\n",
                      "application/n-quads");
    EXPECT_EQ(r.status, http::status::ok);

    EXPECT_EQ(sendFinish(port).status, http::status::ok);
  }

  if (buildEx) {
    std::rethrow_exception(buildEx);
  }

  EngineConfig ec{config};
  Qlever engine{ec};

  auto res =
      engine.query("SELECT ?s WHERE { GRAPH <g1> { ?s <p> ?o } } ORDER BY ?s",
                   ad_utility::MediaType::tsv);
  EXPECT_EQ(res, "?s\n<s1>\n<s2>\n");
}

// ____________________________________________________________________________
// Upload N-Triples (Content-Type: application/n-triples) and verify the result.
TEST(InputFileServer, NTriplesUploadAndQuery) {
  auto port = findFreePort();
  IndexBuilderConfig config;
  config.inputServerPort_ = port;
  config.baseName_ = "InputFileServerTestNTriples";

  std::exception_ptr buildEx;
  {
    ad_utility::JThread buildThread{[&]() {
      try {
        Qlever::buildIndex(config);
      } catch (...) {
        buildEx = std::current_exception();
      }
    }};

    ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

    // N-Triples format — same as Turtle for our parser.
    auto r = syncPost(port, "<a> <b> <c> .\n", "application/n-triples");
    EXPECT_EQ(r.status, http::status::ok);

    EXPECT_EQ(sendFinish(port).status, http::status::ok);
  }

  if (buildEx) {
    std::rethrow_exception(buildEx);
  }

  EngineConfig ec{config};
  Qlever engine{ec};

  auto res = engine.query("SELECT ?o WHERE { <a> <b> ?o }",
                          ad_utility::MediaType::tsv);
  EXPECT_EQ(res, "?o\n<c>\n");
}

// ____________________________________________________________________________
// Upload turtle into a specific named graph via the `graph` header.
TEST(InputFileServer, GraphHeaderAndQuery) {
  auto port = findFreePort();
  IndexBuilderConfig config;
  config.inputServerPort_ = port;
  config.baseName_ = "InputFileServerTestGraphHeader";

  std::exception_ptr buildEx;
  {
    ad_utility::JThread buildThread{[&]() {
      try {
        Qlever::buildIndex(config);
      } catch (...) {
        buildEx = std::current_exception();
      }
    }};

    ASSERT_TRUE(waitForServer(port)) << "InputFileServer did not become ready";

    // The `graph` header assigns all triples to the named graph.
    auto r = syncPost(port, "<s1> <p> <o1>.", "text/turtle",
                      {{"graph", "https://example.org/g"}});
    EXPECT_EQ(r.status, http::status::ok);

    EXPECT_EQ(sendFinish(port).status, http::status::ok);
  }

  if (buildEx) {
    std::rethrow_exception(buildEx);
  }

  EngineConfig ec{config};
  Qlever engine{ec};

  auto res = engine.query(
      "SELECT ?s WHERE { GRAPH <https://example.org/g> { ?s <p> ?o } }",
      ad_utility::MediaType::tsv);
  EXPECT_EQ(res, "?s\n<s1>\n");
}
