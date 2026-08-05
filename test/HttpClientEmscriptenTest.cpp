//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for the Emscripten implementation of `sendHttpOrHttpsRequest` (see
// `src/util/http/HttpClientEmscripten.cpp`). They run against a Node.js HTTP
// server in the same process, so they only work under Node.js, which is where
// the unit tests of the WebAssembly build are run.

#ifdef __EMSCRIPTEN__

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <emscripten/em_js.h>
#include <emscripten/emscripten.h>
#include <emscripten/threading.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/http/HttpClient.h"
#include "util/json.h"

namespace {
namespace http = boost::beast::http;
using ad_utility::httpUtils::Url;

// NOTE: `clang-format` is disabled below because it breaks JavaScript (it turns
// `===` into `== =`).
// clang-format off

// The test server below needs `require` and a socket to listen on.
EM_JS(bool, isNodeJs, (void), {
  return typeof process !== "undefined" && !!process.versions &&
         !!process.versions.node;
});

// Start the test server and store the port it listens on at the given address
// (that of an `std::atomic<int32_t>`), thereby signalling that it is ready.
EM_JS(void, startTestServer, (void* portAddress), {
  var http = require("http");
  var server = http.createServer(function(request, response) {
    var url = request.url;
    if (url === "/hello") {
      response.writeHead(200, {"Content-Type" : "text/turtle"});
      response.end("Hello, World!");
    } else if (url === "/echo") {
      // Report the request, so that the test can check it.
      var chunks = [];
      request.on("data", function(chunk) { chunks.push(chunk); });
      request.on("end", function() {
        response.writeHead(200, {"Content-Type" : "application/json"});
        response.end(JSON.stringify({
          method : request.method,
          body : Buffer.concat(chunks).toString(),
          accept : request.headers["accept"] || "",
          contentType : request.headers["content-type"] || ""
        }));
      });
    } else if (url === "/large") {
      // A body that is larger than the buffer for a single chunk, sent in many
      // small chunks.
      response.writeHead(200, {"Content-Type" : "text/plain"});
      for (var i = 0; i < 50; ++i) {
        response.write("x".repeat(10000));
      }
      response.end();
    } else if (url === "/stream") {
      // Much larger than the ring buffer, and sent slowly, so that a client
      // which stops reading is still connected when it does so.
      response.writeHead(200, {"Content-Type" : "text/plain"});
      var chunksSent = 0;
      var timer = setInterval(function() {
        if (chunksSent === 200) {
          clearInterval(timer);
          response.end();
          return;
        }
        ++chunksSent;
        response.write("y".repeat(65536));
      }, 2);
      // Count the connections that were closed before we were done sending,
      // that is, the requests the client aborted.
      response.on("close", function() {
        clearInterval(timer);
        if (!response.writableEnded) {
          globalThis.numAbortedRequests = (globalThis.numAbortedRequests || 0) + 1;
        }
      });
    } else if (url === "/aborted-requests") {
      response.writeHead(200, {"Content-Type" : "text/plain"});
      response.end(String(globalThis.numAbortedRequests || 0));
    } else if (url === "/redirect") {
      response.writeHead(308, {"Location" : "/hello"});
      response.end();
    } else if (url === "/empty") {
      response.writeHead(204);
      response.end();
    } else {
      response.writeHead(404, {"Content-Type" : "text/plain"});
      response.end("not found");
    }
  });
  // Port 0 lets the OS pick a free one, so several test binaries can run at
  // the same time. NOTE: `MEMORY64` passes pointers as `BigInt`, which cannot
  // index the heap views, hence the `Number(...)` conversion (a no-op without
  // it). The view is built freshly because Emscripten's cached `HEAPU8` can be
  // stale after a memory growth.
  server.listen(0, "127.0.0.1", function() {
    Atomics.store(new Int32Array(HEAPU8.buffer), Number(portAddress) / 4,
                  server.address().port);
  });
  // Don't keep the process alive just because of the server.
  server.unref();
});

// clang-format on

// The URL of the test server, which is started on first use. It needs a thread
// of its own, because the thread that performs a request is blocked while it
// waits for the response and hence cannot run the server.
const std::string& testServerUrl() {
  static const std::string url = []() {
    static std::atomic<int32_t> port{0};
    std::thread{[]() {
      startTestServer(&port);
      // Keep the Web Worker of this thread (and hence the server) alive.
      emscripten_exit_with_live_runtime();
    }}.detach();
    while (port.load() == 0) {
      emscripten_thread_sleep(10);
    }
    return absl::StrCat("http://127.0.0.1:", port.load());
  }();
  return url;
}

// The body of the given response as a string.
std::string toString(HttpOrHttpsResponse& response) {
  std::string result;
  for (ql::span<std::byte> bytes : response.body_) {
    result += std::string_view{reinterpret_cast<const char*>(bytes.data()),
                               bytes.size()};
  }
  return result;
}

class HttpClientEmscriptenTest : public ::testing::Test {
 protected:
  ad_utility::SharedCancellationHandle handle_ =
      std::make_shared<ad_utility::CancellationHandle<>>();
  std::string url_;

  void SetUp() override {
    if (!isNodeJs()) {
      GTEST_SKIP() << "The test server requires Node.js";
    }
    url_ = testServerUrl();
  }
};

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, simpleGetRequest) {
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/hello"}, handle_);
  EXPECT_EQ(response.status_, http::status::ok);
  EXPECT_EQ(response.contentType_, "text/turtle");
  EXPECT_EQ(toString(response), "Hello, World!");
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, postRequestWithBodyAndHeaders) {
  // The kind of request that the `SERVICE` operation sends.
  auto response = sendHttpOrHttpsRequest(
      Url{url_ + "/echo"}, handle_, http::verb::post,
      "SELECT * WHERE { ?s ?p ?o }", "application/sparql-query",
      "application/sparql-results+json");
  EXPECT_EQ(response.status_, http::status::ok);
  EXPECT_EQ(nlohmann::json::parse(toString(response)),
            nlohmann::json({{"method", "POST"},
                            {"body", "SELECT * WHERE { ?s ?p ?o }"},
                            {"accept", "application/sparql-results+json"},
                            {"contentType", "application/sparql-query"}}));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, largeBodyIsReadInChunks) {
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/large"}, handle_);
  size_t numChunks = 0;
  size_t numBytes = 0;
  for (ql::span<std::byte> bytes : response.body_) {
    ++numChunks;
    numBytes += bytes.size();
  }
  EXPECT_EQ(numBytes, 500000u);
  EXPECT_GT(numChunks, 1u);
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, bodyThatIsNotReadCompletely) {
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/large"}, handle_);
  EXPECT_EQ(std::move(response).readResponseHead(20), std::string(20, 'x'));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, bodyLargerThanTheRingBufferIsStreamed) {
  // 200 chunks of 64 KiB are more than an order of magnitude more than the ring
  // buffer holds, so this only works if the body is consumed as it arrives.
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/stream"}, handle_);
  size_t numBytes = 0;
  size_t numChunks = 0;
  for (ql::span<std::byte> bytes : response.body_) {
    numBytes += bytes.size();
    ++numChunks;
  }
  EXPECT_EQ(numBytes, 200 * 65536u);
  EXPECT_GT(numChunks, 100u);
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, notReadingTheBodyToTheEndAbortsTheRequest) {
  size_t numAbortedBefore = 0;
  {
    auto response =
        sendHttpOrHttpsRequest(Url{url_ + "/aborted-requests"}, handle_);
    numAbortedBefore = std::stoul(toString(response));
  }
  {
    // Read a single chunk of a response that is still being sent, then let the
    // response go out of scope.
    auto response = sendHttpOrHttpsRequest(Url{url_ + "/stream"}, handle_);
    auto iterator = response.body_.begin();
    ASSERT_NE(iterator, response.body_.end());
    EXPECT_GT((*iterator).size(), 0u);
  }
  // The server has to have seen the connection close before it was done
  // sending; it learns about that asynchronously, hence the retries.
  size_t numAbortedAfter = numAbortedBefore;
  for (size_t i = 0; i < 100 && numAbortedAfter == numAbortedBefore; ++i) {
    emscripten_thread_sleep(20);
    auto response =
        sendHttpOrHttpsRequest(Url{url_ + "/aborted-requests"}, handle_);
    numAbortedAfter = std::stoul(toString(response));
  }
  EXPECT_EQ(numAbortedAfter, numAbortedBefore + 1);
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, emptyBody) {
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/empty"}, handle_);
  EXPECT_EQ(response.status_, http::status::no_content);
  EXPECT_TRUE(toString(response).empty());
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, errorStatusIsReported) {
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/nope"}, handle_);
  EXPECT_EQ(response.status_, http::status::not_found);
  EXPECT_EQ(toString(response), "not found");
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, redirects) {
  // Without an explicit limit, redirects are not followed, so the request
  // has to fail.
  AD_EXPECT_THROW_WITH_MESSAGE(
      sendHttpOrHttpsRequest(Url{url_ + "/redirect"}, handle_),
      ::testing::HasSubstr("failed"));
  // With a limit, the redirect is followed by the JavaScript environment.
  auto response =
      sendHttpOrHttpsRequest(Url{url_ + "/redirect"}, handle_, http::verb::get,
                             "", "text/plain", "text/plain", 3);
  EXPECT_EQ(response.status_, http::status::ok);
  EXPECT_EQ(toString(response), "Hello, World!");
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, unreachableEndpoint) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      sendHttpOrHttpsRequest(Url{"http://127.0.0.1:1/unreachable"}, handle_),
      ::testing::HasSubstr("failed"));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, cancellation) {
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(sendHttpOrHttpsRequest(Url{url_ + "/hello"}, handle),
               ad_utility::CancellationException);
  // A request after a cancelled one still works.
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/hello"}, handle_);
  EXPECT_EQ(toString(response), "Hello, World!");
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, requestFromTheMainThreadOfABrowserFailsFast) {
  // Pretend to be on the main thread of a browser (detected by the absence of
  // `WorkerGlobalScope`), where blocking is not allowed. NOTE: This has to run
  // in the JavaScript context of the thread that performs the request, which is
  // the thread this test runs on.
  EM_ASM({ globalThis.window = {}; });
  absl::Cleanup restoreEnvironment{
      []() { EM_ASM({ delete globalThis.window; }); }};
  AD_EXPECT_THROW_WITH_MESSAGE(
      sendHttpOrHttpsRequest(Url{url_ + "/hello"}, handle_),
      ::testing::HasSubstr("main thread of a browser"));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, concurrentRequestsFromSeveralThreads) {
  std::atomic<size_t> numSuccesses = 0;
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([&url = url_, &numSuccesses]() {
      auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
      auto response = sendHttpOrHttpsRequest(Url{url + "/hello"}, handle);
      if (toString(response) == "Hello, World!") {
        ++numSuccesses;
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(numSuccesses.load(), 4u);
}
}  // namespace

#endif  // __EMSCRIPTEN__
