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
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <emscripten/em_js.h>
#include <emscripten/emscripten.h>
#include <emscripten/threading.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <functional>
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

// The parameters of the responses of the test server below, which has an
// endpoint for each kind of response that the tests need (a large body, a
// slowly streamed one, one with every byte value, ...). They are passed to the
// server rather than written into its JavaScript code, so that the tests can
// use the same values in their expectations instead of repeating them.
//
// The bodies of `/large` and `/stream` are far too large to be delivered in a
// single piece, so a consumer has to receive them chunk by chunk. Byte `i` of
// the body of `/stream` is `i % STREAM_BYTE_MODULUS`, so that a consumer can
// tell whether it received exactly the bytes that were sent, in order (see
// `expectStreamedBytes`).
constexpr int32_t LARGE_BODY_SIZE = 500'000;
constexpr int32_t NUM_STREAM_CHUNKS = 200;
constexpr int32_t STREAM_CHUNK_SIZE = 1 << 16;  // 64 KiB
// A prime below 256, so that each value fits in a byte, and one that does not
// divide `STREAM_CHUNK_SIZE`. Otherwise every chunk would consist of the same
// bytes, and a reordered chunk would go unnoticed.
constexpr int32_t STREAM_BYTE_MODULUS = 251;
static_assert(STREAM_CHUNK_SIZE % STREAM_BYTE_MODULUS != 0);

// How often `/all-bytes` repeats the 256 byte values.
constexpr int32_t NUM_ALL_BYTE_ROUNDS = 40;

// The length of the padding of the `Content-Type` of `/long-content-type`,
// which is far longer than any header value that QLever itself ever sees.
constexpr int32_t LONG_HEADER_SIZE = 4096;

// NOTE: `clang-format` is disabled below because it breaks JavaScript (it turns
// `===` into `== =`).
// clang-format off

// The test server below needs `require` and a socket to listen on.
EM_JS(bool, isNodeJs, (void), {
  return typeof process !== "undefined" &&
      typeof process.versions?.node === "string";
});

// Start the test server and store the port it listens on at the given address
// (that of an `std::atomic<int32_t>`), thereby signalling that it is ready. For
// the meaning of the sizes, see the constants above.
EM_JS(void, startTestServer,
      (void* portAddress, int32_t largeBodySize, int32_t numStreamChunks,
       int32_t streamChunkSize, int32_t streamByteModulus,
       int32_t longHeaderSize, int32_t numAllByteRounds), {
  const http = require("http");
  const server = http.createServer((request, response) => {
    if (request.url === "/hello") {
      response.writeHead(200, {"Content-Type": "text/turtle"});
      response.end("Hello, World!");
    } else if (request.url === "/echo") {
      // Report the request, so that the test can check it.
      const chunks = [];
      request.on("data", (chunk) => chunks.push(chunk));
      request.on("end", () => {
        // Node.js gives the headers as a plain object with lowercase names;
        // `Headers` looks them up case-insensitively, like on the client side.
        const headers = new Headers(request.headers);
        response.writeHead(200, {"Content-Type": "application/json"});
        response.end(JSON.stringify({
          method: request.method,
          body: Buffer.concat(chunks).toString(),
          bodyHex: Buffer.concat(chunks).toString("hex"),
          accept: headers.get("Accept") ?? "",
          contentType: headers.get("Content-Type") ?? ""
        }));
      });
    } else if (request.url === "/all-bytes") {
      // Every byte value, several times over.
      response.writeHead(200, {"Content-Type": "application/octet-stream"});
      const all = Buffer.alloc(256 * numAllByteRounds);
      for (let i = 0; i < all.length; ++i) {
        all[i] = i % 256;
      }
      response.end(all);
    } else if (request.url === "/large") {
      // Sent in many small pieces, so that the client has to assemble it.
      response.writeHead(200, {"Content-Type": "text/plain"});
      // The last piece is whatever is left, so any size works.
      const pieceSize = Math.ceil(largeBodySize / 50);
      for (let sent = 0; sent < largeBodySize; sent += pieceSize) {
        response.write("x".repeat(Math.min(pieceSize, largeBodySize - sent)));
      }
      response.end();
    } else if (request.url === "/stream") {
      // Sent slowly, so that a client which stops reading is still connected
      // when it does so.
      response.writeHead(200, {"Content-Type": "text/plain"});
      let chunksSent = 0;
      const timer = setInterval(() => {
        if (chunksSent === numStreamChunks) {
          clearInterval(timer);
          response.end();
          return;
        }
        const chunk = Buffer.alloc(streamChunkSize);
        for (let i = 0; i < chunk.length; ++i) {
          chunk[i] = (chunksSent * streamChunkSize + i) % streamByteModulus;
        }
        ++chunksSent;
        response.write(chunk);
      }, 2);
      // Count the connections that were closed before we were done sending,
      // that is, the requests the client aborted.
      response.on("close", () => {
        clearInterval(timer);
        // `writableEnded` is true iff `end()` has been called, which the timer
        // above does once it has sent all chunks.
        if (!response.writableEnded) {
          globalThis.numAbortedRequests =
              (globalThis.numAbortedRequests ?? 0) + 1;
        }
      });
    } else if (request.url === "/aborted-requests") {
      response.writeHead(200, {"Content-Type": "text/plain"});
      response.end(String(globalThis.numAbortedRequests ?? 0));
    } else if (request.url === "/redirect") {
      response.writeHead(308, {"Location": "/hello"});
      response.end();
    } else if (request.url === "/long-content-type") {
      response.writeHead(200, {
        "Content-Type": "text/plain;x=" +
            "y".repeat(longHeaderSize)
      });
      response.end("body");
    } else if (request.url === "/closed-port") {
      // A port that nobody listens on (any more), for a request that cannot
      // connect. NOTE: Not a fixed small one like 1, which `fetch` refuses
      // without even trying, because it is on the list of blocked ports of the
      // Fetch standard.
      const probe = require("net").createServer();
      probe.listen(0, "127.0.0.1", () => {
        const port = probe.address().port;
        probe.close(() => {
          response.writeHead(200, {"Content-Type": "text/plain"});
          response.end(String(port));
        });
      });
    } else if (request.url === "/empty") {
      response.writeHead(204);
      response.end();
    } else {
      response.writeHead(404, {"Content-Type": "text/plain"});
      response.end("not found");
    }
  });
  // Port 0 lets the OS pick a free one, so several test binaries can run at
  // the same time. NOTE: `MEMORY64` passes pointers as `BigInt`, which cannot
  // index the heap views, hence the `Number(...)` conversion (a no-op without
  // it). The view is built freshly because Emscripten's cached `HEAPU8` can be
  // stale after a memory growth.
  server.listen(0, "127.0.0.1", () => {
    Atomics.store(
        new Int32Array(HEAPU8.buffer), Number(portAddress) / 4,
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
      startTestServer(&port, LARGE_BODY_SIZE, NUM_STREAM_CHUNKS,
                      STREAM_CHUNK_SIZE, STREAM_BYTE_MODULUS, LONG_HEADER_SIZE,
                      NUM_ALL_BYTE_ROUNDS);
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

// Every byte value, in order, `numRounds` times over.
std::string allByteValues(int numRounds = 1) {
  std::string result;
  for (int round = 0; round < numRounds; ++round) {
    for (int byte = 0; byte < 256; ++byte) {
      result += static_cast<char>(byte);
    }
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

  // The number of requests that the test server has seen the client abort.
  size_t numAbortedRequests() {
    auto response =
        sendHttpOrHttpsRequest(Url{url_ + "/aborted-requests"}, handle_);
    return std::stoul(toString(response));
  }

  // Read the whole body of `/stream` and check that it is exactly the sequence
  // of bytes that the server sent (see there), so that neither a lost nor a
  // duplicated nor a reordered chunk goes unnoticed. Sleeping for
  // `millisecondsPerChunk` after each chunk lets a test consume slower than the
  // server sends.
  void expectStreamedBytes(HttpOrHttpsResponse& response,
                           int millisecondsPerChunk = 0) {
    size_t numBytes = 0;
    size_t numChunks = 0;
    for (ql::span<std::byte> bytes : response.body_) {
      for (std::byte byte : bytes) {
        ASSERT_EQ(static_cast<size_t>(byte), numBytes % STREAM_BYTE_MODULUS)
            << "at byte " << numBytes;
        ++numBytes;
      }
      ++numChunks;
      if (millisecondsPerChunk > 0) {
        emscripten_thread_sleep(millisecondsPerChunk);
      }
    }
    EXPECT_EQ(numBytes,
              static_cast<size_t>(NUM_STREAM_CHUNKS * STREAM_CHUNK_SIZE));
    // The body has to have been consumed as it arrived, not in one piece. How
    // many chunks that makes is up to the JavaScript environment, which cuts
    // the response wherever it happens to, so this is deliberately a long way
    // below the number of pieces that the server sends.
    EXPECT_GT(numChunks, static_cast<size_t>(NUM_STREAM_CHUNKS / 10));
  }

  // Request `/stream` (a response that is still being sent when `consume`
  // returns), let `consume` read as much of it as it wants, and check that the
  // server saw the connection close before it was done sending, that is, that
  // the request was aborted.
  void expectRequestIsAborted(
      const std::function<void(HttpOrHttpsResponse&)>& consume) {
    size_t numAbortedBefore = numAbortedRequests();
    {
      auto response = sendHttpOrHttpsRequest(Url{url_ + "/stream"}, handle_);
      consume(response);
    }
    // The server learns about the closed connection asynchronously, hence the
    // retries.
    size_t numAbortedAfter = numAbortedBefore;
    for (size_t i = 0; i < 100 && numAbortedAfter == numAbortedBefore; ++i) {
      emscripten_thread_sleep(20);
      numAbortedAfter = numAbortedRequests();
    }
    EXPECT_EQ(numAbortedAfter, numAbortedBefore + 1);
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
  EXPECT_EQ(
      nlohmann::json::parse(toString(response)),
      nlohmann::json(
          {{"method", "POST"},
           {"body", "SELECT * WHERE { ?s ?p ?o }"},
           {"bodyHex", absl::BytesToHexString("SELECT * WHERE { ?s ?p ?o }")},
           {"accept", "application/sparql-results+json"},
           {"contentType", "application/sparql-query"}}));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, binaryResponseBody) {
  // A response body is bytes, not text: every byte value has to arrive exactly
  // as it was sent, including the null byte and the ones that are not valid
  // UTF-8. This matters for the binary result formats that a `SERVICE` to
  // another QLever instance may use.
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/all-bytes"}, handle_);
  EXPECT_EQ(response.contentType_, "application/octet-stream");
  EXPECT_EQ(toString(response), allByteValues(NUM_ALL_BYTE_ROUNDS));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, binaryRequestBody) {
  // Every byte value, including the ones that are not valid UTF-8 and the null
  // byte, has to arrive exactly as it was sent.
  std::string body = allByteValues();
  auto response =
      sendHttpOrHttpsRequest(Url{url_ + "/echo"}, handle_, http::verb::post,
                             body, "application/octet-stream", "*/*");
  EXPECT_EQ(
      nlohmann::json::parse(toString(response))["bodyHex"].get<std::string>(),
      absl::BytesToHexString(body));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, emptyRequestBodyIsNotSent) {
  // `fetch` rejects a `GET` that has a body, so an empty one must not become
  // an empty `Uint8Array` that is then sent anyway.
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/hello"}, handle_);
  EXPECT_EQ(toString(response), "Hello, World!");
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
  EXPECT_EQ(numBytes, static_cast<size_t>(LARGE_BODY_SIZE));
  EXPECT_GT(numChunks, 1u);
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, bodyThatIsNotReadCompletely) {
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/large"}, handle_);
  EXPECT_EQ(std::move(response).readResponseHead(20), std::string(20, 'x'));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, largeBodyIsStreamed) {
  // The body is far larger than anything that is buffered along the way, so
  // this only works if it is consumed as it arrives.
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/stream"}, handle_);
  expectStreamedBytes(response);
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, slowConsumerGetsTheWholeBody) {
  // Consume much slower than the server sends, so that the response piles up in
  // the JavaScript environment while we are not asking for it (which the test
  // above, where the consumer is the faster one, does not exercise). Not a
  // single byte may be lost or duplicated when we continue.
  auto response = sendHttpOrHttpsRequest(Url{url_ + "/stream"}, handle_);
  expectStreamedBytes(response, 2);
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, notReadingTheBodyToTheEndAbortsTheRequest) {
  expectRequestIsAborted([](HttpOrHttpsResponse& response) {
    // Read a single chunk, then let the response go out of scope.
    auto iterator = response.body_.begin();
    ASSERT_NE(iterator, response.body_.end());
    EXPECT_GT((*iterator).size(), 0u);
  });
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, abortingInTheMiddleOfAResponseWorks) {
  // Read a few chunks slowly, so that the response is still being sent, and
  // more of it is already waiting in the JavaScript environment, when we stop.
  expectRequestIsAborted([](HttpOrHttpsResponse& response) {
    size_t numChunks = 0;
    for ([[maybe_unused]] ql::span<std::byte> bytes : response.body_) {
      emscripten_thread_sleep(20);
      if (++numChunks == 3) {
        break;
      }
    }
  });
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, notReadingTheBodyAtAllAbortsTheRequest) {
  // Not even starting to read the body has to abort the request as well, even
  // though the body generator is then destroyed without its body ever having
  // been run.
  expectRequestIsAborted([](HttpOrHttpsResponse&) {});
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
      ::testing::HasSubstr("fetch failed (unexpected redirect)"));
  // With a limit, the redirect is followed by the JavaScript environment.
  auto response =
      sendHttpOrHttpsRequest(Url{url_ + "/redirect"}, handle_, http::verb::get,
                             "", "text/plain", "text/plain", 3);
  EXPECT_EQ(response.status_, http::status::ok);
  EXPECT_EQ(toString(response), "Hello, World!");
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, unreachableEndpoint) {
  auto portResponse =
      sendHttpOrHttpsRequest(Url{url_ + "/closed-port"}, handle_);
  std::string url =
      absl::StrCat("http://127.0.0.1:", toString(portResponse), "/");
  // The message that `fetch` itself gives is just "fetch failed"; the reason
  // is in its `cause`, which has to be part of the message as well.
  AD_EXPECT_THROW_WITH_MESSAGE(
      sendHttpOrHttpsRequest(Url{url}, handle_),
      ::testing::AllOf(::testing::HasSubstr(absl::StrCat(
                           "The HTTP request to <", url, "> failed: ")),
                       ::testing::HasSubstr("fetch failed"),
                       ::testing::HasSubstr("(ECONNREFUSED: ")));
}

// _____________________________________________________________________________
TEST_F(HttpClientEmscriptenTest, longHeaderValue) {
  // A header value of any length simply arrives; nothing truncates it.
  auto response =
      sendHttpOrHttpsRequest(Url{url_ + "/long-content-type"}, handle_);
  EXPECT_EQ(response.contentType_,
            absl::StrCat("text/plain;x=", std::string(LONG_HEADER_SIZE, 'y')));
  EXPECT_EQ(toString(response), "body");
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
