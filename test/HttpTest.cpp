// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <boost/url/url_view.hpp>
#include <charconv>
#include <thread>

#include "HttpTestHelpers.h"
#include "global/RuntimeParameters.h"
#include "util/GTestHelpers.h"
#include "util/Random.h"
#include "util/http/HttpClient.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"
#include "util/http/beast.h"
#include "util/jthread.h"

using namespace ad_utility::httpUtils;
using namespace boost::beast::http;
using ::testing::HasSubstr;

namespace {

/// Join all of the bytes into a big string.
std::string toString(cppcoro::generator<ql::span<std::byte>> generator) {
  std::string result;
  for (std::byte byte : generator | ql::ranges::views::join) {
    result.push_back(static_cast<char>(byte));
  }
  return result;
}
}  // namespace

namespace {

// Returns a string representation of an HTTP verb for use in echo responses.
std::string_view verbName(verb v) { return toStd(to_string(v)); }

// Assembles the full request body from an eager-mode request (reads from
// `req.body()`) or a lazy-mode `bodyGetter` (accumulates chunks). Both
// overloads return `net::awaitable<std::string>` so callers can uniformly
// use `co_await consumeBody(req, args...)`.
boost::asio::awaitable<std::string> consumeBody(
    http::request<http::string_body> req) {
  co_return req.body();
}
template <typename Request, typename BodyGetter>
boost::asio::awaitable<std::string> consumeBody(
    [[maybe_unused]] const Request& request, BodyGetter bodyGetter) {
  std::string body;
  while (auto chunk = co_await bodyGetter()) {
    body += chunk.value();
  }
  co_return body;
}

// Echo handler: sends "METHOD\nTARGET\nBODY". Works for both eager mode
// (no extra args) and lazy mode (bodyGetter as last arg) via variadic `args`.
auto makeEchoHandler() {
  return
      [](auto req, auto&& send, auto... args) -> boost::asio::awaitable<void> {
        std::string body = co_await consumeBody(req, args...);
        co_await send(
            createOkResponse(absl::StrCat(verbName(req.method()), "\n",
                                          toStd(req.target()), "\n", body),
                             req, ad_utility::MediaType::textPlain));
      };
}

// Creates an echo server for `mode` with the given chunk size and returns it.
template <BodyReadMode mode>
auto makeEchoServer(size_t chunkSize) {
  auto handler = makeEchoHandler();
  return TestHttpServer<decltype(handler), mode>(std::move(handler), chunkSize);
}

// Creates an ignore-body server for `mode`: always responds with a fixed
// string without reading the request body.
template <BodyReadMode mode>
auto makeIgnoreBodyServer(size_t chunkSize) {
  auto handler = [](auto req, auto&& send,
                    auto...) -> boost::asio::awaitable<void> {
    co_await send(createOkResponse("ignored body", req,
                                   ad_utility::MediaType::textPlain));
  };
  return TestHttpServer<decltype(handler), mode>(std::move(handler), chunkSize);
}

// Sends an echo response (METHOD\nTARGET\nBODY) and then rethrows `exception`.
// Shared between eager and lazy `makeThrowingEchoServer` handlers.
template <typename Req, typename Send>
boost::asio::awaitable<void> throwingEchoBody(const Req& req, Send& send,
                                              std::string_view body,
                                              std::exception_ptr exception) {
  co_await send(createOkResponse(absl::StrCat(verbName(req.method()), "\n",
                                              toStd(req.target()), "\n", body),
                                 req, ad_utility::MediaType::textPlain));
  std::rethrow_exception(exception);
}

// Creates an echo server that throws `exception` after sending the response.
template <BodyReadMode mode>
auto makeThrowingEchoServer(std::exception_ptr exception, size_t chunkSize) {
  auto handler = [exception](auto req, auto&& send,
                             auto... args) -> boost::asio::awaitable<void> {
    std::string body = co_await consumeBody(req, args...);
    co_return co_await throwingEchoBody(req, send, body, exception);
  };
  return TestHttpServer<decltype(handler), mode>(std::move(handler), chunkSize);
}

// Common redirect/success handler logic: inspects URL parameters to redirect
// or respond with "Success", and sets `lastTarget` to the request target.
template <typename Req, typename Send>
boost::asio::awaitable<void> redirectHandlerCore(const Req& req, Send& send,
                                                 std::string& lastTarget) {
  boost::url_view url{req.target()};
  lastTarget = req.target();
  auto redirectIt = url.params().find("location");
  if (redirectIt == url.params().end()) {
    co_return co_await send(
        createOkResponse("Success", req, ad_utility::MediaType::textPlain));
  }
  http::response<http::string_body> resp;
  unsigned redirectStatus = 200;
  auto statusIt = url.params().find("status");
  if (statusIt != url.params().end()) {
    std::string statusString = (*statusIt)->value;
    auto result = std::from_chars(statusString.data(),
                                  statusString.data() + statusString.size(),
                                  redirectStatus);
    AD_CORRECTNESS_CHECK(result.ec == std::errc());
  }
  resp.result(redirectStatus);
  resp.set(http::field::content_type, "text/plain");
  resp.set(http::field::location, (*redirectIt)->value.data());
  resp.body() = "";
  resp.prepare_payload();
  co_await send(std::move(resp));
}

// Creates a redirect/success server for `mode`. The redirect logic is
// body-mode-agnostic; `args` is empty in eager mode or holds `bodyGetter`
// in lazy mode.
template <BodyReadMode mode>
auto makeRedirectServer(std::string& lastTarget, size_t chunkSize) {
  auto handler = [&lastTarget](auto req, auto&& send,
                               auto...) -> boost::asio::awaitable<void> {
    co_return co_await redirectHandlerCore(req, send, lastTarget);
  };
  return TestHttpServer<decltype(handler), mode>(std::move(handler), chunkSize);
}

}  // namespace

// Fixture type tag: wraps `BodyReadMode` as a type so it can be used with
// `TYPED_TEST_SUITE`.
template <BodyReadMode M>
using ModeTag = std::integral_constant<BodyReadMode, M>;

template <typename T>
class HttpServerBodyTest : public ::testing::Test {
 protected:
  static constexpr BodyReadMode kMode = T::value;
  // Eager mode ignores the chunk size; 100 bytes keeps lazy-mode tests small.
  static constexpr size_t lazyChunkSize = 100u;

  ad_utility::SharedCancellationHandle handle_ =
      std::make_shared<ad_utility::CancellationHandle<>>();
};

using AllBodyReadModes =
    ::testing::Types<ModeTag<BodyReadMode::Eager>, ModeTag<BodyReadMode::Lazy>>;
TYPED_TEST_SUITE(HttpServerBodyTest, AllBodyReadModes);

// POST with a small body is echoed correctly.
TYPED_TEST(HttpServerBodyTest, EchoPostSmallBody) {
  auto server = makeEchoServer<TypeParam::value>(this->lazyChunkSize);
  server.runInOwnThread();

  auto httpClient = std::make_unique<HttpClient>(
      "localhost", std::to_string(server.getPort()));
  auto response =
      HttpClient::sendRequest(std::move(httpClient), verb::post, "localhost",
                              "/target", this->handle_, "hello body");
  EXPECT_EQ(response.status_, status::ok);
  EXPECT_EQ(toString(std::move(response.body_)), "POST\n/target\nhello body");
}

// GET with no body: `bodyGetter` immediately yields nullopt in lazy mode;
// `req.body()` is empty in eager mode.
TYPED_TEST(HttpServerBodyTest, EchoGetNoBody) {
  auto server = makeEchoServer<TypeParam::value>(this->lazyChunkSize);
  server.runInOwnThread();

  auto httpClient = std::make_unique<HttpClient>(
      "localhost", std::to_string(server.getPort()));
  auto response = HttpClient::sendRequest(std::move(httpClient), verb::get,
                                          "localhost", "/other", this->handle_);
  EXPECT_EQ(response.status_, status::ok);
  EXPECT_EQ(toString(std::move(response.body_)), "GET\n/other\n");
}

// Body that spans multiple chunks (3 × lazyChunkSize bytes) is echoed in full.
TYPED_TEST(HttpServerBodyTest, EchoPostMultipleChunks) {
  auto server = makeEchoServer<TypeParam::value>(this->lazyChunkSize);
  server.runInOwnThread();

  ad_utility::SlowRandomIntGenerator<int> gen('a', 'z');
  std::string largeBody(3 * this->lazyChunkSize, '\0');
  // Note: we need the explicit casting to `char`, because
  // `SlowRandomIntGenerator<char>` doesn't compile on AppleClang, because the
  // libc++ variation there requires actual integer types for the random
  // distributions.
  ql::ranges::generate(largeBody,
                       [&gen]() { return static_cast<char>(gen()); });
  auto httpClient = std::make_unique<HttpClient>(
      "localhost", std::to_string(server.getPort()));
  auto response =
      HttpClient::sendRequest(std::move(httpClient), verb::post, "localhost",
                              "/large", this->handle_, largeBody);
  EXPECT_EQ(response.status_, status::ok);
  EXPECT_EQ(toString(std::move(response.body_)), "POST\n/large\n" + largeBody);
}

// The handler may send a response without consuming the body; the connection
// should still close cleanly.
TYPED_TEST(HttpServerBodyTest, EarlyResponse) {
  auto server = makeIgnoreBodyServer<TypeParam::value>(this->lazyChunkSize);
  server.runInOwnThread();

  auto httpClient = std::make_unique<HttpClient>(
      "localhost", std::to_string(server.getPort()));
  auto response =
      HttpClient::sendRequest(std::move(httpClient), verb::post, "localhost",
                              "/skip", this->handle_, "unread body content");
  EXPECT_EQ(response.status_, status::ok);
  EXPECT_EQ(toString(std::move(response.body_)), "ignored body");
}

// Runs GET/POST echo, WebSocket upgrade and rejection, and
// `sendHttpOrHttpsRequest` for both body-read modes.
TYPED_TEST(HttpServerBodyTest, HttpTest) {
  // This test used to spuriously crash because of something that we (joka92,
  // RobinTF) currently consider to be a bug in Boost::ASIO. (See
  // `util/http/beast.h` for details). Repeat this test several times to make
  // such failures less spurious should they ever reoccur in the future.
  for (size_t k = 0; k < 10; ++k) {
    auto httpServer = makeEchoServer<TypeParam::value>(this->lazyChunkSize);
    httpServer.runInOwnThread();

    // Create a client, and send a GET request.
    // The constants in those test loops can be increased to find threading
    // issues using the thread sanitizer. However, these constants can't be
    // higher by default because the checks on GitHub actions will run forever
    // if they are.
    {
      std::vector<ad_utility::JThread> threads;
      for (size_t i = 0; i < 2; ++i) {
        threads.emplace_back([&]() {
          for (size_t j = 0; j < 20; ++j) {
            {
              auto httpClient = std::make_unique<HttpClient>(
                  "localhost", std::to_string(httpServer.getPort()));
              auto response = HttpClient::sendRequest(std::move(httpClient),
                                                      verb::get, "localhost",
                                                      "target1", this->handle_);
              ASSERT_EQ(response.status_, boost::beast::http::status::ok);
              ASSERT_EQ(response.contentType_, "text/plain");
              ASSERT_EQ(toString(std::move(response.body_)), "GET\ntarget1\n");
            }
          }
        });
      }
    }

    // Do the same thing in a second session (to check if everything is still
    // fine with the server after we have communicated with it for one session).
    {
      auto httpClient = std::make_unique<HttpClient>(
          "localhost", std::to_string(httpServer.getPort()));
      auto response = HttpClient::sendRequest(std::move(httpClient), verb::post,
                                              "localhost", "target2",
                                              this->handle_, "body2");
      ASSERT_EQ(response.status_, boost::beast::http::status::ok);
      ASSERT_EQ(response.contentType_, "text/plain");
      ASSERT_EQ(toString(std::move(response.body_)), "POST\ntarget2\nbody2");
    }

    // Test if websocket is correctly opened and closed.
    for (size_t i = 0; i < 20; ++i) {
      {
        HttpClient httpClient("localhost",
                              std::to_string(httpServer.getPort()));
        auto response = httpClient.sendWebSocketHandshake(
            verb::get, "localhost", "/watch/some-id");
        // verify request is upgraded
        ASSERT_EQ(response.base().result(), http::status::switching_protocols);
      }
    }

    // Test if websocket is denied on wrong paths.
    {
      HttpClient httpClient("localhost", std::to_string(httpServer.getPort()));
      auto response = httpClient.sendWebSocketHandshake(verb::get, "localhost",
                                                        "/other-path");
      // Check for not found error
      ASSERT_EQ(response.base().result(), http::status::not_found);
    }

    // Also test the convenience function `sendHttpOrHttpsRequest`, which
    // creates an own client for each request.
    {
      Url url{
          absl::StrCat("http://localhost:", httpServer.getPort(), "/target")};
      ASSERT_EQ(
          toString(sendHttpOrHttpsRequest(url, this->handle_, verb::get).body_),
          "GET\n/target\n");
      ASSERT_EQ(toString(sendHttpOrHttpsRequest(url, this->handle_, verb::post,
                                                "body")
                             .body_),
                "POST\n/target\nbody");
    }

    // Check that after shutting down, no more new connections are accepted.
    httpServer.shutDown();
    ASSERT_ANY_THROW(
        HttpClient("localhost", std::to_string(httpServer.getPort())));
  }
}

// Test the various `catch` clauses in `HttpServer::session`.
TYPED_TEST(HttpServerBodyTest, ErrorHandlingInSession) {
  // Do the following: Create an HttpServer that echoes the response and then
  // throws `exceptionObject`. Send an HTTP request to trigger the exception.
  // Capture the server log and return it for inspection.
  // Note: We need a separate server for each call because we must shut down
  // before reading the log to avoid a race condition on the logging stream.
  auto throwAndCaptureLog = [this](auto exceptionObject) {
    // We interfere with the logging to test it.
    auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();

    // Convert the `exceptionObject` to an `exception_ptr`.
    std::exception_ptr exception;
    try {
      throw exceptionObject;
    } catch (...) {
      exception = std::current_exception();
    }

    auto httpServer = makeThrowingEchoServer<TypeParam::value>(
        exception, this->lazyChunkSize);
    httpServer.runInOwnThread();
    auto httpClient = std::make_unique<HttpClient>(
        "localhost", std::to_string(httpServer.getPort()));

    auto response =
        HttpClient::sendRequest(std::move(httpClient), verb::get, "localhost",
                                "target1", this->handle_);
    EXPECT_EQ(response.status_, boost::beast::http::status::ok);
    EXPECT_EQ(response.contentType_, "text/plain");
    EXPECT_EQ(toString(std::move(response.body_)), "GET\ntarget1\n");

    // We need to shut down the server first to not have a race condition on the
    // logging stream.
    httpServer.shutDown();
    return logStream.str();
  };

  using namespace ::testing;
  // Logging of a general `boost` exception.
  std::string result;
  auto s = throwAndCaptureLog(
      beast::system_error{boost::asio::error::host_not_found_try_again});

  // TODO<joka921> Actually this always should yield `not found`, but on the
  // Docker cross-compilation build for ARM, this test sometimes fails because
  // we don't land in the correct catch clause for some reason. This needs
  // further debugging.
  std::string expectedHostNotFound =
      beast::system_error{boost::asio::error::host_not_found_try_again}.what();
  EXPECT_THAT(s, AnyOf(HasSubstr(expectedHostNotFound), Eq("")));

  // The `timeout` and `eof` exceptions are only logged at `TRACE` level;
  // normally they are silently caught and ignored.
  s = throwAndCaptureLog(beast::system_error{beast::error::timeout});
  s += throwAndCaptureLog(beast::system_error{boost::asio::error::eof});
  if (LOGLEVEL >= TRACE) {
    EXPECT_THAT(s,
                AllOf(HasSubstr("due to a timeout"), HasSubstr("End of file")));
  } else {
    EXPECT_THAT(s, Eq(""));
  }

  // Handling of `std::exception`.
  if constexpr (LOGLEVEL >= ERROR) {
    s = throwAndCaptureLog(std::runtime_error{"The runtime error for testing"});
    EXPECT_THAT(s, HasSubstr("The runtime error for testing"));
  }

  // Thrown object that is not a `std::exception`.
  if constexpr (LOGLEVEL >= ERROR) {
    s = throwAndCaptureLog(47);
    EXPECT_THAT(
        s, HasSubstr("Weird exception not inheriting from std::exception"));
  }
}

// Test the request body size limit (eager mode) and basic body handling (lazy).
TYPED_TEST(HttpServerBodyTest, RequestBodySizeLimit) {
  constexpr BodyReadMode kMode = TypeParam::value;
  auto httpServer = makeIgnoreBodyServer<TypeParam::value>(this->lazyChunkSize);
  httpServer.runInOwnThread();

  auto ResponseMetadata = [](const status status,
                             std::string_view content_type) {
    return AllOf(
        AD_FIELD(HttpOrHttpsResponse, status_, testing::Eq(status)),
        AD_FIELD(HttpOrHttpsResponse, contentType_, testing::Eq(content_type)));
  };
  auto expectRequestHelper = [&httpServer](
                                 const ad_utility::MemorySize& requestBodySize,
                                 const std::string& expectedBody,
                                 const auto& responseMatcher) {
    ad_utility::SharedCancellationHandle handle =
        std::make_shared<ad_utility::CancellationHandle<>>();

    auto httpClient = std::make_unique<HttpClient>(
        "localhost", std::to_string(httpServer.getPort()));

    const std::string body(requestBodySize.getBytes(), 'f');

    auto response = HttpClient::sendRequest(
        std::move(httpClient), verb::post, "localhost", "target", handle, body);
    EXPECT_THAT(response, responseMatcher);
    EXPECT_THAT(toString(std::move(response.body_)), expectedBody);
  };

  if constexpr (kMode == BodyReadMode::Eager) {
    auto expectRequestFails =
        [&ResponseMetadata,
         &expectRequestHelper](const ad_utility::MemorySize& requestBodySize) {
          const ad_utility::MemorySize currentLimit =
              getRuntimeParameter<&RuntimeParameters::requestBodyLimit_>();
          // For large requests we get an exception while writing to the request
          // stream when going over the limit. For small requests we get the
          // response normally. We would need the HttpClient to return the
          // response even if it couldn't send the request fully in that case.
          expectRequestHelper(
              requestBodySize,
              "Request body size exceeds the allowed size (" +
                  currentLimit.asString() +
                  "), send a smaller request or set the allowed size via the "
                  "runtime parameter `request-body-limit`",
              ResponseMetadata(status::payload_too_large, "text/plain"));
        };
    auto expectRequestSucceeds =
        [&expectRequestHelper,
         &ResponseMetadata](const ad_utility::MemorySize requestBodySize) {
          expectRequestHelper(requestBodySize, "ignored body",
                              ResponseMetadata(status::ok, "text/plain"));
        };
    constexpr auto testingRequestBodyLimit = 50_kB;

    // Set a smaller limit for testing. The default of 100 MB is quite large.
    setRuntimeParameter<&RuntimeParameters::requestBodyLimit_>(50_kB);
    // Requests with bodies smaller than the request body limit are processed.
    expectRequestSucceeds(3_B);
    // Exactly the limit is allowed.
    expectRequestSucceeds(testingRequestBodyLimit);
    // Larger than the limit is forbidden.
    expectRequestFails(testingRequestBodyLimit + 1_B);

    // Setting a smaller request-body limit.
    setRuntimeParameter<&RuntimeParameters::requestBodyLimit_>(1_B);
    expectRequestFails(3_B);
    // Only the request body size counts. The empty body is allowed even if the
    // body is limited to 1 byte.
    expectRequestSucceeds(0_B);

    // Disable the request body limit, by setting it to 0.
    setRuntimeParameter<&RuntimeParameters::requestBodyLimit_>(0_B);
    // Arbitrarily large requests are now allowed.
    expectRequestSucceeds(10_kB);
    expectRequestSucceeds(5_MB);
  } else {
    // In lazy mode the server does not enforce a body size limit; verify that
    // requests of various sizes all succeed. Use small bodies to avoid broken-
    // pipe issues when the handler ignores the body.
    auto expectSucceeds = [&expectRequestHelper,
                           &ResponseMetadata](ad_utility::MemorySize size) {
      expectRequestHelper(size, "ignored body",
                          ResponseMetadata(status::ok, "text/plain"));
    };
    expectSucceeds(0_B);
    expectSucceeds(3_B);
    expectSucceeds(100_B);
  }
}

// Test HTTP redirect handling in `sendHttpOrHttpsRequest`.
TYPED_TEST(HttpServerBodyTest, Redirects) {
  using ::testing::AllOf;

  std::string lastTarget;
  auto server =
      makeRedirectServer<TypeParam::value>(lastTarget, this->lazyChunkSize);
  server.runInOwnThread();

  // Test that all four redirect types (301, 302, 307, 308) work.
  for (auto redirectStatus :
       {status::moved_permanently, status::found, status::temporary_redirect,
        status::permanent_redirect}) {
    std::string redirectUrl =
        absl::StrCat("http://localhost:", server.getPort(),
                     "/?status=", redirectStatus, "&location=%2Fok");

    // Send request with `maxRedirects = 1'.
    auto response = sendHttpOrHttpsRequest(Url{redirectUrl}, this->handle_,
                                           verb::get, "", "", "", 1);
    EXPECT_EQ(response.status_, status::ok);
    EXPECT_EQ(toString(std::move(response.body_)), "Success");
  }

  // Test that redirect with empty `Location` header throws.
  {
    std::string url = absl::StrCat("http://localhost:", server.getPort(),
                                   "/?status=301&location=");

    AD_EXPECT_THROW_WITH_MESSAGE(
        sendHttpOrHttpsRequest(Url{url}, this->handle_, verb::get, "", "", "",
                               1),
        AllOf(HasSubstr("redirect status code"),
              HasSubstr("no Location header")));
  }

  // Test that redirect with invalid `Location` header throws.
  {
    std::string url =
        absl::StrCat("http://localhost:", server.getPort(),
                     "/?status=301&location=Not%20a%20valid%20URL");

    AD_EXPECT_THROW_WITH_MESSAGE(
        sendHttpOrHttpsRequest(Url{url}, this->handle_, verb::get, "", "", "",
                               1),
        AllOf(HasSubstr("redirect status code")));
  }

  // Test that relative URLs are properly resolved.
  {
    std::string url =
        absl::StrCat("http://localhost:", server.getPort(),
                     "/some/relative/path?status=301&location=..%2Fabc");

    auto response = sendHttpOrHttpsRequest(Url{url}, this->handle_, verb::get,
                                           "", "", "", 1);
    EXPECT_EQ(lastTarget, "/some/abc");
    EXPECT_EQ(response.status_, status::ok);
    EXPECT_EQ(toString(std::move(response.body_)), "Success");
  }

  // Test that exceeding max redirects throws, using a chain of two redirects.
  {
    std::string url = absl::StrCat(
        "http://localhost:", server.getPort(),
        "/?status=301&location=%2Fredirect%3Fstatus%3D301%26location%3D%2Fok");

    // With maxRedirects=1, this should fail (needs 2 redirects)
    AD_EXPECT_THROW_WITH_MESSAGE(
        sendHttpOrHttpsRequest(Url{url}, this->handle_, verb::get, "", "", "",
                               1),
        AllOf(HasSubstr("exceeded"), HasSubstr("redirect limit")));
  }

  // Test that `maxRedirects = 0` means no redirects are followed.
  {
    std::string url = absl::StrCat("http://localhost:", server.getPort(),
                                   "/?status=301&location=%2F");
    AD_EXPECT_THROW_WITH_MESSAGE(
        sendHttpOrHttpsRequest(Url{url}, this->handle_, verb::get, "", "", "",
                               0),
        AllOf(HasSubstr("exceeded"), HasSubstr("redirect limit")));
  }
  server.shutDown();
}

namespace {

// Minimal stubs to instantiate `HttpServer` solely for accessing its public
// static helpers `readIntoBuffer` and `materializeBody`. No actual server is
// constructed.
struct DummyWsHandler {
  net::awaitable<void> operator()(const http::request<http::string_body>&,
                                  tcp::socket) const {
    co_return;
  }
};
struct DummyHttpHandler {
  template <typename Send>
  net::awaitable<void> operator()(http::request<http::string_body>,
                                  Send&&) const {
    co_return;
  }
};
using TestServer =
    HttpServer<BodyReadMode::Eager, DummyHttpHandler, DummyWsHandler>;

// Sets up a local TCP loopback pair. The writer side sends `body` as the body
// of a POST request; the reader side reads only the request headers and then
// co_awaits `fn(stream, buffer, parser)`. Everything runs on the calling
// executor so a single `io_context::run()` drives both sides to completion.
template <typename Fn>
net::awaitable<void> withParsedRequest(std::string body, Fn fn) {
  auto ex = co_await net::this_coro::executor;
  tcp::acceptor acceptor(ex, {tcp::v4(), 0});
  const auto port = acceptor.local_endpoint().port();

  // Use a capture-free IIFE to avoid a GCC 11 ICE with capturing coroutine
  // lambdas defined inside a coroutine.
  net::co_spawn(
      ex,
      [](std::string body, unsigned short port) -> net::awaitable<void> {
        tcp::socket sock(co_await net::this_coro::executor);
        co_await sock.async_connect(
            tcp::endpoint(net::ip::address_v4::loopback(), port),
            net::use_awaitable);
        std::string request = absl::StrCat(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: ",
            body.size(), "\r\n\r\n", body);
        co_await net::async_write(sock, net::buffer(request),
                                  net::use_awaitable);
        sock.shutdown(tcp::socket::shutdown_send);
      }(std::move(body), port),
      net::detached);

  tcp::socket sock = co_await acceptor.async_accept(net::use_awaitable);
  beast::tcp_stream stream(std::move(sock));
  beast::flat_buffer buffer;
  http::request_parser<http::buffer_body> parser;
  parser.body_limit(boost::none);
  co_await http::async_read_header(stream, buffer, parser, net::use_awaitable);
  co_await fn(stream, buffer, parser);
}

// Runs `coro` on a fresh `io_context` to exhaustion on the calling thread.
// Rethrows any exception raised by the coroutine.
void runCoroutine(net::awaitable<void> coro) {
  net::io_context ioc;
  std::exception_ptr eptr;
  net::co_spawn(ioc.get_executor(), std::move(coro),
                [&eptr](std::exception_ptr p) { eptr = p; });
  ioc.run();
  if (eptr) {
    std::rethrow_exception(eptr);
  }
}

}  // namespace

// Test `HttpServer::readIntoBuffer` in isolation, covering three cases:
// body fits in the output buffer (all bytes read); output buffer smaller than
// the body (exactly `bufferSize` bytes read); body and buffer the same size.
TYPED_TEST(HttpServerBodyTest, ReadIntoBuffer) {
  struct Case {
    std::string body;
    size_t bufferSize;
    size_t expectedRead;
    std::string name;
  };
  const std::vector<Case> cases = {
      {"hello", 100, 5, "bodyFitsInBuffer"},
      {std::string(200, 'b'), 50, 50, "bufferSmallerThanBody"},
      {std::string(100, 'c'), 100, 100, "exactFit"},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    runCoroutine(withParsedRequest(
        c.body,
        [expectedBody = c.body.substr(0, c.expectedRead),
         bufferSize = c.bufferSize, expectedRead = c.expectedRead](
            beast::tcp_stream& stream, beast::flat_buffer& buffer,
            http::request_parser<http::buffer_body>& parser)
            -> net::awaitable<void> {
          std::vector<char> out(bufferSize, '\0');
          const size_t n = co_await TestServer::readIntoBuffer(
              stream, buffer, parser, ql::span<char>{out.data(), bufferSize});
          EXPECT_EQ(n, expectedRead);
          EXPECT_EQ((std::string{out.data(), n}), expectedBody);
        }));
  }
}

// Test `HttpServer::materializeBody` in isolation. The internal read chunk is
// `min(bodyLimit, 4096)` bytes, so a body larger than 4096 bytes forces the
// loop to iterate more than once. Cases covered: small body (single chunk,
// body exhausted early); body exactly one chunk; body spanning two chunks
// (loop iterates twice); body truncated by `bodyLimit`; two full chunks.
TYPED_TEST(HttpServerBodyTest, MaterializeBody) {
  struct Case {
    std::string body;
    size_t limit;
    std::string expected;
    std::string name;
  };
  const std::vector<Case> cases = {
      {"hello", 100, "hello", "smallBody"},
      {std::string(4096, 'a'), 4096, std::string(4096, 'a'), "exactOneChunk"},
      {std::string(5000, 'b'), 5000, std::string(5000, 'b'), "loopRequired"},
      {std::string(5000, 'c'), 3000, std::string(3000, 'c'),
       "truncatedByLimit"},
      {std::string(8192, 'd'), 8192, std::string(8192, 'd'), "twoFullChunks"},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    runCoroutine(withParsedRequest(
        c.body,
        [expected = c.expected, limit = c.limit](
            beast::tcp_stream& stream, beast::flat_buffer& buffer,
            http::request_parser<http::buffer_body>& parser)
            -> net::awaitable<void> {
          auto result = co_await TestServer::materializeBody(stream, buffer,
                                                             parser, limit);
          EXPECT_EQ(result, expected);
        }));
  }
}
