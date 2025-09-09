// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <thread>

#include "HttpTestHelpers.h"
#include "global/RuntimeParameters.h"
#include "util/GTestHelpers.h"
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

TEST(HttpServer, HttpTest) {
  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  // This test used to spuriously crash because of something that we (joka92,
  // RobinTF) currently consider to be a bug in Boost::ASIO. (See
  // `util/http/beast.h` for details). Repeat this test several times to make
  // such failures less spurious should they ever reoccur in the future.
  for (size_t k = 0; k < 10; ++k) {
    // Create and run an HTTP server, which replies to each request with three
    // lines: the request method (GET, POST, or OTHER), a copy of the request
    // target (might be empty), and a copy of the request body (might be empty).
    TestHttpServer httpServer([](auto request,
                                 auto&& send) -> boost::asio::awaitable<void> {
      std::string methodName;
      switch (request.method()) {
        case boost::beast::http::verb::get:
          methodName = "GET";
          break;
        case boost::beast::http::verb::post:
          methodName = "POST";
          break;
        default:
          methodName = "OTHER";
      }

      auto response = [](std::string methodName, std::string target,
                         std::string body) -> cppcoro::generator<std::string> {
        co_yield methodName;
        co_yield "\n";
        co_yield target;
        co_yield "\n";
        co_yield body;
      }(methodName, std::string(toStd(request.target())), request.body());

      co_return co_await send(createOkResponse(
          std::move(response), request, ad_utility::MediaType::textPlain));
    });
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

              auto response =
                  HttpClient::sendRequest(std::move(httpClient), verb::get,
                                          "localhost", "target1", handle);
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
      auto response =
          HttpClient::sendRequest(std::move(httpClient), verb::post,
                                  "localhost", "target2", handle, "body2");
      ASSERT_EQ(response.status_, boost::beast::http::status::ok);
      ASSERT_EQ(response.contentType_, "text/plain");
      ASSERT_EQ(toString(std::move(response.body_)), "POST\ntarget2\nbody2");
    }

    // Test if websocket is correctly opened and closed
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

    // Test if websocket is denied on wrong paths
    {
      HttpClient httpClient("localhost", std::to_string(httpServer.getPort()));
      auto response = httpClient.sendWebSocketHandshake(verb::get, "localhost",
                                                        "/other-path");
      // Check for not found error
      ASSERT_EQ(response.base().result(), http::status::not_found);
    }

    // Also test the convenience function `sendHttpOrHttpsRequest` (which
    // creates an own client for each request).
    {
      Url url{
          absl::StrCat("http://localhost:", httpServer.getPort(), "/target")};
      ASSERT_EQ(toString(sendHttpOrHttpsRequest(url, handle, verb::get).body_),
                "GET\n/target\n");
      ASSERT_EQ(
          toString(
              sendHttpOrHttpsRequest(url, handle, verb::post, "body").body_),
          "POST\n/target\nbody");
    }

    // Check that after shutting down, no more new connections are accepted.
    httpServer.shutDown();
    ASSERT_ANY_THROW(
        HttpClient("localhost", std::to_string(httpServer.getPort())));
  }
}

// Test the various `catch` clauses in `HttpServer::session`.
TEST(HttpServer, ErrorHandlingInSession) {
  // We will interfere with the logging to test it, so we have to reset the
  // logging after we are done.
  absl::Cleanup cleanup{
      []() { ad_utility::setGlobalLoggingStream(&std::cout); }};
  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  // Create an HTTP server, which replies to each request with three
  // lines: the request method (GET, POST, or OTHER), a copy of the request
  // target (might be empty), and a copy of the request body (might be empty).
  // After sending the response, the exception denoted by the `exceptionPtr`
  // will be thrown. This exception will be propagated to the
  // `HttpServer::session` method, the exception behavior of which we want to
  // test.
  auto makeHttpServer = [](std::exception_ptr exceptionPtr) {
    AD_CORRECTNESS_CHECK(exceptionPtr);
    return TestHttpServer([exception = std::move(exceptionPtr)](
                              auto request,
                              auto&& send) -> boost::asio::awaitable<void> {
      std::string methodName;
      switch (request.method()) {
        case boost::beast::http::verb::get:
          methodName = "GET";
          break;
        case boost::beast::http::verb::post:
          methodName = "POST";
          break;
        default:
          methodName = "OTHER";
      }

      auto response = [](std::string methodName, std::string target,
                         std::string body) -> cppcoro::generator<std::string> {
        co_yield methodName;
        co_yield "\n";
        co_yield target;
        co_yield "\n";
        co_yield body;
      }(methodName, std::string(toStd(request.target())), request.body());

      // First send a response, to make the client happy.
      co_await send(createOkResponse(std::move(response), request,
                                     ad_utility::MediaType::textPlain));
      // Then throw an exception.
      AD_CORRECTNESS_CHECK(exception);
      std::rethrow_exception(exception);
    });
  };

  // Do the following: Create an HtttpServer using the above method that throws
  // the `exceptionObject` after sending the response. Then send an HTTP request
  // to that server to trigger the exception. Capture the log of the server and
  // return it, s.t. it can be tested.
  auto throwAndCaptureLog = [&makeHttpServer, &handle](auto exceptionObject) {
    // Redirect the log, s.t. we can return it later.
    std::stringstream logStream;
    ad_utility::setGlobalLoggingStream(&logStream);

    // Convert the `exceptionObject` to an `exception_ptr`
    std::exception_ptr exception;
    try {
      throw exceptionObject;
    } catch (...) {
      exception = std::current_exception();
    }

    // Create and run a server and send a request to it. The exception will be
    // caught by the `session` method, and a log statement will be issued
    // depending on the exception. Note: We need a separate server for each
    // test, because we need to shut down the server before extracting its log,
    // otherwise we have a race condition on the logging. An alternative would
    // be to implement a threadsafe `stringstream`, but the chosen approach also
    // works for our purposes.
    auto httpServer = makeHttpServer(exception);
    httpServer.runInOwnThread();
    auto httpClient = std::make_unique<HttpClient>(
        "localhost", std::to_string(httpServer.getPort()));

    auto response = HttpClient::sendRequest(std::move(httpClient), verb::get,
                                            "localhost", "target1", handle);
    // Check the response.
    EXPECT_EQ(response.status_, boost::beast::http::status::ok);
    EXPECT_EQ(response.contentType_, "text/plain");
    EXPECT_EQ(toString(std::move(response.body_)), "GET\ntarget1\n");

    // We need to shut down the server first to not have a race condition on the
    // logging stream.
    httpServer.shutDown();
    // logStream.emit();
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
  EXPECT_THAT(s, AnyOf(HasSubstr("not found"), Eq("")));

  // The `timeout`and `eof` exceptions are only logged in the `TRACE` level,
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
  s = throwAndCaptureLog(std::runtime_error{"The runtime error for testing"});
  EXPECT_THAT(s, HasSubstr("The runtime error for testing"));

  // Thrown object that is not a `std::exception`.
  s = throwAndCaptureLog(47);
  EXPECT_THAT(s,
              HasSubstr("Weird exception not inheriting from std::exception"));
}

// Test the request body size limit
TEST(HttpServer, RequestBodySizeLimit) {
  ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  TestHttpServer httpServer([](auto request,
                               auto&& send) -> boost::asio::awaitable<void> {
    std::string methodName;
    switch (request.method()) {
      case verb::get:
        methodName = "GET";
        break;
      case verb::post:
        methodName = "POST";
        break;
      default:
        methodName = "OTHER";
    }

    auto response = [](std::string methodName,
                       std::string target) -> cppcoro::generator<std::string> {
      co_yield methodName;
      co_yield "\n";
      co_yield target;
    }(methodName, std::string(toStd(request.target())));

    // Send a response.
    co_await send(createOkResponse(std::move(response), request,
                                   ad_utility::MediaType::textPlain));
  });

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

  auto expectRequestFails = [&ResponseMetadata, &expectRequestHelper](
                                const ad_utility::MemorySize& requestBodySize) {
    const ad_utility::MemorySize currentLimit =
        RuntimeParameters().get<"request-body-limit">();
    // For large requests we get an exception while writing to the request
    // stream when going over the limit. For small requests we get the response
    // normally. We would need the HttpClient to return the response even
    // if it couldn't send the request fully in that case.
    expectRequestHelper(
        requestBodySize,
        "Request body size exceeds the allowed size (" +
            currentLimit.asString() +
            "), send a smaller request or set the allowed size via the runtime "
            "parameter `request-body-limit`",
        ResponseMetadata(status::payload_too_large, "text/plain"));
  };
  auto expectRequestSucceeds =
      [&expectRequestHelper,
       &ResponseMetadata](const ad_utility::MemorySize requestBodySize) {
        expectRequestHelper(requestBodySize, "POST\ntarget",
                            ResponseMetadata(status::ok, "text/plain"));
      };
  constexpr auto testingRequestBodyLimit = 50_kB;

  // Set a smaller limit for testing. The default of 100 MB is quite large.
  RuntimeParameters().set("request-body-limit", 50_kB .asString());
  // Requests with bodies smaller than the request body limit are processed.
  expectRequestSucceeds(3_B);
  // Exactly the limit is allowed.
  expectRequestSucceeds(testingRequestBodyLimit);
  // Larger than the limit is forbidden.
  expectRequestFails(testingRequestBodyLimit + 1_B);

  // Setting a smaller request-body limit.
  RuntimeParameters().set("request-body-limit", 1_B .asString());
  expectRequestFails(3_B);
  // Only the request body size counts. The empty body is allowed even if the
  // body is limited to 1 byte.
  expectRequestSucceeds(0_B);

  // Disable the request body limit, by setting it to 0.
  RuntimeParameters().set("request-body-limit", 0_B .asString());
  // Arbitrarily large requests are now allowed.
  expectRequestSucceeds(10_kB);
  expectRequestSucceeds(5_MB);
}
