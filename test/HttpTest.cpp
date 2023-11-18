// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include <thread>

#include "./HttpTestHelpers.h"
#include "absl/strings/str_cat.h"
#include "util/http/HttpClient.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"
#include "util/jthread.h"

using namespace ad_utility::httpUtils;
using namespace boost::beast::http;

TEST(HttpServer, HttpTest) {
  // Create and run an HTTP server, which replies to each request with three
  // lines: the request method (GET, POST, or OTHER), a copy of the request
  // target (might be empty), and a copy of the request body (might be empty).
  TestHttpServer httpServer(
      [](auto request, auto&& send) -> boost::asio::awaitable<void> {
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
        std::string response = absl::StrCat(
            methodName, "\n", toStd(request.target()), "\n", request.body());
        co_return co_await send(createOkResponse(
            response, request, ad_utility::MediaType::textPlain));
      });
  httpServer.runInOwnThread();

  // Create a client, and send a GET and a POST request in one session.
  // The constants in those test loops can be increased to find threading issues
  // using the thread sanitizer. However, these constants can't be higher by
  // default because the checks on GitHub actions will run forwever if they are.
  {
    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < 2; ++i) {
      threads.emplace_back([&]() {
        for (size_t j = 0; j < 5; ++j) {
          {
            HttpClient httpClient("localhost",
                                  std::to_string(httpServer.getPort()));
            ASSERT_EQ(
                httpClient.sendRequest(verb::get, "localhost", "target1").str(),
                "GET\ntarget1\n");
            ASSERT_EQ(
                httpClient
                    .sendRequest(verb::post, "localhost", "target1", "body1")
                    .str(),
                "POST\ntarget1\nbody1");
          }
        }
      });
    }
  }

  // Do the same thing in a second session (to check if everything is still fine
  // with the server after we have communicated with it for one session).
  {
    HttpClient httpClient("localhost", std::to_string(httpServer.getPort()));
    ASSERT_EQ(httpClient.sendRequest(verb::get, "localhost", "target2").str(),
              "GET\ntarget2\n");
    ASSERT_EQ(
        httpClient.sendRequest(verb::post, "localhost", "target2", "body2")
            .str(),
        "POST\ntarget2\nbody2");
  }

  // Test if websocket is correctly opened and closed
  {
    HttpClient httpClient("localhost", std::to_string(httpServer.getPort()));
    auto response = httpClient.sendWebSocketHandshake(verb::get, "localhost",
                                                      "/watch/some-id");
    // verify request is upgraded
    ASSERT_EQ(response.base().result(), http::status::switching_protocols);
  }

  // Test if websocket is denied on wrong paths
  {
    HttpClient httpClient("localhost", std::to_string(httpServer.getPort()));
    auto response = httpClient.sendWebSocketHandshake(verb::get, "localhost",
                                                      "/other-path");
    // Check for not found error
    ASSERT_EQ(response.base().result(), http::status::not_found);
  }

  // Also test the convenience function `sendHttpOrHttpsRequest` (which creates
  // an own client for each request).
  {
    Url url{absl::StrCat("http://localhost:", httpServer.getPort(), "/target")};
    ASSERT_EQ(sendHttpOrHttpsRequest(url, verb::get).str(), "GET\n/target\n");
    ASSERT_EQ(sendHttpOrHttpsRequest(url, verb::post, "body").str(),
              "POST\n/target\nbody");
  }

  // Check that after shutting down, no more new connections are accepted.
  httpServer.shutDown();
  ASSERT_ANY_THROW(
      HttpClient("localhost", std::to_string(httpServer.getPort())));
}
