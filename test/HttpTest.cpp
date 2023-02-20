// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "./HttpTestHelpers.h"
#include "absl/strings/str_cat.h"
#include "util/http/HttpClient.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"

using namespace ad_utility::httpUtils;

TEST(HttpServer, HttpTest) {
  // Create and run a HTTP server, which replies to each request with three
  // lines: the request method (GET, POST, or OTHER), a copy of the request
  // target (might be empty), and a copy of the request body (might be empty).
  TestHttpServer httpServer([](auto request,
                               auto&& send) -> boost::asio::awaitable<void> {
    std::string methodName;
    switch (request.method()) {
      case http::verb::get:
        methodName = "GET";
        break;
      case http::verb::post:
        methodName = "POST";
        break;
      default:
        methodName = "OTHER";
    }
    std::string response =
        absl::StrCat(methodName, "\n", request.target(), "\n", request.body());
    co_return co_await send(
        createOkResponse(response, request, ad_utility::MediaType::textPlain));
  });
  httpServer.runInOwnThread();

  // Helper lambdas for testing GET and POST requests.
  auto testGetRequest = [](HttpClient* httpClient, const std::string& target) {
    std::istringstream response = httpClient->sendRequest(
        boost::beast::http::verb::get, "localhost", target);
    ASSERT_EQ(response.str(), absl::StrCat("GET\n", target, "\n"));
  };
  auto testPostRequest = [](HttpClient* httpClient, const std::string& target,
                            const std::string& body) {
    std::istringstream response = httpClient->sendRequest(
        boost::beast::http::verb::post, "localhost", target, body);
    ASSERT_EQ(response.str(), absl::StrCat("POST\n", target, "\n", body));
  };

  // First session (checks whether client and server can communicate as they
  // should).
  {
    HttpClient httpClient("localhost", std::to_string(httpServer.getPort()));
    testGetRequest(&httpClient, "target1");
    testPostRequest(&httpClient, "target1", "body1");
  }

  // Second session (checks if everything is still fine with the server after we
  // have communicated with it for one session).
  {
    HttpClient httpClient("localhost", std::to_string(httpServer.getPort()));
    testGetRequest(&httpClient, "target2");
    testPostRequest(&httpClient, "target2", "body2");
  }

  // Third session (check that after shutting down, no more new connections are
  // being accepted.
  // httpServer->shutDown();
  httpServer.shutDown();
  ASSERT_THROW(HttpClient("localhost", std::to_string(httpServer.getPort())),
               std::exception);
}
