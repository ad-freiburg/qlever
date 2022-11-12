// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "absl/strings/str_cat.h"
#include "util/http/HttpClient.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"

using namespace ad_utility::httpUtils;
using namespace std::literals;

TEST(HttpServer, HttpTest) {
  // A simple HTTP session handler, which replies with three lines: the request
  // method (GET, POST, or OTHER), a copy of the request target (might be
  // empty), and a copy of the request body (might be empty).
  auto mirroringHttpSessionHandler =
      [](auto request, auto&& send) -> boost::asio::awaitable<void> {
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
  };
  // Set up a HTTP server and run it.
  // TODO: Try out random ports until we find a free one.
  short unsigned int port = 37654;
  const std::string& ipAdress = "0.0.0.0";
  int numServerThreads = 1;
  auto httpServer = HttpServer{port, ipAdress, numServerThreads,
                               std::move(mirroringHttpSessionHandler)};
  std::jthread httpServerThread([&]() { httpServer.run(); });

  // TODO: Without the `sleep_for (1ms is also enough on my machine)`, the
  // listener exits its loop before waiting for the first request, and the
  // following request hangs.  What would be a more robust way of closing down
  // the server?
  std::this_thread::sleep_for(50ms);
  httpServer.exitServerLoopAfterNextRequest();
  HttpClient httpClient;

  // Helper lambdas for testing GET and POST requests.
  auto testGetRequest = [&httpClient](const std::string& target) {
    std::istringstream response = httpClient.sendRequest(
        boost::beast::http::verb::get, "localhost", target);
    ASSERT_EQ(response.str(), absl::StrCat("GET\n", target, "\n"));
  };
  auto testPostRequest = [&httpClient](const std::string& target,
                                       const std::string& body) {
    std::istringstream response = httpClient.sendRequest(
        boost::beast::http::verb::post, "localhost", target, body);
    ASSERT_EQ(response.str(), absl::StrCat("POST\n", target, "\n", body));
  };

  // Send a GET request and a POST request via the same connection (one after
  // the other).
  httpClient.openStream("localhost", std::to_string(port));
  testGetRequest("target");
  testPostRequest("target", "body");
  httpClient.closeStream();

  // Sending a request after the connection is closed should fail.
  ASSERT_THROW(testGetRequest("target"), std::exception);
  ASSERT_THROW(testPostRequest("target", "body"), std::exception);
}
