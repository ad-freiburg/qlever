// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
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
  // Set up a HTTP server and run it. Try out 10 different ports, if connection
  // to all of them fail, the test fails.
  //
  // TODO: Is there a more robust way to do this? Should we try out more ports?
  auto httpServer = [&mirroringHttpSessionHandler]() {
    std::vector<short unsigned int> ports(10);
    std::generate(ports.begin(), ports.end(),
                  []() { return 1024 + std::rand() % (65535 - 1024); });
    const std::string& ipAddress = "0.0.0.0";
    int numServerThreads = 1;
    for (const short unsigned int port : ports) {
      try {
        using Server = HttpServer<decltype(mirroringHttpSessionHandler)>;
        return std::make_shared<Server>(port, ipAddress, numServerThreads,
                                        mirroringHttpSessionHandler);
      } catch (const boost::system::system_error& b) {
        LOG(INFO) << "Starting test HTTP server on port " << port
                  << " failed, trying next port ..." << std::endl;
      }
    }
    throw std::runtime_error(
        absl::StrCat("Could not start test HTTP server on any of these ports: ",
                     absl::StrJoin(ports, ", ")));
  }();

  // Run the server in its own thread. Wait for 100ms until the server is
  // up (it should be up immediately).
  //
  // NOTE: It is important to *copy* the `httpServer` pointer into the thread.
  // That way, whoever dies first (this thread or the `httpServerThread`), the
  // pointer is still valid in the other thread.
  std::jthread httpServerThread([httpServer]() { httpServer->run(); });
  auto waitTimeUntilServerIsUp = 100ms;
  std::this_thread::sleep_for(waitTimeUntilServerIsUp);
  if (!httpServer->serverIsReady()) {
    // Detach the server thread (the `run()` above never terminates), so that we
    // can exit this test.
    httpServerThread.detach();
    throw std::runtime_error(absl::StrCat("HttpServer was not up after ",
                                          waitTimeUntilServerIsUp.count(),
                                          "ms, this should not happen"));
  }

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
    HttpClient httpClient("localhost", std::to_string(httpServer->getPort()));
    testGetRequest(&httpClient, "target1");
    testPostRequest(&httpClient, "target1", "body1");
  }

  // Second session (checks if everything is still fine with the server after we
  // have communicated with it for one session).
  {
    HttpClient httpClient("localhost", std::to_string(httpServer->getPort()));
    testGetRequest(&httpClient, "target2");
    testPostRequest(&httpClient, "target2", "body2");
  }

  // Third session (check that after shutting down, no more new connections are
  // being accepted.
  httpServer->shutDown();
  ASSERT_THROW(HttpClient("localhost", std::to_string(httpServer->getPort())),
               std::exception);
}
