// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#ifndef QLEVER_TEST_HTTPTESTHELPERS_H
#define QLEVER_TEST_HTTPTESTHELPERS_H

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "util/http/HttpClient.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"
#include "util/http/websocket/QueryId.h"
#include "util/jthread.h"

using namespace std::literals;

// Test HTTP Server.
template <typename HttpHandler>
class TestHttpServer {
 private:
  static constexpr auto webSocketSessionSupplier =
      [](net::io_context& ioContext) {
        using namespace ad_utility::websocket;
        return [queryHub = QueryHub{ioContext}, registry = QueryRegistry{}](
                   const http::request<http::string_body>& request,
                   tcp::socket socket) mutable {
          return WebSocketSession::handleSession(queryHub, registry, request,
                                                 std::move(socket));
        };
      };
  using WebSocketHandlerType =
      decltype(webSocketSessionSupplier(std::declval<net::io_context&>()));

  // The server.
  std::shared_ptr<HttpServer<HttpHandler, WebSocketHandlerType>> server_;

  // The own thread in which the server is running.
  //
  // NOTE: It is important that this thread object lives as long as the server
  // lives. If it would be destroyed while the server is still running, the
  // program would hang because the thread would wait for the server to exit.
  std::unique_ptr<ad_utility::JThread> serverThread_;

  // Indicator whether the server has been shut down. We need this because
  // `HttpServer::shutDown` must only be called once.
  std::atomic<bool> hasBeenShutDown_ = false;

 public:
  // Create server on localhost. Port 0 instructs the operating system to choose
  // a free port of its choice.
  explicit TestHttpServer(HttpHandler httpHandler) {
    server_ = std::make_shared<HttpServer<HttpHandler, WebSocketHandlerType>>(
        0, "0.0.0.0", 1, std::move(httpHandler), webSocketSessionSupplier);
  }

  // Get port on which this server is running.
  auto getPort() const { return server_->getPort(); }

  // Run the server in its own thread. Wait for 100ms until the server is up and
  // throw `std::runtime_error` if it's not (it should be up immediately).
  //
  // NOTE 1: It is important to *copy* the `httpServer` shared pointer into the
  // thread. That way, no matter which thread completes first (this thread or
  // `httpServerThread`), the `httpServer` is still alive and can be used in the
  // other thread.
  //
  // NOTE 2: Catch all exceptions in the server thread so that if an exception
  // is thrown, the whole process does not simply terminate.
  void runInOwnThread() {
    auto runnerTask =
        std::packaged_task<void()>{[server = server_]() { server->run(); }};
    auto runnerFuture = runnerTask.get_future();
    serverThread_ =
        std::make_unique<ad_utility::JThread>(std::move(runnerTask));
    auto waitTimeUntilServerIsUp = 100ms;
    auto status = runnerFuture.wait_for(waitTimeUntilServerIsUp);
    if (status == std::future_status::ready) {
      // Propagate exception
      runnerFuture.get();
    }
    if (!server_->serverIsReady()) {
      // Detach the server thread (the `run()` above never returns), so that we
      // can exit this test.
      serverThread_->detach();
      throw std::runtime_error(absl::StrCat("HttpServer was not up after ",
                                            waitTimeUntilServerIsUp.count(),
                                            "ms, this should not happen"));
    }
  }

  // Shut down the server explicitly (needed in `HttpTest`).
  //
  // NOTE: This works by causing the `httpServer->run()` running in the server
  // thread to return, so that the thread can complete.
  void shutDown() {
    if (server_->serverIsReady() && !hasBeenShutDown_.exchange(true)) {
      server_->shutDown();
    }
  }

  // Since we detach the server thread in `runInOwnThread`, we need to make sure
  // that the server is always shut down when this object does out of scope.
  ~TestHttpServer() { shutDown(); }
};

#endif  // QLEVER_TEST_HTTPTESTHELPERS_H
