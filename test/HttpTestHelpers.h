// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "util/http/HttpClient.h"
#include "util/http/HttpServer.h"
#include "util/http/HttpUtils.h"
#include "util/jthread.h"

using namespace std::literals;

// Test HTTP Server.
template <typename HttpHandler>
class TestHttpServer {
 private:
  // The server.
  std::shared_ptr<HttpServer<HttpHandler>> server_;

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
    const std::string& ipAddress = "0.0.0.0";
    int numServerThreads = 1;
    server_ = std::make_shared<HttpServer<HttpHandler>>(
        0, ipAddress, numServerThreads, std::move(httpHandler));
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
    std::exception_ptr exception_ptr = nullptr;
    std::shared_ptr<HttpServer<HttpHandler>> serverCopy = server_;
    serverThread_ =
        std::make_unique<ad_utility::JThread>([&exception_ptr, serverCopy]() {
          try {
            serverCopy->run();
          } catch (...) {
            exception_ptr = std::current_exception();
          }
        });
    auto waitTimeUntilServerIsUp = 100ms;
    std::this_thread::sleep_for(waitTimeUntilServerIsUp);
    if (exception_ptr || !server_->serverIsReady()) {
      // Detach the server thread (the `run()` above never returns), so that we
      // can exit this test.
      serverThread_->detach();
      if (exception_ptr) {
        std::rethrow_exception(exception_ptr);
      } else {
        throw std::runtime_error(absl::StrCat("HttpServer was not up after ",
                                              waitTimeUntilServerIsUp.count(),
                                              "ms, this should not happen"));
      }
    }
  }

  // Shut down the server explicitly (needed in `HttpTest`).
  //
  // NOTE: This works by causing the `httpServer->run()` running in the server
  // thread to return, so that the thread can complete.
  void shutDown() {
    if (!hasBeenShutDown_) {
      server_->shutDown();
      hasBeenShutDown_ = true;
    }
  }

  // Since we detach the server thread in `runInOwnThread`, we need to make sure
  // that the server is always shut down when this object does out of scope.
  ~TestHttpServer() { shutDown(); }
};
