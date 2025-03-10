// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Moritz Dom (domm@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>

#include "HttpTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/http/websocket/WebSocketClient.h"

namespace ad_utility::websocket {
// _____________________________________________________________________________
TEST(WebSocketClient, HttpConnection) {
  {
    HttpWebSocketClient w("http://invalid.hostname", "443", "/");

    // 0. Close without established connection.
    EXPECT_NO_THROW(w.close());
    EXPECT_NO_THROW(w.readMessages());

    // 1. Can't resolve hostname
    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(w.stream_.is_open());
  }

  auto httpServer =
      TestHttpServer([]([[maybe_unused]] auto _, [[maybe_unused]] auto&& _1)
                         -> boost::asio::awaitable<void> { co_return; });

  // 2. Connection failed (server not running)
  {
    HttpWebSocketClient w("localhost", std::to_string(httpServer.getPort()),
                          "/");
    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(w.stream_.is_open());
  }

  httpServer.runInOwnThread();

  // 3. WebSocket handshake failed (wrong path)
  {
    HttpWebSocketClient w("localhost", std::to_string(httpServer.getPort()),
                          "/wrong-path");
    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(w.stream_.is_open());
  }

  // 4. WebSocket handshake worked
  {
    HttpWebSocketClient w("localhost", std::to_string(httpServer.getPort()),
                          absl::StrCat(WEBSOCKET_PATH, "some-id"));
    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(w.stream_.is_open());
    EXPECT_TRUE(w.ioThread_.joinable());

    w.close();
    EXPECT_FALSE(w.stream_.is_open());
    EXPECT_FALSE(w.ioThread_.joinable());
  }

  httpServer.shutDown();
}

// _____________________________________________________________________________
TEST(WebSocketClient, concatUrlPaths) {
  EXPECT_ANY_THROW(concatUrlPaths("", "/path"));
  EXPECT_ANY_THROW(concatUrlPaths("/url", ""));
  EXPECT_ANY_THROW(concatUrlPaths("url", "/path"));
  EXPECT_ANY_THROW(concatUrlPaths("/url", "path"));
  EXPECT_EQ(concatUrlPaths("/url", "/path"), "/url/path");
  EXPECT_EQ(concatUrlPaths("/url/", "/path"), "/url/path");
}

}  // namespace ad_utility::websocket
