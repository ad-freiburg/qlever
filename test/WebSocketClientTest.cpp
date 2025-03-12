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
  auto isConnected = []<typename T>(const T& w) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return w.isConnected_.load();
  };

  // 0. Close without established connection.
  {
    HttpWebSocketClient w("localhost", "1", "/");
    EXPECT_NO_THROW(w.close());
    EXPECT_NO_THROW(w.readMessages());
  }

  // 1. Can't resolve hostname
  {
    HttpWebSocketClient w("invalid.hostname", "9999", "/");
    w.start();
    EXPECT_FALSE(isConnected(w));
  }

  auto httpServer =
      TestHttpServer([]([[maybe_unused]] auto _, [[maybe_unused]] auto&& _1)
                         -> boost::asio::awaitable<void> { co_return; });

  // 2. Connection failed (server not running)
  {
    auto check = [&](std::string_view s) {
      auto w = getWebSocketClient(
          ad_utility::httpUtils::Url(s), "/",
          []([[maybe_unused]] const std::string& _) { return _; });

      std::visit([&](auto& client) { EXPECT_FALSE(isConnected(*client)); }, w);
    };
    check("http://localhost:" + std::to_string(httpServer.getPort()) + "/");
    check("https://localhost:" + std::to_string(httpServer.getPort()) + "/");
  }

  httpServer.runInOwnThread();

  // 3. WebSocket handshake failed (wrong path)
  {
    HttpWebSocketClient w("localhost", std::to_string(httpServer.getPort()),
                          "/wrong-path");
    w.start();
    EXPECT_FALSE(isConnected(w));
  }

  // 4. WebSocket handshake worked
  {
    HttpWebSocketClient w("localhost", std::to_string(httpServer.getPort()),
                          absl::StrCat(WEBSOCKET_PATH, "some-id"));
    w.start();
    EXPECT_TRUE(isConnected(w));

    w.close();
    EXPECT_FALSE(isConnected(w));
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
