//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/http/websocket/WebSocketSession.h"

using ad_utility::websocket::WebSocketSession;
namespace net = boost::asio;
namespace http = boost::beast::http;

http::request<http::string_body> withPath(std::string_view path) {
  http::request<http::string_body> request{};
  request.target(path);
  return request;
}

TEST(WebSocketSession, EnsureCorrectPathAcceptAndRejectBehaviour) {
  auto testFunc = WebSocketSession::getErrorResponseIfPathIsInvalid;

  EXPECT_EQ(std::nullopt, testFunc(withPath("/watch/some-id")));
  EXPECT_EQ(std::nullopt, testFunc(withPath("/watch/😇")));

  auto assertNotFoundResponse = [](auto response) {
    ASSERT_TRUE(response.has_value());
    EXPECT_EQ(response->result(), http::status::not_found);
  };

  assertNotFoundResponse(testFunc(withPath("")));
  assertNotFoundResponse(testFunc(withPath("/")));
  assertNotFoundResponse(testFunc(withPath("/watch")));
  assertNotFoundResponse(testFunc(withPath("/watch/")));
  assertNotFoundResponse(testFunc(withPath("/watch//")));
  assertNotFoundResponse(testFunc(withPath("/watch///")));
  assertNotFoundResponse(testFunc(withPath("/watch/trailing-slash/")));
  assertNotFoundResponse(testFunc(withPath("/other-prefix/some-id")));
}
