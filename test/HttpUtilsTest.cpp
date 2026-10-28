// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de

#include <gtest/gtest.h>

#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <tuple>

#include "util/http/HttpUtils.h"
#include "util/http/beast.h"

using ad_utility::httpUtils::Url;

// ___________________________________________________________________________
TEST(HttpUtils, Url) {
  const auto& HTTP = Url::Protocol::HTTP;
  const auto& HTTPS = Url::Protocol::HTTPS;
  auto check = [](const std::string& urlString, const Url::Protocol& protocol,
                  const std::string& host, const std::string& port,
                  const std::string& target) {
    Url url(urlString);
    ASSERT_EQ(url.protocol(), protocol);
    ASSERT_EQ(url.host(), host);
    ASSERT_EQ(url.port(), port);
    ASSERT_EQ(url.target(), target);
  };
  check("http://host.name/tar/get", HTTP, "host.name", "80", "/tar/get");
  check("https://host.name/tar/get", HTTPS, "host.name", "443", "/tar/get");
  check("http://host.name:81/tar/get", HTTP, "host.name", "81", "/tar/get");
  check("https://host.name:442/tar/get", HTTPS, "host.name", "442", "/tar/get");
  check("http://host.name", HTTP, "host.name", "80", "/");
  check("http://host.name:81", HTTP, "host.name", "81", "/");
  check("https://host.name", HTTPS, "host.name", "443", "/");
  check("https://host.name:442", HTTPS, "host.name", "442", "/");

  ASSERT_EQ(Url("http://bla").protocolAsString(), "http");
  ASSERT_EQ(Url("https://bla").protocolAsString(), "https");

  ASSERT_EQ(Url("http://bla/bli").asString(), "http://bla:80/bli");
  ASSERT_EQ(Url("https://bla:81/bli").asString(), "https://bla:81/bli");

  ASSERT_ANY_THROW(Url("htt://host.name/tar/get"));
  ASSERT_ANY_THROW(Url("http://host.name:8x/tar/get"));
  ASSERT_ANY_THROW(Url("http://host.name:8x"));
}

// ___________________________________________________________________________
TEST(HttpUtils, GetHeaderOnlyRequest) {
  namespace http = boost::beast::http;
  http::request<http::string_body> original;
  original.method(http::verb::post);
  original.target("/api/test");
  original.version(11);
  original.keep_alive(true);
  original.set(http::field::content_type, "text/plain");
  original.set(http::field::authorization, "Bearer token123");
  original.body() = "some body that must not appear in the header-only copy";

  auto headerOnly = ad_utility::httpUtils::getHeaderOnlyRequest(original);

  EXPECT_EQ(headerOnly.method(), http::verb::post);
  EXPECT_EQ(headerOnly.target(), "/api/test");
  EXPECT_EQ(headerOnly.version(), 11);
  EXPECT_TRUE(headerOnly.keep_alive());
  EXPECT_EQ(headerOnly.at(http::field::content_type), "text/plain");
  EXPECT_EQ(headerOnly.at(http::field::authorization), "Bearer token123");
}

// ___________________________________________________________________________
TEST(HttpUtils, GetStringBodyRequest) {
  namespace http = boost::beast::http;
  http::request<http::string_body> original;
  original.method(http::verb::post);
  original.target("/api/test");
  original.version(11);
  original.keep_alive(true);
  original.set(http::field::content_type, "text/plain");
  original.set(http::field::authorization, "Bearer token123");
  original.body() = "original body (must not appear in result)";

  auto result =
      ad_utility::httpUtils::getStringBodyRequest(original, "new body");

  EXPECT_EQ(result.method(), http::verb::post);
  EXPECT_EQ(result.target(), "/api/test");
  EXPECT_EQ(result.version(), 11);
  EXPECT_TRUE(result.keep_alive());
  EXPECT_EQ(result.at(http::field::content_type), "text/plain");
  EXPECT_EQ(result.at(http::field::authorization), "Bearer token123");
  EXPECT_EQ(result.body(), "new body");
}

// Test that `createOkResponse` keeps a generator as the body of the response
// and compresses it if the request accepts `gzip`.
TEST(HttpUtils, CreateOkResponseFromGeneratorWithCompression) {
  namespace http = boost::beast::http;
  using ad_utility::MediaType;
  auto generator = []() -> cppcoro::generator<std::string> {
    co_yield "Hello, ";
    co_yield "World!";
  };
  auto bodyToString = [](auto& body) {
    std::string result;
    for (const auto& chunk : body) {
      result += chunk;
    }
    return result;
  };
  http::request<http::string_body> request;

  // Without an `Accept-Encoding` header, the body is not compressed.
  auto response = ad_utility::httpUtils::createOkResponse(generator(), request,
                                                          MediaType::textPlain);
  EXPECT_EQ(response.count(http::field::content_encoding), 0);
  EXPECT_EQ(bodyToString(response.body()), "Hello, World!");

  // With `Accept-Encoding: gzip`, the body is compressed.
  request.set(http::field::accept_encoding, "gzip");
  response = ad_utility::httpUtils::createOkResponse(generator(), request,
                                                     MediaType::textPlain);
  EXPECT_EQ(response.at(http::field::content_encoding), "gzip");
  std::string decompressed;
  {
    namespace io = boost::iostreams;
    io::filtering_ostream decompressor;
    decompressor.push(io::gzip_decompressor());
    decompressor.push(io::back_inserter(decompressed));
    decompressor << bodyToString(response.body());
  }
  EXPECT_EQ(decompressed, "Hello, World!");
}
