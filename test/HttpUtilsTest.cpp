// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de

#include <gtest/gtest.h>

#include <tuple>

#include "util/http/HttpUtils.h"

using ad_utility::httpUtils::Url;

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
