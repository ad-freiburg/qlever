// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de

#include <gtest/gtest.h>

#include <tuple>

#include "util/http/HttpUtils.h"

using ad_utility::httpUtils::UrlComponents;

TEST(HttpUtils, constructFromUrl) {
  const auto& HTTP = UrlComponents::Protocol::HTTP;
  const auto& HTTPS = UrlComponents::Protocol::HTTPS;
  auto check = [](const std::string& url,
                  const UrlComponents::Protocol& protocol,
                  const std::string& host, const std::string& port,
                  const std::string& target) {
    UrlComponents uc(url);
    ASSERT_EQ(uc.protocol, protocol);
    ASSERT_EQ(uc.host, host);
    ASSERT_EQ(uc.port, port);
    ASSERT_EQ(uc.target, target);
  };
  check("http://host.name/tar/get", HTTP, "host.name", "80", "/tar/get");
  check("https://host.name/tar/get", HTTPS, "host.name", "443", "/tar/get");
  check("http://host.name:81/tar/get", HTTP, "host.name", "81", "/tar/get");
  check("https://host.name:442/tar/get", HTTPS, "host.name", "442", "/tar/get");
  check("http://host.name", HTTP, "host.name", "80", "");
  check("http://host.name:81", HTTP, "host.name", "81", "");
  check("https://host.name", HTTPS, "host.name", "443", "");
  check("https://host.name:442", HTTPS, "host.name", "442", "");

  using Error = std::runtime_error;
  ASSERT_THROW(UrlComponents("htt://host.name/tar/get"), Error);
  ASSERT_THROW(UrlComponents("http://host.name:8x/tar/get"), Error);
  ASSERT_THROW(UrlComponents("http://host.name:8x"), Error);
}
