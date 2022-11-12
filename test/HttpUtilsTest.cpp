// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de

#include <gtest/gtest.h>

#include <tuple>

#include "util/http/HttpUtils.h"

using ad_utility::httpUtils::getProtocolHostPortTargetFromUrl;

TEST(HttpUtils, getProtocolHostPortTargetFromUrl) {
  auto check = [](const std::string& url, const std::string& protocol,
                  const std::string& host, const std::string& port,
                  const std::string& target) {
    auto protocolHostPortTarget = getProtocolHostPortTargetFromUrl(url);
    ASSERT_EQ(std::get<0>(protocolHostPortTarget), protocol);
    ASSERT_EQ(std::get<1>(protocolHostPortTarget), host);
    ASSERT_EQ(std::get<2>(protocolHostPortTarget), port);
    ASSERT_EQ(std::get<3>(protocolHostPortTarget), target);
  };
  check("http://host.name/tar/get", "http", "host.name", "80", "/tar/get");
  check("https://host.name/tar/get", "https", "host.name", "443", "/tar/get");
  check("http://host.name:81/tar/get", "http", "host.name", "81", "/tar/get");
  check("https://host.name:442/tar/get", "https", "host.name", "442",
        "/tar/get");
  check("http://host.name", "http", "host.name", "80", "");
  check("http://host.name:81", "http", "host.name", "81", "");
  check("https://host.name", "https", "host.name", "443", "");
  check("https://host.name:442", "https", "host.name", "442", "");

  auto assertThrow = [](const std::string& url) {
    ASSERT_THROW(getProtocolHostPortTargetFromUrl(url), std::runtime_error)
        << "url = " << url;
  };
  assertThrow("htt://host.name/tar/get");
  assertThrow("http://host.name:8x/tar/get");
  assertThrow("http://host.name:8x");
}
