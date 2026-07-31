//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/http/HttpProxyConfig.h"

using namespace ad_utility::httpProxy;
using ::testing::HasSubstr;
using ::testing::Optional;

namespace {
// Matcher for a `Proxy` with the given host and port and no authentication.
auto isProxy(std::string_view host, std::string_view port) {
  return Optional(Proxy{std::string{host}, std::string{port}, ""});
}
}  // namespace

// A fixture that clears the proxy-related environment variables, so that the
// tests are independent of the environment they happen to run in, and restores
// them afterwards. Note that `HTTP_PROXY` is cleared as well, although it is
// never read, so that `uppercaseHttpProxyIsIgnored` also holds when it happens
// to be set in the environment.
class ProxyEnvironmentTest : public ::testing::Test {
 private:
  static constexpr std::array variableNames_{"http_proxy", "HTTP_PROXY"};
  std::vector<std::pair<std::string, std::string>> savedValues_;

 protected:
  void SetUp() override {
    for (const char* name : variableNames_) {
      if (const char* value = std::getenv(name); value != nullptr) {
        savedValues_.emplace_back(name, value);
      }
      ASSERT_EQ(::unsetenv(name), 0);
    }
  }

  void TearDown() override {
    for (const char* name : variableNames_) {
      ::unsetenv(name);
    }
    for (const auto& [name, value] : savedValues_) {
      ::setenv(name.c_str(), value.c_str(), 1);
    }
  }

  static void setEnv(const char* name, const char* value) {
    ASSERT_EQ(::setenv(name, value, 1), 0);
  }
};

// _____________________________________________________________________________
TEST(ParseProxyUrl, emptyValueMeansNoProxy) {
  EXPECT_EQ(parseProxyUrl(""), std::nullopt);
  EXPECT_EQ(parseProxyUrl("   "), std::nullopt);
  EXPECT_EQ(parseProxyUrl("\t\n"), std::nullopt);
}

// _____________________________________________________________________________
TEST(ParseProxyUrl, hostAndPort) {
  EXPECT_THAT(parseProxyUrl("http://proxy.example.org:3128"),
              isProxy("proxy.example.org", "3128"));
  // The scheme may be omitted.
  EXPECT_THAT(parseProxyUrl("proxy.example.org:3128"),
              isProxy("proxy.example.org", "3128"));
  // Surrounding whitespace is ignored.
  EXPECT_THAT(parseProxyUrl("  http://proxy.example.org:3128  "),
              isProxy("proxy.example.org", "3128"));
  // A trailing slash is a valid empty path.
  EXPECT_THAT(parseProxyUrl("http://proxy.example.org:3128/"),
              isProxy("proxy.example.org", "3128"));
  // An IP address works as well.
  EXPECT_THAT(parseProxyUrl("10.0.0.1:8080"), isProxy("10.0.0.1", "8080"));
}

// _____________________________________________________________________________
TEST(ParseProxyUrl, portDefaultsTo80) {
  EXPECT_THAT(parseProxyUrl("http://proxy.example.org"),
              isProxy("proxy.example.org", "80"));
  EXPECT_THAT(parseProxyUrl("proxy.example.org"),
              isProxy("proxy.example.org", "80"));
}

// _____________________________________________________________________________
TEST(ParseProxyUrl, credentialsBecomeBasicAuthorization) {
  auto proxy = parseProxyUrl("http://user:password@proxy.example.org:3128");
  ASSERT_TRUE(proxy.has_value());
  EXPECT_EQ(proxy->host_, "proxy.example.org");
  EXPECT_EQ(proxy->port_, "3128");
  EXPECT_EQ(proxy->authorization_,
            absl::StrCat("Basic ", absl::Base64Escape("user:password")));

  // A user without a password is allowed, and yields an empty password.
  auto userOnly = parseProxyUrl("http://user@proxy.example.org:3128");
  ASSERT_TRUE(userOnly.has_value());
  EXPECT_EQ(userOnly->authorization_,
            absl::StrCat("Basic ", absl::Base64Escape("user:")));

  // Percent-encoded credentials are decoded, so that a password containing
  // special characters (like `@` or `:`) can be expressed.
  auto encoded = parseProxyUrl("http://user%40realm:p%3Ass@proxy:3128");
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(encoded->authorization_,
            absl::StrCat("Basic ", absl::Base64Escape("user@realm:p:ss")));
}

// _____________________________________________________________________________
TEST(ParseProxyUrl, malformedUrlsAreRejected) {
  AD_EXPECT_THROW_WITH_MESSAGE(parseProxyUrl("https://proxy:3128"),
                               HasSubstr("only supports plain HTTP proxies"));
  AD_EXPECT_THROW_WITH_MESSAGE(parseProxyUrl("socks5://proxy:1080"),
                               HasSubstr("only supports plain HTTP proxies"));
  AD_EXPECT_THROW_WITH_MESSAGE(parseProxyUrl("http://proxy:3128/some/path"),
                               HasSubstr("must not have a path"));
  AD_EXPECT_THROW_WITH_MESSAGE(parseProxyUrl("http://proxy:3128?a=b"),
                               HasSubstr("must not have a query or fragment"));
  AD_EXPECT_THROW_WITH_MESSAGE(parseProxyUrl("http://proxy:3128#frag"),
                               HasSubstr("must not have a query or fragment"));
  AD_EXPECT_THROW_WITH_MESSAGE(parseProxyUrl("http://"),
                               HasSubstr("does not specify a host"));
  AD_EXPECT_THROW_WITH_MESSAGE(parseProxyUrl("http://proxy:not-a-port"),
                               HasSubstr("could not be parsed"));
}

// _____________________________________________________________________________
TEST(Proxy, asStringForLoggingHidesCredentials) {
  EXPECT_EQ(parseProxyUrl("http://proxy:3128")->asStringForLogging(),
            "proxy:3128");
  // The default port is spelled out, so that the log is unambiguous.
  EXPECT_EQ(parseProxyUrl("proxy")->asStringForLogging(), "proxy:80");

  std::string withAuth =
      parseProxyUrl("http://user:secret@proxy:3128")->asStringForLogging();
  EXPECT_THAT(withAuth, HasSubstr("proxy:3128"));
  EXPECT_THAT(withAuth, HasSubstr("(with authentication)"));
  // Neither the password nor its encoding may appear in the log.
  EXPECT_THAT(withAuth, ::testing::Not(HasSubstr("secret")));
  EXPECT_THAT(withAuth,
              ::testing::Not(HasSubstr(absl::Base64Escape("user:secret"))));
}

// _____________________________________________________________________________
TEST(AbsoluteFormTarget, portIsOmittedOnlyForTheDefault) {
  EXPECT_EQ(absoluteFormTarget("example.org", "8080", "/sparql?query=X"),
            "http://example.org:8080/sparql?query=X");
  EXPECT_EQ(absoluteFormTarget("example.org", "80", "/sparql"),
            "http://example.org/sparql");
  // The target must be in origin form.
  EXPECT_ANY_THROW(absoluteFormTarget("example.org", "80", "sparql"));
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, unsetOrEmptyMeansNoProxy) {
  EXPECT_EQ(proxyFromEnvironment(), std::nullopt);
  // Setting the variable to the empty string is the conventional way of undoing
  // a proxy setting inherited from a parent process.
  setEnv("http_proxy", "");
  EXPECT_EQ(proxyFromEnvironment(), std::nullopt);
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, proxyIsReadFromHttpProxy) {
  setEnv("http_proxy", "http://proxy:3128");
  EXPECT_THAT(proxyFromEnvironment(), isProxy("proxy", "3128"));
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, uppercaseHttpProxyIsIgnored) {
  // `HTTP_PROXY` is deliberately not honored, following `curl`: in a CGI
  // environment it would be settable by a remote client via the `Proxy:`
  // request header.
  setEnv("HTTP_PROXY", "http://should-be-ignored:3128");
  EXPECT_EQ(proxyFromEnvironment(), std::nullopt);
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, malformedEnvironmentValueThrows) {
  setEnv("http_proxy", "socks5://proxy:1080");
  AD_EXPECT_THROW_WITH_MESSAGE(proxyFromEnvironment(),
                               HasSubstr("only supports plain HTTP proxies"));
}

// _____________________________________________________________________________
// Note: this is the only test that touches `globalProxy()`, which is important
// because it reads the environment only once per process.
TEST_F(ProxyEnvironmentTest, globalProxyIsReadOnlyOnce) {
  setEnv("http_proxy", "http://proxy:3128");
  EXPECT_THAT(globalProxy(), isProxy("proxy", "3128"));
  // A later change of the environment has no effect anymore.
  setEnv("http_proxy", "http://other-proxy:3129");
  EXPECT_THAT(globalProxy(), isProxy("proxy", "3128"));
  EXPECT_THAT(proxyFromEnvironment(), isProxy("other-proxy", "3129"));
}
