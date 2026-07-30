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
#include <string>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/http/HttpProxyConfig.h"

using namespace ad_utility::httpProxy;
using ::testing::HasSubstr;
using ::testing::Optional;

namespace {
// The environment variables that `ProxyConfiguration::fromEnvironment` looks
// at.
constexpr std::array proxyEnvironmentVariables{
    "http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY",
    "all_proxy",  "ALL_PROXY",  "no_proxy",    "NO_PROXY"};

// Matcher for a `Proxy` with the given host and port and no authentication.
auto isProxy(std::string_view host, std::string_view port) {
  return Optional(Proxy{std::string{host}, std::string{port}, ""});
}
}  // namespace

// A fixture that clears all proxy-related environment variables, so that the
// tests are independent of the environment they happen to run in, and restores
// them afterwards.
class ProxyEnvironmentTest : public ::testing::Test {
 private:
  std::vector<std::pair<std::string, std::string>> savedValues_;

 protected:
  void SetUp() override {
    for (const char* name : proxyEnvironmentVariables) {
      if (const char* value = std::getenv(name); value != nullptr) {
        savedValues_.emplace_back(name, value);
      }
      ASSERT_EQ(::unsetenv(name), 0);
    }
  }

  void TearDown() override {
    for (const char* name : proxyEnvironmentVariables) {
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
TEST(IsExcludedByNoProxy, exactAndSuffixMatches) {
  EXPECT_TRUE(isExcludedByNoProxy("example.org", "example.org"));
  EXPECT_TRUE(isExcludedByNoProxy("sparql.example.org", "example.org"));
  EXPECT_TRUE(isExcludedByNoProxy("a.b.example.org", "example.org"));
  // A leading dot on the entry is conventional and carries no extra meaning.
  EXPECT_TRUE(isExcludedByNoProxy("sparql.example.org", ".example.org"));
  EXPECT_TRUE(isExcludedByNoProxy("example.org", ".example.org"));

  // Matching is on domain-label boundaries, not plain string suffixes.
  EXPECT_FALSE(isExcludedByNoProxy("notexample.org", "example.org"));
  EXPECT_FALSE(isExcludedByNoProxy("example.org.evil.com", "example.org"));
  EXPECT_FALSE(isExcludedByNoProxy("example.com", "example.org"));
}

// _____________________________________________________________________________
TEST(IsExcludedByNoProxy, listHandling) {
  std::string_view list = "example.org, foo.com ,, bar.net";
  EXPECT_TRUE(isExcludedByNoProxy("example.org", list));
  EXPECT_TRUE(isExcludedByNoProxy("x.foo.com", list));
  EXPECT_TRUE(isExcludedByNoProxy("bar.net", list));
  EXPECT_FALSE(isExcludedByNoProxy("other.org", list));

  // An empty list excludes nothing.
  EXPECT_FALSE(isExcludedByNoProxy("example.org", ""));
  EXPECT_FALSE(isExcludedByNoProxy("example.org", " , , "));

  // `*` excludes everything.
  EXPECT_TRUE(isExcludedByNoProxy("example.org", "*"));
  EXPECT_TRUE(isExcludedByNoProxy("anything.at.all", "foo.com,*"));
}

// _____________________________________________________________________________
TEST(IsExcludedByNoProxy, caseInsensitiveAndPortIsIgnored) {
  EXPECT_TRUE(isExcludedByNoProxy("EXAMPLE.org", "example.ORG"));
  EXPECT_TRUE(isExcludedByNoProxy("Sparql.Example.Org", "example.org"));
  // A port on the entry is stripped, so it matches on all ports.
  EXPECT_TRUE(isExcludedByNoProxy("example.org", "example.org:80"));
  EXPECT_TRUE(isExcludedByNoProxy("example.org", "example.org:443"));
  // A colon that is not a port is left alone (an IPv6 address).
  EXPECT_TRUE(isExcludedByNoProxy("fe80::1", "fe80::1"));
}

// _____________________________________________________________________________
TEST(IsLoopbackHost, namesAndAddresses) {
  EXPECT_TRUE(isLoopbackHost("localhost"));
  EXPECT_TRUE(isLoopbackHost("LOCALHOST"));
  EXPECT_TRUE(isLoopbackHost("foo.localhost"));
  EXPECT_TRUE(isLoopbackHost("127.0.0.1"));
  EXPECT_TRUE(isLoopbackHost("127.1.2.3"));
  EXPECT_TRUE(isLoopbackHost("::1"));
  EXPECT_TRUE(isLoopbackHost("[::1]"));

  EXPECT_FALSE(isLoopbackHost("example.org"));
  EXPECT_FALSE(isLoopbackHost("notlocalhost"));
  EXPECT_FALSE(isLoopbackHost("localhost.example.org"));
  EXPECT_FALSE(isLoopbackHost("10.0.0.1"));
  EXPECT_FALSE(isLoopbackHost("128.0.0.1"));
  EXPECT_FALSE(isLoopbackHost(""));
}

// _____________________________________________________________________________
TEST(ProxyConfiguration, schemeSelectsTheProxy) {
  ProxyConfiguration configuration{"http://http-proxy:3128",
                                   "http://https-proxy:3129", ""};
  EXPECT_THAT(configuration.proxyFor("http", "example.org"),
              isProxy("http-proxy", "3128"));
  EXPECT_THAT(configuration.proxyFor("https", "example.org"),
              isProxy("https-proxy", "3129"));
  EXPECT_FALSE(configuration.empty());
}

// _____________________________________________________________________________
TEST(ProxyConfiguration, onlyOneSchemeConfigured) {
  ProxyConfiguration onlyHttps{"", "http://https-proxy:3129", ""};
  EXPECT_EQ(onlyHttps.proxyFor("http", "example.org"), std::nullopt);
  EXPECT_THAT(onlyHttps.proxyFor("https", "example.org"),
              isProxy("https-proxy", "3129"));
  EXPECT_FALSE(onlyHttps.empty());

  ProxyConfiguration none{"", "", ""};
  EXPECT_EQ(none.proxyFor("http", "example.org"), std::nullopt);
  EXPECT_EQ(none.proxyFor("https", "example.org"), std::nullopt);
  EXPECT_TRUE(none.empty());
}

// _____________________________________________________________________________
TEST(ProxyConfiguration, noProxyAndLoopbackBypassTheProxy) {
  ProxyConfiguration configuration{"http://proxy:3128", "http://proxy:3128",
                                   "internal.example.org"};
  EXPECT_THAT(configuration.proxyFor("https", "example.org"),
              isProxy("proxy", "3128"));
  EXPECT_EQ(configuration.proxyFor("https", "internal.example.org"),
            std::nullopt);
  EXPECT_EQ(configuration.proxyFor("https", "sub.internal.example.org"),
            std::nullopt);
  // Loopback always bypasses the proxy, even when not listed in `no_proxy`.
  EXPECT_EQ(configuration.proxyFor("http", "localhost"), std::nullopt);
  EXPECT_EQ(configuration.proxyFor("http", "127.0.0.1"), std::nullopt);
}

// _____________________________________________________________________________
TEST(ProxyConfiguration, invalidSchemeIsAContractViolation) {
  ProxyConfiguration configuration{"http://proxy:3128", "http://proxy:3128",
                                   ""};
  EXPECT_ANY_THROW(configuration.proxyFor("ftp", "example.org"));
}

// _____________________________________________________________________________
TEST(ProxyConfiguration, asStringForLoggingHidesCredentials) {
  ProxyConfiguration configuration{"http://user:secret@proxy:3128",
                                   "http://proxy:3129", "example.org"};
  std::string logged = configuration.asStringForLogging();
  EXPECT_THAT(logged, HasSubstr("proxy:3128"));
  EXPECT_THAT(logged, HasSubstr("(with auth)"));
  EXPECT_THAT(logged, HasSubstr("proxy:3129"));
  EXPECT_THAT(logged, HasSubstr("example.org"));
  // Neither the password nor its encoding may appear in the log.
  EXPECT_THAT(logged, ::testing::Not(HasSubstr("secret")));
  EXPECT_THAT(logged,
              ::testing::Not(HasSubstr(absl::Base64Escape("user:secret"))));

  EXPECT_EQ(ProxyConfiguration("", "", "").asStringForLogging(), "none");
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, emptyEnvironmentMeansNoProxy) {
  auto configuration = ProxyConfiguration::fromEnvironment();
  EXPECT_TRUE(configuration.empty());
  EXPECT_EQ(configuration.proxyFor("http", "example.org"), std::nullopt);
  EXPECT_EQ(configuration.proxyFor("https", "example.org"), std::nullopt);
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, schemeSpecificVariables) {
  setEnv("http_proxy", "http://http-proxy:3128");
  setEnv("https_proxy", "http://https-proxy:3129");
  auto configuration = ProxyConfiguration::fromEnvironment();
  EXPECT_THAT(configuration.proxyFor("http", "example.org"),
              isProxy("http-proxy", "3128"));
  EXPECT_THAT(configuration.proxyFor("https", "example.org"),
              isProxy("https-proxy", "3129"));
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, uppercaseHttpsProxyIsHonored) {
  setEnv("HTTPS_PROXY", "http://https-proxy:3129");
  EXPECT_THAT(
      ProxyConfiguration::fromEnvironment().proxyFor("https", "example.org"),
      isProxy("https-proxy", "3129"));
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, lowercaseWinsOverUppercase) {
  setEnv("https_proxy", "http://lower:1");
  setEnv("HTTPS_PROXY", "http://upper:2");
  EXPECT_THAT(
      ProxyConfiguration::fromEnvironment().proxyFor("https", "example.org"),
      isProxy("lower", "1"));
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, uppercaseHttpProxyIsIgnored) {
  // `HTTP_PROXY` is deliberately not honored, see the comment on
  // `ProxyConfiguration::fromEnvironment`.
  setEnv("HTTP_PROXY", "http://should-be-ignored:3128");
  auto configuration = ProxyConfiguration::fromEnvironment();
  EXPECT_TRUE(configuration.empty());
  EXPECT_EQ(configuration.proxyFor("http", "example.org"), std::nullopt);
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, allProxyIsTheFallbackForBothSchemes) {
  setEnv("all_proxy", "http://all-proxy:1080");
  auto configuration = ProxyConfiguration::fromEnvironment();
  EXPECT_THAT(configuration.proxyFor("http", "example.org"),
              isProxy("all-proxy", "1080"));
  EXPECT_THAT(configuration.proxyFor("https", "example.org"),
              isProxy("all-proxy", "1080"));

  // A scheme-specific variable takes precedence over `all_proxy`.
  setEnv("https_proxy", "http://https-proxy:3129");
  auto withHttps = ProxyConfiguration::fromEnvironment();
  EXPECT_THAT(withHttps.proxyFor("http", "example.org"),
              isProxy("all-proxy", "1080"));
  EXPECT_THAT(withHttps.proxyFor("https", "example.org"),
              isProxy("https-proxy", "3129"));
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, emptyVariableDisablesAnInheritedProxy) {
  // Setting a variable to the empty string is the conventional way of undoing a
  // proxy setting inherited from a parent process. Note that it must also
  // override `all_proxy`, not fall back to it.
  setEnv("all_proxy", "http://all-proxy:1080");
  setEnv("https_proxy", "");
  auto configuration = ProxyConfiguration::fromEnvironment();
  EXPECT_THAT(configuration.proxyFor("http", "example.org"),
              isProxy("all-proxy", "1080"));
  EXPECT_EQ(configuration.proxyFor("https", "example.org"), std::nullopt);
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, noProxyFromEnvironment) {
  setEnv("https_proxy", "http://proxy:3128");
  setEnv("no_proxy", "internal.example.org");
  auto configuration = ProxyConfiguration::fromEnvironment();
  EXPECT_EQ(configuration.proxyFor("https", "internal.example.org"),
            std::nullopt);
  EXPECT_THAT(configuration.proxyFor("https", "external.example.org"),
              isProxy("proxy", "3128"));

  setEnv("NO_PROXY", "other.example.org");
  ::unsetenv("no_proxy");
  auto upperCase = ProxyConfiguration::fromEnvironment();
  EXPECT_EQ(upperCase.proxyFor("https", "other.example.org"), std::nullopt);
}

// _____________________________________________________________________________
TEST_F(ProxyEnvironmentTest, malformedEnvironmentValueThrows) {
  setEnv("https_proxy", "socks5://proxy:1080");
  AD_EXPECT_THROW_WITH_MESSAGE(ProxyConfiguration::fromEnvironment(),
                               HasSubstr("only supports plain HTTP proxies"));
}
