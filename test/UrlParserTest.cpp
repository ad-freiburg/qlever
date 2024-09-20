// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/GTestHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

using namespace ad_utility;

TEST(UrlParserTest, paramsToMap) {
  auto parseParams =
      [](const string& queryString) -> HashMap<std::string, std::string> {
    boost::urls::url_view url(queryString);
    return url_parser::paramsToMap(url.params());
  };
  auto HashMapEq = [](const HashMap<std::string, std::string>& hashMap)
      -> testing::Matcher<HashMap<std::string, std::string>> {
    return testing::ContainerEq(hashMap);
  };

  AD_EXPECT_THROW_WITH_MESSAGE(
      parseParams("?foo=a&foo=b"),
      testing::StrEq(
          "HTTP parameter \"foo\" is set twice. It is \"a\" and \"b\""));
  // Parameter with key "" is set twice.
  EXPECT_THROW(parseParams("?&"), std::runtime_error);

  EXPECT_THAT(parseParams("?query=SELECT%20%2A%20WHERE%20%7B%7D"),
              HashMapEq({{"query", "SELECT * WHERE {}"}}));
  EXPECT_THAT(parseParams("?cmd=stats"), HashMapEq({{"cmd", "stats"}}));
  EXPECT_THAT(parseParams("/ping"), HashMapEq({}));
  EXPECT_THAT(parseParams(""), HashMapEq({}));
  // Producing a sequence with one empty param here is a design decision by
  // Boost.URL to make it distinguishable from the case where there is no query
  // string.
  EXPECT_THAT(parseParams("?"), HashMapEq({{"", ""}}));
  EXPECT_THAT(parseParams("/ping?a=b&c=d"),
              HashMapEq({{"a", "b"}, {"c", "d"}}));
  EXPECT_THAT(parseParams("?foo=a"), HashMapEq({{"foo", "a"}}));
  EXPECT_THAT(parseParams("?a=b&c=d&e=f"),
              HashMapEq({{"a", "b"}, {"c", "d"}, {"e", "f"}}));
  EXPECT_THAT(parseParams("?=foo"), HashMapEq({{"", "foo"}}));
  EXPECT_THAT(parseParams("?=foo&a=b"), HashMapEq({{"", "foo"}, {"a", "b"}}));
  EXPECT_THAT(parseParams("?foo="), HashMapEq({{"foo", ""}}));
  EXPECT_THAT(parseParams("?foo=&bar=baz"),
              HashMapEq({{"foo", ""}, {"bar", "baz"}}));
}

TEST(UrlParserTest, parseRequestTarget) {
  using namespace ad_utility::url_parser;

  auto IsParsedUrl = [](const string& path,
                        const HashMap<std::string, std::string>& parameters)
      -> testing::Matcher<const ParsedUrl&> {
    return testing::AllOf(
        AD_FIELD(ParsedUrl, path_, testing::Eq(path)),
        AD_FIELD(ParsedUrl, parameters_, testing::ContainerEq(parameters)));
  };

  EXPECT_THAT(parseRequestTarget("/"), IsParsedUrl("/", {}));
  EXPECT_THAT(parseRequestTarget("/?cmd=stats"),
              IsParsedUrl("/", {{"cmd", "stats"}}));
  EXPECT_THAT(parseRequestTarget("/?cmd=clear-cache"),
              IsParsedUrl("/", {{"cmd", "clear-cache"}}));
  EXPECT_THAT(parseRequestTarget(
                  "/?query=SELECT%20%2A%20WHERE%20%7B%7D&action=csv_export"),
              IsParsedUrl("/", {{"query", "SELECT * WHERE {}"},
                                {"action", "csv_export"}}));
  EXPECT_THAT(parseRequestTarget("/ping?foo=bar"),
              IsParsedUrl("/ping", {{"foo", "bar"}}));
  EXPECT_THAT(parseRequestTarget("/foo??update=bar"),
              IsParsedUrl("/foo", {{"?update", "bar"}}));
  // This a complete URL and not only the request target
  AD_EXPECT_THROW_WITH_MESSAGE(
      parseRequestTarget("file://more-than-target"),
      testing::StrEq("Failed to parse URL: \"file://more-than-target\"."));
}
