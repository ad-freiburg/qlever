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
    return UrlParser::paramsToMap(url.params());
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
  auto IsParsedUrl = [](const string& path,
                        const HashMap<std::string, std::string>& parameters)
      -> testing::Matcher<const UrlParser::ParsedUrl&> {
    return testing::AllOf(
        AD_FIELD(UrlParser::ParsedUrl, path_, testing::Eq(path)),
        AD_FIELD(UrlParser::ParsedUrl, parameters_,
                 testing::ContainerEq(parameters)));
  };

  EXPECT_THAT(UrlParser::parseRequestTarget("/"), IsParsedUrl("/", {}));
  EXPECT_THAT(UrlParser::parseRequestTarget("/?cmd=stats"),
              IsParsedUrl("/", {{"cmd", "stats"}}));
  EXPECT_THAT(UrlParser::parseRequestTarget("/?cmd=clear-cache"),
              IsParsedUrl("/", {{"cmd", "clear-cache"}}));
  EXPECT_THAT(UrlParser::parseRequestTarget(
                  "/?query=SELECT%20%2A%20WHERE%20%7B%7D&action=csv_export"),
              IsParsedUrl("/", {{"query", "SELECT * WHERE {}"},
                                {"action", "csv_export"}}));
  EXPECT_THAT(UrlParser::parseRequestTarget("/ping?foo=bar"),
              IsParsedUrl("/ping", {{"foo", "bar"}}));
  EXPECT_THAT(UrlParser::parseRequestTarget("/foo??update=bar"),
              IsParsedUrl("/foo", {{"?update", "bar"}}));
}
