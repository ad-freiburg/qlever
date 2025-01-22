// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/GTestHelpers.h"
#include "util/TripleComponentTestHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

using namespace ad_utility;
using namespace ad_utility::testing;

TEST(UrlParserTest, getParameterCheckAtMostOnce) {
  const url_parser::ParamValueMap map = {{"once", {"a"}},
                                         {"multiple_times", {"b", "c"}}};

  EXPECT_THAT(url_parser::getParameterCheckAtMostOnce(map, "absent"),
              ::testing::Eq(std::nullopt));
  EXPECT_THAT(url_parser::getParameterCheckAtMostOnce(map, "once"),
              ::testing::Optional(::testing::StrEq("a")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      url_parser::getParameterCheckAtMostOnce(map, "multiple_times"),
      ::testing::StrEq("Parameter \"multiple_times\" must be given "
                       "exactly once. Is: 2"));
}

TEST(UrlParserTest, paramsToMap) {
  auto parseParams =
      [](const string& queryString) -> url_parser::ParamValueMap {
    const boost::urls::url_view url(queryString);
    return url_parser::paramsToMap(url.params());
  };
  auto HashMapEq = [](const url_parser::ParamValueMap& hashMap)
      -> ::testing::Matcher<const url_parser::ParamValueMap> {
    return ::testing::ContainerEq(hashMap);
  };

  EXPECT_THAT(parseParams("?foo=a&foo=b"), HashMapEq({{"foo", {"a", "b"}}}));
  EXPECT_THAT(parseParams("?&"), HashMapEq({{"", {"", ""}}}));
  EXPECT_THAT(parseParams("?query=SELECT%20%2A%20WHERE%20%7B%7D"),
              HashMapEq({{"query", {"SELECT * WHERE {}"}}}));
  EXPECT_THAT(
      parseParams("?query=SELECT%20%2A%20WHERE%20%7B%7D&default-graph-uri="
                  "https%3A%2F%2Fw3.org%2Fdefault&named-graph-uri=https%3A%2F%"
                  "2Fw3.org%2F1&named-graph-uri=https%3A%2F%2Fw3.org%2F2"),
      HashMapEq(
          {{"query", {"SELECT * WHERE {}"}},
           {"default-graph-uri", {"https://w3.org/default"}},
           {"named-graph-uri", {"https://w3.org/1", "https://w3.org/2"}}}));
  EXPECT_THAT(parseParams("?cmd=stats"), HashMapEq({{"cmd", {"stats"}}}));
  EXPECT_THAT(parseParams("/ping"), HashMapEq({}));
  EXPECT_THAT(parseParams(""), HashMapEq({}));
  // Producing a sequence with one empty param here is a design decision by
  // Boost.URL to make it distinguishable from the case where there is no query
  // string.
  EXPECT_THAT(parseParams("?"), HashMapEq({{"", {""}}}));
  EXPECT_THAT(parseParams("/ping?a=b&c=d"),
              HashMapEq({{"a", {"b"}}, {"c", {"d"}}}));
  EXPECT_THAT(parseParams("?foo=a"), HashMapEq({{"foo", {"a"}}}));
  EXPECT_THAT(parseParams("?a=b&c=d&e=f"),
              HashMapEq({{"a", {"b"}}, {"c", {"d"}}, {"e", {"f"}}}));
  EXPECT_THAT(parseParams("?=foo"), HashMapEq({{"", {"foo"}}}));
  EXPECT_THAT(parseParams("?=foo&a=b"),
              HashMapEq({{"", {"foo"}}, {"a", {"b"}}}));
  EXPECT_THAT(parseParams("?foo="), HashMapEq({{"foo", {""}}}));
  EXPECT_THAT(parseParams("?foo=&bar=baz"),
              HashMapEq({{"foo", {""}}, {"bar", {"baz"}}}));
}

TEST(UrlParserTest, parseRequestTarget) {
  using namespace ad_utility::url_parser;

  auto IsParsedUrl = [](const string& path,
                        const url_parser::ParamValueMap& parameters)
      -> ::testing::Matcher<const ParsedUrl&> {
    return ::testing::AllOf(
        AD_FIELD(ParsedUrl, path_, ::testing::Eq(path)),
        AD_FIELD(ParsedUrl, parameters_, ::testing::ContainerEq(parameters)));
  };

  EXPECT_THAT(parseRequestTarget("/"), IsParsedUrl("/", {}));
  EXPECT_THAT(parseRequestTarget("/?cmd=stats"),
              IsParsedUrl("/", {{"cmd", {"stats"}}}));
  EXPECT_THAT(parseRequestTarget("/?cmd=clear-cache"),
              IsParsedUrl("/", {{"cmd", {"clear-cache"}}}));
  EXPECT_THAT(parseRequestTarget(
                  "/?query=SELECT%20%2A%20WHERE%20%7B%7D&action=csv_export"),
              IsParsedUrl("/", {{"query", {"SELECT * WHERE {}"}},
                                {"action", {"csv_export"}}}));
  EXPECT_THAT(parseRequestTarget("/ping?foo=bar"),
              IsParsedUrl("/ping", {{"foo", {"bar"}}}));
  EXPECT_THAT(parseRequestTarget("/foo??update=bar"),
              IsParsedUrl("/foo", {{"?update", {"bar"}}}));
  // This a complete URL and not only the request target
  AD_EXPECT_THROW_WITH_MESSAGE(
      parseRequestTarget("file://more-than-target"),
      ::testing::StrEq("Failed to parse URL: \"file://more-than-target\"."));
}

TEST(UrlParserTest, parseDatasetClausesFrom) {
  using namespace ad_utility::url_parser;

  // Construct the vector from an initializer list without specifying the type.
  auto IsDatasets = [](const std::vector<DatasetClause>& datasetClauses)
      -> ::testing::Matcher<const std::vector<DatasetClause>&> {
    return ::testing::ContainerEq(datasetClauses);
  };

  EXPECT_THAT(parseDatasetClausesFrom({}, "default-grpah-uri", false),
              IsDatasets({}));
  EXPECT_THAT(
      parseDatasetClausesFrom({{"default-graph-uri", {"https://w3.org/1"}}},
                              "default-graph-uri", false),
      IsDatasets({{iri("<https://w3.org/1>"), false}}));
  EXPECT_THAT(
      parseDatasetClausesFrom({{"named-graph-uri", {"https://w3.org/1"}}},
                              "named-graph-uri", true),
      IsDatasets({{iri("<https://w3.org/1>"), true}}));
  EXPECT_THAT(
      parseDatasetClausesFrom({{"default-graph-uri", {"https://w3.org/1"}},
                               {"named-graph-uri", {"https://w3.org/2"}}},
                              "default-graph-uri", false),
      IsDatasets({
          {iri("<https://w3.org/1>"), false},
      }));
  EXPECT_THAT(
      parseDatasetClausesFrom(
          {{"default-graph-uri", {"https://w3.org/1", "https://w3.org/2"}},
           {"named-graph-uri", {"https://w3.org/3", "https://w3.org/4"}}},
          "named-graph-uri", true),
      IsDatasets({{iri("<https://w3.org/3>"), true},
                  {iri("<https://w3.org/4>"), true}}));
}

TEST(UrlParserTest, checkParameter) {
  const url_parser::ParamValueMap exampleParams = {{"foo", {"bar"}},
                                                   {"baz", {"qux", "quux"}}};

  EXPECT_THAT(url_parser::checkParameter(exampleParams, "doesNotExist", ""),
              ::testing::Eq(std::nullopt));
  EXPECT_THAT(url_parser::checkParameter(exampleParams, "foo", "baz"),
              ::testing::Eq(std::nullopt));
  EXPECT_THAT(url_parser::checkParameter(exampleParams, "foo", "bar"),
              ::testing::Optional(::testing::StrEq("bar")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      url_parser::checkParameter(exampleParams, "baz", "qux"),
      ::testing::StrEq("Parameter \"baz\" must be given exactly once. Is: 2"));
  EXPECT_THAT(url_parser::checkParameter(exampleParams, "foo", std::nullopt),
              ::testing::Optional(::testing::StrEq("bar")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      url_parser::checkParameter(exampleParams, "baz", std::nullopt),
      ::testing::StrEq("Parameter \"baz\" must be given exactly once. Is: 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      url_parser::checkParameter(exampleParams, "baz", std::nullopt),
      ::testing::StrEq("Parameter \"baz\" must be given exactly once. Is: 2"));
}
