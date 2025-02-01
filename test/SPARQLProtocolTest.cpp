// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <engine/SPARQLProtocol.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/beast/http.hpp>

#include "util/GTestHelpers.h"
#include "util/HttpRequestHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"

using namespace ad_utility::url_parser;
using namespace ad_utility::url_parser::sparqlOperation;
using namespace ad_utility::testing;

namespace {
auto ParsedRequestIs = [](const std::string& path,
                          const std::optional<std::string>& accessToken,
                          const ParamValueMap& parameters,
                          const std::variant<Query, Update, None>& operation)
    -> testing::Matcher<const ParsedRequest> {
  return testing::AllOf(
      AD_FIELD(ad_utility::url_parser::ParsedRequest, path_, testing::Eq(path)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, accessToken_,
               testing::Eq(accessToken)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, parameters_,
               testing::ContainerEq(parameters)),
      AD_FIELD(ad_utility::url_parser::ParsedRequest, operation_,
               testing::Eq(operation)));
};
}  // namespace

TEST(SPARQLProtocolTest, parseHttpRequest) {
  auto parse = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    return SPARQLProtocol::parseHttpRequest(request);
  };
  const std::string URLENCODED =
      "application/x-www-form-urlencoded;charset=UTF-8";
  const std::string QUERY = "application/sparql-query";
  const std::string UPDATE = "application/sparql-update";
  EXPECT_THAT(parse(makeGetRequest("/")),
              ParsedRequestIs("/", std::nullopt, {}, None{}));
  EXPECT_THAT(parse(makeGetRequest("/ping")),
              ParsedRequestIs("/ping", std::nullopt, {}, None{}));
  EXPECT_THAT(parse(makeGetRequest("/?cmd=stats")),
              ParsedRequestIs("/", std::nullopt, {{"cmd", {"stats"}}}, None{}));
  EXPECT_THAT(parse(makeGetRequest(
                  "/?query=SELECT+%2A%20WHERE%20%7B%7D&action=csv_export")),
              ParsedRequestIs("/", std::nullopt, {{"action", {"csv_export"}}},
                              Query{"SELECT * WHERE {}", {}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                      Query{"SELECT * WHERE {}", {}}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED,
                            "ääär y=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ::testing::HasSubstr("Invalid URL-encoded POST request"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?query=SELECT%20%2A%20WHERE%20%7B%7D&query=SELECT%"
                           "20%3Ffoo%20WHERE%20%7B%7D")),
      ::testing::StrEq(
          "Parameter \"query\" must be given exactly once. Is: 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&update=DELETE%"
                            "20%7B%7D%20WHERE%20%7B%7D")),
      ::testing::HasSubstr(
          "Request must only contain one of \"query\" and \"update\"."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED,
                            "update=DELETE%20%7B%7D%20WHERE%20%7B%7D&update="
                            "DELETE%20%7B%7D%20WHERE%20%7B%7D")),
      ::testing::StrEq(
          "Parameter \"update\" must be given exactly once. Is: 2"));
  EXPECT_THAT(
      parse(makePostRequest("/", "application/x-www-form-urlencoded",
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                      Query{"SELECT * WHERE {}", {}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Query{"SELECT * WHERE {}", {}}));
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  EXPECT_THAT(
      parse(makePostRequest(
          "/", URLENCODED,
          "query=SELECT%20%2A%20WHERE%20%7B%7D&default-graph-uri=https%3A%2F%"
          "2Fw3.org%2Fdefault&named-graph-uri=https%3A%2F%2Fw3.org%2F1&named-"
          "graph-uri=https%3A%2F%2Fw3.org%2F2")),
      ParsedRequestIs(
          "/", std::nullopt,
          {{"default-graph-uri", {"https://w3.org/default"}},
           {"named-graph-uri", {"https://w3.org/1", "https://w3.org/2"}}},
          Query{"SELECT * WHERE {}",
                {DatasetClause{Iri("<https://w3.org/default>"), false},
                 DatasetClause{Iri("<https://w3.org/1>"), true},
                 DatasetClause{Iri("<https://w3.org/2>"), true}}}));
  ;
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?send=100", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("URL-encoded POST requests must not contain query "
                     "parameters in the URL."));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED, "cmd=clear-cache")),
      ParsedRequestIs("/", std::nullopt, {{"cmd", {"clear-cache"}}}, None{}));
  EXPECT_THAT(
      parse(makePostRequest("/", QUERY, "SELECT * WHERE {}")),
      ParsedRequestIs("/", std::nullopt, {}, Query{"SELECT * WHERE {}", {}}));
  EXPECT_THAT(parse(makePostRequest("/?send=100", QUERY, "SELECT * WHERE {}")),
              ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                              Query{"SELECT * WHERE {}", {}}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeRequest(http::verb::patch, "/")),
      testing::StrEq(
          "Request method \"PATCH\" not supported (has to be GET or POST)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", "invalid/content-type", "")),
      testing::StrEq(
          "POST request with content type \"invalid/content-type\" not "
          "supported (must be \"application/x-www-form-urlencoded\", "
          "\"application/sparql-query\" or \"application/sparql-update\")"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?update=DELETE%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("SPARQL Update is not allowed as GET request."));
  EXPECT_THAT(
      parse(makePostRequest("/", UPDATE, "DELETE * WHERE {}")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}", {}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "update=DELETE%20%2A%20WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}", {}}));
  EXPECT_THAT(
      parse(
          makePostRequest("/", URLENCODED, "update=DELETE+%2A+WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}", {}}));
  // Check that the correct datasets for the method (GET or POST) are added
  EXPECT_THAT(
      parse(makeGetRequest("/?query=SELECT%20%2A%20WHERE%20%7B%7D&default-"
                           "graph-uri=foo&named-graph-uri=bar&using-graph-uri="
                           "baz&using-named-graph-uri=cat")),
      ParsedRequestIs("/", std::nullopt,
                      {{"default-graph-uri", {"foo"}},
                       {"named-graph-uri", {"bar"}},
                       {"using-graph-uri", {"baz"}},
                       {"using-named-graph-uri", {"cat"}}},
                      Query{"SELECT * WHERE {}",
                            {DatasetClause{Iri("<foo>"), false},
                             DatasetClause{Iri("<bar>"), true}}}));
  EXPECT_THAT(
      parse(makePostRequest("/?default-"
                            "graph-uri=foo&named-graph-uri=bar&using-graph-uri="
                            "baz&using-named-graph-uri=cat",
                            QUERY, "SELECT * WHERE {}")),
      ParsedRequestIs("/", std::nullopt,
                      {{"default-graph-uri", {"foo"}},
                       {"named-graph-uri", {"bar"}},
                       {"using-graph-uri", {"baz"}},
                       {"using-named-graph-uri", {"cat"}}},
                      Query{"SELECT * WHERE {}",
                            {DatasetClause{Iri("<foo>"), false},
                             DatasetClause{Iri("<bar>"), true}}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D&default-graph-"
                            "uri=foo&named-graph-uri=bar&using-graph-uri=baz&"
                            "using-named-graph-uri=cat")),
      ParsedRequestIs("/", std::nullopt,
                      {{"default-graph-uri", {"foo"}},
                       {"named-graph-uri", {"bar"}},
                       {"using-graph-uri", {"baz"}},
                       {"using-named-graph-uri", {"cat"}}},
                      Query{"SELECT * WHERE {}",
                            {DatasetClause{Iri("<foo>"), false},
                             DatasetClause{Iri("<bar>"), true}}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "update=INSERT%20DATA%20%7B%7D&default-graph-uri="
                            "foo&named-graph-uri=bar&using-graph-uri=baz&"
                            "using-named-graph-uri=cat")),
      ParsedRequestIs("/", std::nullopt,
                      {
                          {"default-graph-uri", {"foo"}},
                          {"named-graph-uri", {"bar"}},
                          {"using-graph-uri", {"baz"}},
                          {"using-named-graph-uri", {"cat"}},
                      },
                      Update{"INSERT DATA {}",
                             {DatasetClause{Iri("<baz>"), false},
                              DatasetClause{Iri("<cat>"), true}}}));
  EXPECT_THAT(
      parse(makePostRequest(
          "/?default-graph-uri=foo&named-graph-uri=bar&using-graph-uri=baz&"
          "using-named-graph-uri=cat",
          UPDATE, "INSERT DATA {}")),
      ParsedRequestIs("/", std::nullopt,
                      {
                          {"default-graph-uri", {"foo"}},
                          {"named-graph-uri", {"bar"}},
                          {"using-graph-uri", {"baz"}},
                          {"using-named-graph-uri", {"cat"}},
                      },
                      Update{"INSERT DATA {}",
                             {DatasetClause{Iri("<baz>"), false},
                              DatasetClause{Iri("<cat>"), true}}}));
  auto testAccessTokenCombinations =
      [&](const http::verb& method, std::string_view pathBase,
          const std::variant<Query, Update, None>& expectedOperation,
          const ad_utility::HashMap<http::field, std::string>& headers = {},
          const std::optional<std::string>& body = std::nullopt,
          ad_utility::source_location l =
              ad_utility::source_location::current()) {
        auto t = generateLocationTrace(l);
        // Test the cases:
        // 1. No access token
        // 2. Access token in query
        // 3. Access token in `Authorization` header
        // 4. Different access tokens
        // 5. Same access token
        boost::urls::url pathWithAccessToken{pathBase};
        pathWithAccessToken.params().append({"access-token", "foo"});
        ad_utility::HashMap<http::field, std::string>
            headersWithDifferentAccessToken{headers};
        headersWithDifferentAccessToken.insert(
            {http::field::authorization, "Bearer bar"});
        ad_utility::HashMap<http::field, std::string>
            headersWithSameAccessToken{headers};
        headersWithSameAccessToken.insert(
            {http::field::authorization, "Bearer foo"});
        EXPECT_THAT(parse(makeRequest(method, pathBase, headers, body)),
                    ParsedRequestIs("/", std::nullopt, {}, expectedOperation));
        EXPECT_THAT(parse(makeRequest(method, pathWithAccessToken.buffer(),
                                      headers, body)),
                    ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                                    expectedOperation));
        EXPECT_THAT(parse(makeRequest(method, pathBase,
                                      headersWithDifferentAccessToken, body)),
                    ParsedRequestIs("/", "bar", {}, expectedOperation));
        EXPECT_THAT(parse(makeRequest(method, pathWithAccessToken.buffer(),
                                      headersWithSameAccessToken, body)),
                    ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                                    expectedOperation));
        AD_EXPECT_THROW_WITH_MESSAGE(
            parse(makeRequest(method, pathWithAccessToken.buffer(),
                              headersWithDifferentAccessToken, body)),
            testing::HasSubstr(
                "Access token is specified both in the "
                "`Authorization` header and by the `access-token` "
                "parameter, but they are not the same"));
      };
  testAccessTokenCombinations(http::verb::get, "/?query=a", Query{"a", {}});
  testAccessTokenCombinations(http::verb::post, "/", Query{"a", {}},
                              {{http::field::content_type, QUERY}}, "a");
  testAccessTokenCombinations(http::verb::post, "/", Update{"a", {}},
                              {{http::field::content_type, UPDATE}}, "a");
  auto testAccessTokenCombinationsUrlEncoded =
      [&](const std::string& bodyBase,
          const std::variant<Query, Update, None>& expectedOperation,
          ad_utility::source_location l =
              ad_utility::source_location::current()) {
        auto t = generateLocationTrace(l);
        // Test the cases:
        // 1. No access token
        // 2. Access token in query
        // 3. Access token in `Authorization` header
        // 4. Different access tokens
        // 5. Same access token
        boost::urls::url paramsWithAccessToken{absl::StrCat("/?", bodyBase)};
        paramsWithAccessToken.params().append({"access-token", "foo"});
        std::string bodyWithAccessToken{
            paramsWithAccessToken.encoded_params().buffer()};
        ad_utility::HashMap<http::field, std::string> headers{
            {http::field::content_type, {URLENCODED}}};
        ad_utility::HashMap<http::field, std::string>
            headersWithDifferentAccessToken{
                {http::field::content_type, {URLENCODED}},
                {http::field::authorization, "Bearer bar"}};
        ad_utility::HashMap<http::field, std::string>
            headersWithSameAccessToken{
                {http::field::content_type, {URLENCODED}},
                {http::field::authorization, "Bearer foo"}};
        EXPECT_THAT(
            parse(makeRequest(http::verb::post, "/", headers, bodyBase)),
            ParsedRequestIs("/", std::nullopt, {}, expectedOperation));
        EXPECT_THAT(parse(makeRequest(http::verb::post, "/", headers,
                                      bodyWithAccessToken)),
                    ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                                    expectedOperation));
        EXPECT_THAT(
            parse(makeRequest(http::verb::post, "/",
                              headersWithDifferentAccessToken, bodyBase)),
            ParsedRequestIs("/", "bar", {}, expectedOperation));
        EXPECT_THAT(parse(makeRequest(http::verb::post, "/",
                                      headersWithSameAccessToken, bodyBase)),
                    ParsedRequestIs("/", "foo", {}, expectedOperation));
        AD_EXPECT_THROW_WITH_MESSAGE(
            parse(makeRequest(http::verb::post, "/",
                              headersWithDifferentAccessToken,
                              bodyWithAccessToken)),
            testing::HasSubstr(
                "Access token is specified both in the "
                "`Authorization` header and by the `access-token` "
                "parameter, but they are not the same"));
      };
  testAccessTokenCombinationsUrlEncoded("query=SELECT%20%2A%20WHERE%20%7B%7D",
                                        Query{"SELECT * WHERE {}", {}});
  testAccessTokenCombinationsUrlEncoded("update=DELETE%20WHERE%20%7B%7D",
                                        Update{"DELETE WHERE {}", {}});
}

TEST(SPARQLProtocolTest, extractAccessToken) {
  auto extract = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    auto parsedUrl = parseRequestTarget(request.target());
    return SPARQLProtocol::extractAccessToken(request, parsedUrl.parameters_);
  };
  EXPECT_THAT(extract(makeGetRequest("/")), testing::Eq(std::nullopt));
  EXPECT_THAT(extract(makeGetRequest("/?access-token=foo")),
              testing::Optional(testing::Eq("foo")));
  EXPECT_THAT(
      extract(makeRequest(http::verb::get, "/",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::Optional(testing::Eq("foo")));
  EXPECT_THAT(
      extract(makeRequest(http::verb::get, "/?access-token=foo",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::Optional(testing::Eq("foo")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::get, "/?access-token=bar",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::HasSubstr(
          "Access token is specified both in the `Authorization` header and by "
          "the `access-token` parameter, but they are not the same"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::get, "/",
                          {{http::field::authorization, "foo"}})),
      testing::HasSubstr(
          "Authorization header doesn't start with \"Bearer \"."));
  EXPECT_THAT(extract(makePostRequest("/", "text/turtle", "")),
              testing::Eq(std::nullopt));
  EXPECT_THAT(extract(makePostRequest("/?access-token=foo", "text/turtle", "")),
              testing::Optional(testing::Eq("foo")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::post, "/?access-token=bar",
                          {{http::field::authorization, "Bearer foo"}})),
      testing::HasSubstr(
          "Access token is specified both in the `Authorization` header and by "
          "the `access-token` parameter, but they are not the same"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::post, "/?access-token=bar",
                          {{http::field::authorization, "foo"}})),
      testing::HasSubstr(
          "Authorization header doesn't start with \"Bearer \"."));
}
