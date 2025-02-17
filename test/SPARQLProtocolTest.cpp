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
auto ParsedRequestIs =
    [](const std::string& path, const std::optional<std::string>& accessToken,
       const ParamValueMap& parameters,
       const Operation& operation) -> testing::Matcher<const ParsedRequest> {
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
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  auto parse = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    return SPARQLProtocol::parseHttpRequest(request);
  };
  const std::string URLENCODED_PLAIN = "application/x-www-form-urlencoded";
  const std::string URLENCODED = URLENCODED_PLAIN + ";charset=UTF-8";
  const std::string QUERY = "application/sparql-query";
  const std::string UPDATE = "application/sparql-update";
  const std::string TURTLE = "text/turtle";
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
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?update=SELECT+%2A%20WHERE%20%7B%7D")),
      testing::HasSubstr("SPARQL Update is not allowed as GET request."));
  EXPECT_THAT(parse(makeGetRequest("/?graph=foo")),
              ParsedRequestIs("/", std::nullopt, {{"graph", {"foo"}}},
                              GraphStoreOperation{Iri("<foo>")}));
  EXPECT_THAT(parse(makeGetRequest("/?default")),
              ParsedRequestIs("/", std::nullopt, {{"default", {""}}},
                              GraphStoreOperation{DEFAULT{}}));
  EXPECT_THAT(
      parse(makeGetRequest("/?default&access-token=foo&timeout=120s")),
      ParsedRequestIs(
          "/", "foo",
          {{"access-token", {"foo"}}, {"default", {""}}, {"timeout", {"120s"}}},
          GraphStoreOperation{DEFAULT{}}));
  EXPECT_THAT(parse(makePostRequest("/?default&access-token=foo", TURTLE,
                                    "<f> <g> <h>")),
              ParsedRequestIs("/", "foo",
                              {{"access-token", {"foo"}}, {"default", {""}}},
                              GraphStoreOperation{DEFAULT{}}));
  EXPECT_THAT(parse(makeRequest(http::verb::post, "/?default",
                                {{http::field::authorization, {"Bearer foo"}},
                                 {http::field::content_type, {TURTLE}}},
                                "<f> <g> <h>")),
              ParsedRequestIs("/", "foo", {{"default", {""}}},
                              GraphStoreOperation{DEFAULT{}}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?default&default")),
      testing::HasSubstr("Parameter \"default\" must be "
                         "given exactly once. Is: 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?graph=%3Cfoo%3E&graph=%3Cbar%3E")),
      testing::HasSubstr("Parameter \"graph\" must be "
                         "given exactly once. Is: 2"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?query=SELECT+%2A%20WHERE%20%7B%7D&graph=foo")),
      testing::HasSubstr(
          R"(Request contains parameters for both a SPARQL Query ("query") and a Graph Store Protocol operation ("graph" or "default").)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?query=SELECT+%2A%20WHERE%20%7B%7D&default")),
      testing::HasSubstr(
          R"(Request contains parameters for both a SPARQL Query ("query") and a Graph Store Protocol operation ("graph" or "default").)"));
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
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?send=100", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("URL-encoded POST requests must not contain query "
                     "parameters in the URL."));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED, "cmd=clear-cache")),
      ParsedRequestIs("/", std::nullopt, {{"cmd", {"clear-cache"}}}, None{}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED, "query=a&update=b")),
      testing::HasSubstr(
          R"(Request must only contain one of "query" and "update".)"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED, "graph=foo")),
      testing::HasSubstr(absl::StrCat("Unsupported Content type \"",
                                      URLENCODED_PLAIN,
                                      "\" for "
                                      "Graph Store protocol.")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED, "default")),
      testing::HasSubstr(absl::StrCat("Unsupported Content type \"",
                                      URLENCODED_PLAIN,
                                      "\" for Graph Store protocol.")));
  EXPECT_THAT(
      parse(makePostRequest("/", QUERY, "SELECT * WHERE {}")),
      ParsedRequestIs("/", std::nullopt, {}, Query{"SELECT * WHERE {}", {}}));
  EXPECT_THAT(parse(makePostRequest("/?send=100", QUERY, "SELECT * WHERE {}")),
              ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                              Query{"SELECT * WHERE {}", {}}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?graph=foo", QUERY, "")),
      testing::HasSubstr(
          "Unsupported Content type \"application/sparql-query\" for "
          "Graph Store protocol."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?default", QUERY, "")),
      testing::HasSubstr(
          "Unsupported Content type \"application/sparql-query\" for "
          "Graph Store protocol."));
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
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?graph=foo", UPDATE, "")),
      testing::HasSubstr(
          "Unsupported Content type \"application/sparql-update\" for "
          "Graph Store protocol."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?default", UPDATE, "")),
      testing::HasSubstr(
          "Unsupported Content type \"application/sparql-update\" for "
          "Graph Store protocol."));
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
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?graph=foo", UPDATE, "")),
      testing::HasSubstr(
          "Unsupported Content type \"application/sparql-update\" for "
          "Graph Store protocol."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?default", UPDATE, "")),
      testing::HasSubstr(
          "Unsupported Content type \"application/sparql-update\" for "
          "Graph Store protocol."));
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
          const Operation& expectedOperation,
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
      [&](const std::string& bodyBase, const Operation& expectedOperation,
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
  EXPECT_THAT(
      parse(makePostRequest("/?default", TURTLE, "<foo> <bar> <baz> .")),
      ParsedRequestIs("/", std::nullopt, {{"default", {""}}},
                      GraphStoreOperation{DEFAULT{}}));
  EXPECT_THAT(
      parse(makePostRequest("/?graph=foo", TURTLE, "<foo> <bar> <baz> .")),
      ParsedRequestIs("/", std::nullopt, {{"graph", {"foo"}}},
                      GraphStoreOperation{Iri("<foo>")}));
  EXPECT_THAT(
      parse(makePostRequest("/?graph=foo&access-token=secret", TURTLE,
                            "<foo> <bar> <baz> .")),
      ParsedRequestIs("/", {"secret"},
                      {{"graph", {"foo"}}, {"access-token", {"secret"}}},
                      GraphStoreOperation{Iri("<foo>")}));
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, Constructor) {
  auto expect = [](const auto& request, const std::string& path,
                   const ParamValueMap& params,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(
        builder.parsedRequest_,
        AllOf(AD_FIELD(ParsedRequest, path_, testing::Eq(path)),
              AD_FIELD(ParsedRequest, accessToken_, testing::Eq(std::nullopt)),
              AD_FIELD(ParsedRequest, parameters_, testing::Eq(params)),
              AD_FIELD(ParsedRequest, operation_,
                       testing::VariantWith<None>(None{}))));
  };
  expect(makeGetRequest("/"), "/", {});
  expect(makeGetRequest("/default?graph=bar"), "/default",
         {{"graph", {"bar"}}});
  expect(makeGetRequest("/api/foo?graph=bar&query=foo&graph=baz"), "/api/foo",
         {{"graph", {"bar", "baz"}}, {"query", {"foo"}}});
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, extractAccessToken) {
  auto expect = [](const auto& request, const std::optional<string>& expected,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(builder.parsedRequest_.accessToken_, testing::Eq(std::nullopt));
    builder.extractAccessToken(request);
    EXPECT_THAT(builder.parsedRequest_.accessToken_, testing::Eq(expected));
  };
  expect(makeGetRequest("/"), std::nullopt);
  expect(makeGetRequest("/?query=foo"), std::nullopt);
  expect(makeGetRequest("/?query=foo&access-token=bar"), "bar");
  expect(makePostRequest("/?access-token=bar",
                         "application/x-www-form-urlencoded", "query=foo"),
         "bar");
  expect(
      makePostRequest("/?access-token=bar", "application/sparql-update", "foo"),
      "bar");
  expect(makeRequest(http::verb::get, "/",
                     {{http::field::authorization, "Bearer bar"}}, ""),
         "bar");
  expect(makeRequest(http::verb::post, "/",
                     {{http::field::authorization, "Bearer bar"}}, ""),
         "bar");
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, extractDatasetClause) {
  using namespace ad_utility::use_type_identity;
  auto expect = []<typename T>(const auto& request, TI<T>,
                               const std::vector<DatasetClause>& expected,
                               const ad_utility::source_location l =
                                   ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    // Initialize an empty operation with no dataset clauses set.
    builder.parsedRequest_.operation_ = T{"", {}};
    builder.extractDatasetClauses();
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<T>(
                    AD_FIELD(T, datasetClauses_, testing::Eq(expected))));
  };
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  expect(makeGetRequest("/"), ti<Query>, {});
  expect(makeGetRequest("/?default-graph-uri=foo"), ti<Query>,
         {{Iri("<foo>"), false}});
  expect(makeGetRequest("/?named-graph-uri=bar"), ti<Query>,
         {{Iri("<bar>"), true}});
  expect(makeGetRequest("/?default-graph-uri=foo&named-graph-uri=bar&using-"
                        "graph-uri=baz&using-named-graph-uri=abc"),
         ti<Query>, {{Iri("<foo>"), false}, {Iri("<bar>"), true}});
  expect(makePostRequest("/?default-graph-uri=foo&named-graph-uri=bar&using-"
                         "graph-uri=baz&using-named-graph-uri=abc",
                         "", ""),
         ti<Update>, {{Iri("<baz>"), false}, {Iri("<abc>"), true}});
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, extractOperationIfSpecified) {
  using namespace ad_utility::use_type_identity;
  auto expect = []<typename T>(const auto& request, TI<T>,
                               std::string_view paramName,
                               const Operation& expected,
                               const ad_utility::source_location l =
                                   ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<None>(None{}));
    // Initialize an empty operation with no dataset clauses set.
    builder.extractOperationIfSpecified(ti<T>, paramName);
    EXPECT_THAT(builder.parsedRequest_.operation_, testing::Eq(expected));
  };
  expect(makeGetRequest("/"), ti<Query>, "query", None{});
  expect(makeGetRequest("/?query=foo"), ti<Update>, "update", None{});
  expect(makeGetRequest("/?query=foo"), ti<Query>, "query", Query{"foo", {}});
  expect(makePostRequest("/", "", ""), ti<Update>, "update", None{});
  expect(makePostRequest("/?update=bar", "", ""), ti<Update>, "update",
         Update{"bar", {}});
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, isGraphStoreOperation) {
  auto isGraphStoreOperation =
      [](const ad_utility::httpUtils::HttpRequest auto& request) {
        const auto builder = ParsedRequestBuilder(request);
        return builder.isGraphStoreOperation();
      };
  EXPECT_THAT(isGraphStoreOperation(makeGetRequest("/")), testing::IsFalse());
  EXPECT_THAT(
      isGraphStoreOperation(makeGetRequest("/?query=foo&access-token=bar")),
      testing::IsFalse());
  EXPECT_THAT(isGraphStoreOperation(makeGetRequest("/?default")),
              testing::IsTrue());
  EXPECT_THAT(isGraphStoreOperation(makeGetRequest("/?graph=foo")),
              testing::IsTrue());
  EXPECT_THAT(isGraphStoreOperation(
                  makeGetRequest("/default?query=foo&access-token=bar")),
              testing::IsFalse());
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, extractGraphStoreOperation) {
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  auto expect = [](const auto& request, const GraphOrDefault& graph,
                   const ad_utility::source_location l =
                       ad_utility::source_location::current()) {
    auto t = generateLocationTrace(l);
    auto builder = ParsedRequestBuilder(request);
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<None>(None{}));
    builder.extractGraphStoreOperation();
    EXPECT_THAT(builder.parsedRequest_.operation_,
                testing::VariantWith<GraphStoreOperation>(
                    AD_FIELD(GraphStoreOperation, graph_, testing::Eq(graph))));
  };
  expect(makeGetRequest("/?default"), DEFAULT{});
  expect(makeGetRequest("/?graph=foo"), Iri("<foo>"));
  expect(makePostRequest("/?default", "", ""), DEFAULT{});
  expect(makePostRequest("/?graph=bar", "", ""), Iri("<bar>"));
  {
    auto builder = ParsedRequestBuilder(makeGetRequest("/?default&graph=foo"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        builder.extractGraphStoreOperation(),
        testing::HasSubstr(
            R"(Parameters "graph" and "default" must not be set at the same time.)"));
  }
  {
    auto builder = ParsedRequestBuilder(makeGetRequest("/default"));
    builder.parsedRequest_.operation_ = Query{"foo", {}};
    AD_EXPECT_THROW_WITH_MESSAGE(builder.extractGraphStoreOperation(),
                                 testing::HasSubstr(""));
  }
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, parametersContain) {
  auto builder =
      ParsedRequestBuilder(makeGetRequest("/?query=foo&access-token=bar&baz"));
  EXPECT_THAT(builder.parametersContain("query"), testing::IsTrue());
  EXPECT_THAT(builder.parametersContain("access-token"), testing::IsTrue());
  EXPECT_THAT(builder.parametersContain("baz"), testing::IsTrue());
  EXPECT_THAT(builder.parametersContain("default"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("graph"), testing::IsFalse());
  builder.parsedRequest_.parameters_ = {{"graph", {"foo"}}};
  EXPECT_THAT(builder.parametersContain("query"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("access-token"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("baz"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("default"), testing::IsFalse());
  EXPECT_THAT(builder.parametersContain("graph"), testing::IsTrue());
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, reportUnsupportedContentTypeIfGraphStore) {
  auto builderGraphStore = ParsedRequestBuilder(makeGetRequest("/?default"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      builderGraphStore.reportUnsupportedContentTypeIfGraphStore(
          "application/x-www-form-urlencoded"),
      testing::HasSubstr(""));
  auto builderQuery = ParsedRequestBuilder(makeGetRequest("/?query=foo"));
  EXPECT_NO_THROW(builderQuery.reportUnsupportedContentTypeIfGraphStore(
      "application/sparql-query"));
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, parameterIsContainedExactlyOnce) {
  auto builder = ParsedRequestBuilder(
      makeGetRequest("/?query=foo&access-token=bar&baz&query=baz"));
  EXPECT_THAT(builder.parameterIsContainedExactlyOnce("does-not-exist"),
              testing::IsFalse());
  EXPECT_THAT(builder.parameterIsContainedExactlyOnce("access-token"),
              testing::IsTrue());
  AD_EXPECT_THROW_WITH_MESSAGE(
      builder.parameterIsContainedExactlyOnce("query"),
      testing::HasSubstr(
          "Parameter \"query\" must be given exactly once. Is: 2"));
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, extractTargetGraph) {
  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  const auto extractTargetGraph = ParsedRequestBuilder::extractTargetGraph;
  // Equivalent to `/?default`
  EXPECT_THAT(extractTargetGraph({{"default", {""}}}), DEFAULT{});
  // Equivalent to `/?graph=foo`
  EXPECT_THAT(extractTargetGraph({{"graph", {"foo"}}}), Iri("<foo>"));
  // Equivalent to `/?graph=foo&graph=bar`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({{"graph", {"foo", "bar"}}}),
      testing::HasSubstr(
          "Parameter \"graph\" must be given exactly once. Is: 2"));
  const std::string eitherDefaultOrGraphErrorMsg =
      R"(Exactly one of the query parameters "default" or "graph" must be set to identify the graph for the graph store protocol request.)";
  // Equivalent to `/` or `/?`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({}), testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
  // Equivalent to `/?unrelated=a&unrelated=b`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({{"unrelated", {"a", "b"}}}),
      testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
  // Equivalent to `/?default&graph=foo`
  AD_EXPECT_THROW_WITH_MESSAGE(
      extractTargetGraph({{"default", {""}}, {"graph", {"foo"}}}),
      testing::HasSubstr(eitherDefaultOrGraphErrorMsg));
}

// _____________________________________________________________________________________________
TEST(SPARQLProtocolTest, extractAccessTokenImpl) {
  auto extract = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    auto parsedUrl = parseRequestTarget(request.target());
    return ParsedRequestBuilder::extractAccessToken(request,
                                                    parsedUrl.parameters_);
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
          "Access token is specified both in the `Authorization` header and "
          "by the `access-token` parameter, but they are not the same"));
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
          "Access token is specified both in the `Authorization` header and "
          "by the `access-token` parameter, but they are not the same"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      extract(makeRequest(http::verb::post, "/?access-token=bar",
                          {{http::field::authorization, "foo"}})),
      testing::HasSubstr(
          "Authorization header doesn't start with \"Bearer \"."));
}
