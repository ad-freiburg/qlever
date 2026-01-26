// Copyright 2024-2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>

#include <boost/beast/http.hpp>

#include "engine/SparqlProtocol.h"
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
auto Iri = ad_utility::triple_component::Iri::fromIriref;

const std::string URLENCODED_PLAIN = "application/x-www-form-urlencoded";
const std::string URLENCODED = URLENCODED_PLAIN + ";charset=UTF-8";
const std::string QUERY = "application/sparql-query";
const std::string UPDATE = "application/sparql-update";
const std::string TURTLE = "text/turtle";

auto testAccessTokenCombinations = [](auto parse, const http::verb& method,
                                      std::string_view pathBase,
                                      const Operation& expectedOperation,
                                      const ad_utility::HashMap<http::field,
                                                                std::string>&
                                          headers = {},
                                      const std::optional<std::string>& body =
                                          std::nullopt,
                                      ad_utility::source_location l =
                                          AD_CURRENT_SOURCE_LOC()) {
  auto t = generateLocationTrace(l);
  // Test the cases:
  // 1. No access token
  // 2. Access token in query
  // 3. Access token in `Authorization` header
  // 4. Different access tokens
  // 5. Same access token
  boost::urls::url pathWithAccessToken{pathBase};
  pathWithAccessToken.params().append({"access-token", "foo"});
  ad_utility::HashMap<http::field, std::string> headersWithDifferentAccessToken{
      headers};
  headersWithDifferentAccessToken.insert(
      {http::field::authorization, "Bearer bar"});
  ad_utility::HashMap<http::field, std::string> headersWithSameAccessToken{
      headers};
  headersWithSameAccessToken.insert({http::field::authorization, "Bearer foo"});
  EXPECT_THAT(parse(makeRequest(method, pathBase, headers, body)),
              ParsedRequestIs("/", std::nullopt, {}, expectedOperation));
  EXPECT_THAT(
      parse(makeRequest(method, pathWithAccessToken.buffer(), headers, body)),
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
      testing::HasSubstr("Access token is specified both in the "
                         "`Authorization` header and by the `access-token` "
                         "parameter, but they are not the same"));
};
auto testAccessTokenCombinationsUrlEncoded = [](auto parse,
                                                const std::string& bodyBase,
                                                const Operation&
                                                    expectedOperation,
                                                ad_utility::source_location l =
                                                    AD_CURRENT_SOURCE_LOC()) {
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
  ad_utility::HashMap<http::field, std::string> headersWithDifferentAccessToken{
      {http::field::content_type, {URLENCODED}},
      {http::field::authorization, "Bearer bar"}};
  ad_utility::HashMap<http::field, std::string> headersWithSameAccessToken{
      {http::field::content_type, {URLENCODED}},
      {http::field::authorization, "Bearer foo"}};
  EXPECT_THAT(parse(makeRequest(http::verb::post, "/", headers, bodyBase)),
              ParsedRequestIs("/", std::nullopt, {}, expectedOperation));
  EXPECT_THAT(
      parse(makeRequest(http::verb::post, "/", headers, bodyWithAccessToken)),
      ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                      expectedOperation));
  EXPECT_THAT(parse(makeRequest(http::verb::post, "/",
                                headersWithDifferentAccessToken, bodyBase)),
              ParsedRequestIs("/", "bar", {}, expectedOperation));
  EXPECT_THAT(parse(makeRequest(http::verb::post, "/",
                                headersWithSameAccessToken, bodyBase)),
              ParsedRequestIs("/", "foo", {}, expectedOperation));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeRequest(http::verb::post, "/", headersWithDifferentAccessToken,
                        bodyWithAccessToken)),
      testing::HasSubstr("Access token is specified both in the "
                         "`Authorization` header and by the `access-token` "
                         "parameter, but they are not the same"));
};

}  // namespace

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parseGET) {
  auto parse =
      CPP_template_lambda()(typename RequestT)(const RequestT& request)(
          requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    return SparqlProtocol::parseGET(request);
  };
  // No SPARQL Operation
  EXPECT_THAT(parse(makeGetRequest("/")),
              ParsedRequestIs("/", std::nullopt, {}, None{}));
  EXPECT_THAT(parse(makeGetRequest("/ping")),
              ParsedRequestIs("/ping", std::nullopt, {}, None{}));
  EXPECT_THAT(parse(makeGetRequest("/?cmd=stats")),
              ParsedRequestIs("/", std::nullopt, {{"cmd", {"stats"}}}, None{}));
  // Query
  EXPECT_THAT(parse(makeGetRequest(
                  "/?query=SELECT+%2A%20WHERE%20%7B%7D&action=csv_export")),
              ParsedRequestIs("/", std::nullopt, {{"action", {"csv_export"}}},
                              Query{"SELECT * WHERE {}", {}}));
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
  // Access token is read correctly
  testAccessTokenCombinations(parse, http::verb::get, "/?query=a",
                              Query{"a", {}});
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?query=SELECT%20%2A%20WHERE%20%7B%7D&query=SELECT%"
                           "20%3Ffoo%20WHERE%20%7B%7D")),
      ::testing::StrEq(
          "Parameter \"query\" must be given exactly once. Is: 2"));
  // Update (not allowed)
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeGetRequest("/?update=DELETE%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("SPARQL Update is not allowed as GET request."));
  // Graph Store Operation
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
}

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parseUrlencodedPOST) {
  auto parse =
      CPP_template_lambda()(typename RequestT)(const RequestT& request)(
          requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    return SparqlProtocol::parseUrlencodedPOST(request);
  };

  // No SPARQL Operation
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED, "cmd=clear-cache")),
      ParsedRequestIs("/", std::nullopt, {{"cmd", {"clear-cache"}}}, None{}));
  // Query
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "query=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                      Query{"SELECT * WHERE {}", {}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
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
  testAccessTokenCombinationsUrlEncoded(parse,
                                        "query=SELECT%20%2A%20WHERE%20%7B%7D",
                                        Query{"SELECT * WHERE {}", {}});
  // Update
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED,
                            "update=DELETE%20%2A%20WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}", {}}));
  EXPECT_THAT(
      parse(
          makePostRequest("/", URLENCODED, "update=DELETE+%2A+WHERE%20%7B%7D")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}", {}}));
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
  testAccessTokenCombinationsUrlEncoded(parse, "update=DELETE%20WHERE%20%7B%7D",
                                        Update{"DELETE WHERE {}", {}});
  // Error conditions
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/?send=100", URLENCODED,
                            "query=SELECT%20%2A%20WHERE%20%7B%7D")),
      testing::StrEq("URL-encoded POST requests must not contain query "
                     "parameters in the URL."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED,
                            "ääär y=SELECT+%2A%20WHERE%20%7B%7D&send=100")),
      ::testing::HasSubstr("Invalid URL-encoded POST request"));
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
  // Graph Store Protocol (not allowed)
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED, "graph=foo")),
      testing::HasSubstr(absl::StrCat("Unsupported Content type \"",
                                      URLENCODED_PLAIN,
                                      "\" for Graph Store protocol.")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makePostRequest("/", URLENCODED, "default")),
      testing::HasSubstr(absl::StrCat("Unsupported Content type \"",
                                      URLENCODED_PLAIN,
                                      "\" for Graph Store protocol.")));
}

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parseQueryPOST) {
  auto parse =
      CPP_template_lambda()(typename RequestT)(const RequestT& request)(
          requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    return SparqlProtocol::parseSPARQLPOST<Query>(
        request, SparqlProtocol::contentTypeSparqlQuery);
  };

  // Query
  EXPECT_THAT(
      parse(makePostRequest("/", QUERY, "SELECT * WHERE {}")),
      ParsedRequestIs("/", std::nullopt, {}, Query{"SELECT * WHERE {}", {}}));
  EXPECT_THAT(parse(makePostRequest("/?send=100", QUERY, "SELECT * WHERE {}")),
              ParsedRequestIs("/", std::nullopt, {{"send", {"100"}}},
                              Query{"SELECT * WHERE {}", {}}));
  // Check that the correct datasets for the method (GET or POST) are added
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
  // Access token is read correctly
  testAccessTokenCombinations(parse, http::verb::post, "/", Query{"a", {}},
                              {{http::field::content_type, QUERY}}, "a");
  // Graph Store Operation (not allowed)
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
}

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parseUpdatePOST) {
  auto parse =
      CPP_template_lambda()(typename RequestT)(const RequestT& request)(
          requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    return SparqlProtocol::parseSPARQLPOST<Update>(
        request, SparqlProtocol::contentTypeSparqlUpdate);
  };

  // Update
  EXPECT_THAT(
      parse(makePostRequest("/", UPDATE, "DELETE * WHERE {}")),
      ParsedRequestIs("/", std::nullopt, {}, Update{"DELETE * WHERE {}", {}}));
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
  // Access token is read correctly
  testAccessTokenCombinations(parse, http::verb::post, "/", Update{"a", {}},
                              {{http::field::content_type, UPDATE}}, "a");
  // Graph Store Protocol (not allowed)
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
}

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parsePOST) {
  auto parse =
      CPP_template_lambda()(typename RequestT)(const RequestT& request)(
          requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    return SparqlProtocol::parsePOST(request);
  };

  // Query
  EXPECT_THAT(parse(makePostRequest("/?access-token=foo", QUERY,
                                    "SELECT * WHERE { ?s ?p ?o }")),
              ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                              Query{"SELECT * WHERE { ?s ?p ?o }", {}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED, "access-token=foo&query=bar")),
      ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                      Query{"bar", {}}));
  // Update
  EXPECT_THAT(parse(makePostRequest("/?access-token=foo", UPDATE,
                                    "INSERT DATA { <a> <b> <c> }")),
              ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                              Update{"INSERT DATA { <a> <b> <c> }", {}}));
  // Update
  // Graph Store Operation
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
  // Unsupported content type
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeRequest(
          http::verb::post, "/",
          {{http::field::content_type, {"unsupported/content-type"}}}, "")),
      testing::HasSubstr(
          R"(POST request with content type "unsupported/content-type" not supported (must be Query/Update with content type "application/x-www-form-urlencoded", "application/sparql-query" or "application/sparql-update" or a valid graph store protocol POST request)"));
}

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parseHttpRequest) {
  auto parse = CPP_template_lambda()(typename RequestT)(RequestT request)(
      requires ad_utility::httpUtils::HttpRequest<RequestT>) {
    return SparqlProtocol::parseHttpRequest(request);
  };

  // Query
  EXPECT_THAT(parse(makeGetRequest("/?query=foo")),
              ParsedRequestIs("/", std::nullopt, {}, Query{"foo", {}}));
  EXPECT_THAT(
      parse(makePostRequest("/", URLENCODED, "access-token=foo&query=bar")),
      ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                      Query{"bar", {}}));
  // TODO<qup42> remove once the conformance tests are fixed
  // We add the leading `/` if it is missing.
  EXPECT_THAT(parse(makeGetRequest("?query=foo")),
              ParsedRequestIs("/", std::nullopt, {}, Query{"foo", {}}));
  // Update
  EXPECT_THAT(parse(makePostRequest("/?access-token=foo", UPDATE,
                                    "INSERT DATA { <a> <b> <c> }")),
              ParsedRequestIs("/", "foo", {{"access-token", {"foo"}}},
                              Update{"INSERT DATA { <a> <b> <c> }", {}}));
  // Graph Store Protocol (Direct Graph Identification)
  {
    auto path =
        absl::StrCat("/", GSP_DIRECT_GRAPH_IDENTIFICATION_PREFIX, "/foo");
    EXPECT_THAT(parse(makeRequest(http::verb::get, path,
                                  {{http::field::host, {"example.com"}}})),
                ParsedRequestIs(path, std::nullopt, {},
                                GraphStoreOperation{Iri(absl::StrCat(
                                    "<http://example.com", path, ">"))}));
  }
  // Graph Store Protocol (Indirect Graph Identification)
  EXPECT_THAT(parse(makeGetRequest("/?graph=foo")),
              ParsedRequestIs("/", std::nullopt, {{"graph", {"foo"}}},
                              GraphStoreOperation{Iri("<foo>")}));
  EXPECT_THAT(parse(makeRequest(http::verb::delete_, "/?graph=foo")),
              ParsedRequestIs("/", std::nullopt, {{"graph", {"foo"}}},
                              GraphStoreOperation{Iri("<foo>")}));
  EXPECT_THAT(parse(makeRequest("TSOP", "/?graph=foo")),
              ParsedRequestIs("/", std::nullopt, {{"graph", {"foo"}}},
                              GraphStoreOperation{Iri("<foo>")}));

  // Unsupported HTTP Method
  AD_EXPECT_THROW_WITH_MESSAGE(
      parse(makeRequest(http::verb::patch, "/")),
      testing::StrEq("Request method \"PATCH\" not supported (GET, POST, TSOP, "
                     "PUT and DELETE are supported; HEAD and PATCH for graph "
                     "store protocol are not yet supported)"));
  AD_EXPECT_THROW_WITH_MESSAGE(parse(makeGetRequest(" ")),
                               testing::StrEq("Failed to parse URL: \"/ \"."));
}

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parseGraphStoreProtocolIndirect) {
  EXPECT_THAT(SparqlProtocol::parseGraphStoreProtocolIndirect(
                  makeGetRequest("/?default&access-token=foo")),
              ParsedRequestIs("/", "foo",
                              {{"default", {""}}, {"access-token", {"foo"}}},
                              GraphStoreOperation{DEFAULT{}}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      SparqlProtocol::parseGraphStoreProtocolIndirect(
          makeGetRequest(absl::StrCat(
              "/", GSP_DIRECT_GRAPH_IDENTIFICATION_PREFIX, "/foo.ttl"))),
      testing::HasSubstr(
          R"(Expecting a Graph Store Protocol request, but missing query parameters "graph" or "default" in request)"));
}

// _____________________________________________________________________________________________
TEST(SparqlProtocolTest, parseGraphStoreProtocolDirect) {
  auto path = absl::StrCat("/", GSP_DIRECT_GRAPH_IDENTIFICATION_PREFIX, "/foo");
  EXPECT_THAT(SparqlProtocol::parseGraphStoreProtocolDirect(makeRequest(
                  http::verb::get, path, {{http::field::host, "example.com"}})),
              ParsedRequestIs(path, std::nullopt, {},
                              GraphStoreOperation{Iri(absl::StrCat(
                                  "<http://example.com", path, ">"))}));
}
