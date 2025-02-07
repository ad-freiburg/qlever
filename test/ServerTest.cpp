// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <engine/QueryPlanner.h>
#include <engine/Server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/beast/http.hpp>

#include "util/GTestHelpers.h"
#include "util/HttpRequestHelpers.h"
#include "util/IndexTestHelpers.h"
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

TEST(ServerTest, parseHttpRequest) {
  auto parse = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    return Server::parseHttpRequest(request);
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

TEST(ServerTest, determineResultPinning) {
  EXPECT_THAT(Server::determineResultPinning(
                  {{"pinsubtrees", {"true"}}, {"pinresult", {"true"}}}),
              testing::Pair(true, true));
  EXPECT_THAT(Server::determineResultPinning({{"pinresult", {"true"}}}),
              testing::Pair(false, true));
  EXPECT_THAT(Server::determineResultPinning({{"pinsubtrees", {"otherValue"}}}),
              testing::Pair(false, false));
}

TEST(ServerTest, determineMediaType) {
  auto MakeRequest = [](const std::optional<std::string>& accept,
                        const http::verb method = http::verb::get,
                        const std::string& target = "/",
                        const std::string& body = "") {
    auto req = http::request<http::string_body>{method, target, 11};
    if (accept.has_value()) {
      req.set(http::field::accept, accept.value());
    }
    req.body() = body;
    req.prepare_payload();
    return req;
  };
  auto checkActionMediatype = [&](const std::string& actionName,
                                  ad_utility::MediaType expectedMediaType) {
    EXPECT_THAT(Server::determineMediaType({{"action", {actionName}}},
                                           MakeRequest(std::nullopt)),
                testing::Eq(expectedMediaType));
  };
  // The media type associated with the action overrides the `Accept` header.
  EXPECT_THAT(Server::determineMediaType(
                  {{"action", {"csv_export"}}},
                  MakeRequest("application/sparql-results+json")),
              testing::Eq(ad_utility::MediaType::csv));
  checkActionMediatype("csv_export", ad_utility::MediaType::csv);
  checkActionMediatype("tsv_export", ad_utility::MediaType::tsv);
  checkActionMediatype("qlever_json_export", ad_utility::MediaType::qleverJson);
  checkActionMediatype("sparql_json_export", ad_utility::MediaType::sparqlJson);
  checkActionMediatype("turtle_export", ad_utility::MediaType::turtle);
  checkActionMediatype("binary_export", ad_utility::MediaType::octetStream);
  EXPECT_THAT(Server::determineMediaType(
                  {}, MakeRequest("application/sparql-results+json")),
              testing::Eq(ad_utility::MediaType::sparqlJson));
  // No supported media type in the `Accept` header. (Contrary to it's docstring
  // and interface) `ad_utility::getMediaTypeFromAcceptHeader` throws an
  // exception if no supported media type is found.
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::determineMediaType({}, MakeRequest("text/css")),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"text/css\"."));
  // No `Accept` header means that any content type is allowed.
  EXPECT_THAT(Server::determineMediaType({}, MakeRequest(std::nullopt)),
              testing::Eq(ad_utility::MediaType::sparqlJson));
  // No `Accept` header and an empty `Accept` header are not distinguished.
  EXPECT_THAT(Server::determineMediaType({}, MakeRequest("")),
              testing::Eq(ad_utility::MediaType::sparqlJson));
}

TEST(ServerTest, getQueryId) {
  using namespace ad_utility::websocket;
  Server server{9999, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
  auto reqWithExplicitQueryId = makeGetRequest("/");
  reqWithExplicitQueryId.set("Query-Id", "100");
  const auto req = makeGetRequest("/");
  {
    // A request with a custom query id.
    auto queryId1 = server.getQueryId(reqWithExplicitQueryId,
                                      "SELECT * WHERE { ?a ?b ?c }");
    // Another request with the same custom query id. This throws an error,
    // because query id cannot be used for multiple queries at the same time.
    AD_EXPECT_THROW_WITH_MESSAGE(
        server.getQueryId(reqWithExplicitQueryId,
                          "SELECT * WHERE { ?a ?b ?c }"),
        testing::HasSubstr("Query id '100' is already in use!"));
  }
  // The custom query id can be reused, once the query is finished.
  auto queryId1 =
      server.getQueryId(reqWithExplicitQueryId, "SELECT * WHERE { ?a ?b ?c }");
  // Without custom query ids, unique ids are generated.
  auto queryId2 = server.getQueryId(req, "SELECT * WHERE { ?a ?b ?c }");
  auto queryId3 = server.getQueryId(req, "SELECT * WHERE { ?a ?b ?c }");
}

TEST(ServerTest, createMessageSender) {
  Server server{9999, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
  auto reqWithExplicitQueryId = makeGetRequest("/");
  std::string customQueryId = "100";
  reqWithExplicitQueryId.set("Query-Id", customQueryId);
  const auto req = makeGetRequest("/");
  // The query hub is only valid once, the server has been started.
  AD_EXPECT_THROW_WITH_MESSAGE(
      server.createMessageSender(server.queryHub_, req,
                                 "SELECT * WHERE { ?a ?b ?c }"),
      testing::HasSubstr("Assertion `queryHubLock` failed."));
  {
    // Set a dummy query hub.
    boost::asio::io_context io_context;
    auto queryHub =
        std::make_shared<ad_utility::websocket::QueryHub>(io_context);
    server.queryHub_ = queryHub;
    // MessageSenders are created normally.
    server.createMessageSender(server.queryHub_, req,
                               "SELECT * WHERE { ?a ?b ?c }");
    server.createMessageSender(server.queryHub_, req,
                               "INSERT DATA { <foo> <bar> <baz> }");
    EXPECT_THAT(
        server.createMessageSender(server.queryHub_, reqWithExplicitQueryId,
                                   "INSERT DATA { <foo> <bar> <baz> }"),
        AD_PROPERTY(ad_utility::websocket::MessageSender, getQueryId,
                    testing::Eq(ad_utility::websocket::QueryId::idFromString(
                        customQueryId))));
  }
  // Once the query hub expires (e.g. because the io context dies), message
  // senders can no longer be created.
  AD_EXPECT_THROW_WITH_MESSAGE(
      server.createMessageSender(server.queryHub_, req,
                                 "SELECT * WHERE { ?a ?b ?c }"),
      testing::HasSubstr("Assertion `queryHubLock` failed."));
}

TEST(ServerTest, createResponseMetadata) {
  // Setup the datastructures
  const ad_utility::SharedCancellationHandle handle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  const ad_utility::Timer requestTimer{
      ad_utility::Timer::InitialStatus::Stopped};
  QueryExecutionContext* qec = ad_utility::testing::getQec("<a> <b> <c>");
  const Index& index = qec->getIndex();
  DeltaTriples deltaTriples{index};
  const std::string update = "INSERT DATA { <b> <c> <d> }";
  ParsedQuery pq = SparqlParser::parseQuery(update);
  QueryPlanner qp(qec, handle);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  const Server::PlannedQuery plannedQuery{std::move(pq), std::move(qet)};

  // Execute the update
  DeltaTriplesCount countBefore = deltaTriples.getCounts();
  UpdateMetadata updateMetadata = ExecuteUpdate::executeUpdate(
      index, plannedQuery.parsedQuery_, plannedQuery.queryExecutionTree_,
      deltaTriples, handle);
  DeltaTriplesCount countAfter = deltaTriples.getCounts();

  // Assertions
  json metadata = Server::createResponseMetadataForUpdate(
      requestTimer, index, deltaTriples, plannedQuery,
      plannedQuery.queryExecutionTree_, countBefore, updateMetadata,
      countAfter);
  json deltaTriplesJson{
      {"before", {{"inserted", 0}, {"deleted", 0}, {"total", 0}}},
      {"after", {{"inserted", 1}, {"deleted", 0}, {"total", 1}}},
      {"difference", {{"inserted", 1}, {"deleted", 0}, {"total", 1}}}};
  json locatedTriplesJson{
      {"SPO", {{"blocks-affected", 1}, {"blocks-total", 1}}},
      {"POS", {{"blocks-affected", 1}, {"blocks-total", 1}}},
      {"OSP", {{"blocks-affected", 1}, {"blocks-total", 1}}},
      {"SOP", {{"blocks-affected", 1}, {"blocks-total", 1}}},
      {"PSO", {{"blocks-affected", 1}, {"blocks-total", 1}}},
      {"OPS", {{"blocks-affected", 1}, {"blocks-total", 1}}}};
  EXPECT_THAT(metadata["update"], testing::Eq(update));
  EXPECT_THAT(metadata["status"], testing::Eq("OK"));
  EXPECT_THAT(metadata["warnings"],
              testing::Eq(std::vector<std::string>{
                  "SPARQL 1.1 Update for QLever is experimental."}));
  EXPECT_THAT(metadata["delta-triples"], testing::Eq(deltaTriplesJson));
  EXPECT_THAT(metadata["located-triples"], testing::Eq(locatedTriplesJson));
}

TEST(ServerTest, extractAccessToken) {
  auto extract = [](const ad_utility::httpUtils::HttpRequest auto& request) {
    auto parsedUrl = parseRequestTarget(request.target());
    return Server::extractAccessToken(request, parsedUrl.parameters_);
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
