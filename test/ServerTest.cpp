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
      {"difference", {{"inserted", 1}, {"deleted", 0}, {"total", 1}}},
      {"operation", {{"inserted", 1}, {"deleted", 0}, {"total", 1}}}};
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
