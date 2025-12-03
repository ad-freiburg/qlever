// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>

#include <boost/beast/http.hpp>

#include "engine/QueryPlanner.h"
#include "engine/Server.h"
#include "parser/SparqlParser.h"
#include "util/GTestHelpers.h"
#include "util/HttpRequestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"
#include "util/json.h"

using nlohmann::json;

namespace {
using namespace ad_utility::url_parser;
using namespace ad_utility::url_parser::sparqlOperation;
using namespace ad_utility::testing;

constexpr auto encodedIriManager = []() -> const EncodedIriManager* {
  static EncodedIriManager encodedIriManager_;
  return &encodedIriManager_;
};
auto parseQuery(std::string query,
                const std::vector<DatasetClause>& datasets = {}) {
  return SparqlParser::parseQuery(encodedIriManager(), std::move(query),
                                  datasets);
}

}  // namespace
TEST(ServerTest, determineResultPinning) {
  EXPECT_THAT(Server::determineResultPinning(
                  {{"pin-subresults", {"true"}}, {"pin-result", {"true"}}}),
              testing::Pair(true, true));
  EXPECT_THAT(Server::determineResultPinning({{"pin-result", {"true"}}}),
              testing::Pair(false, true));
  EXPECT_THAT(
      Server::determineResultPinning({{"pin-subresults", {"otherValue"}}}),
      testing::Pair(false, false));
}

// _____________________________________________________________________________
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
    EXPECT_THAT(Server::determineMediaTypes({{"action", {actionName}}},
                                            MakeRequest(std::nullopt)),
                testing::ElementsAre(expectedMediaType));
  };
  // The media type associated with the action overrides the `Accept` header.
  EXPECT_THAT(Server::determineMediaTypes(
                  {{"action", {"csv_export"}}},
                  MakeRequest("application/sparql-results+json")),
              testing::ElementsAre(ad_utility::MediaType::csv));
  checkActionMediatype("csv_export", ad_utility::MediaType::csv);
  checkActionMediatype("tsv_export", ad_utility::MediaType::tsv);
  checkActionMediatype("qlever_json_export", ad_utility::MediaType::qleverJson);
  checkActionMediatype("sparql_json_export", ad_utility::MediaType::sparqlJson);
  checkActionMediatype("turtle_export", ad_utility::MediaType::turtle);
  checkActionMediatype("binary_export", ad_utility::MediaType::octetStream);
  EXPECT_THAT(Server::determineMediaTypes(
                  {}, MakeRequest("application/sparql-results+json")),
              testing::ElementsAre(ad_utility::MediaType::sparqlJson));
  // No supported media type in the `Accept` header. (Contrary to it's docstring
  // and interface) `ad_utility::getMediaTypeFromAcceptHeader` throws an
  // exception if no supported media type is found.
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::determineMediaTypes({}, MakeRequest("text/css")),
      testing::HasSubstr("Not a single media type known to this parser was "
                         "detected in \"text/css\"."));
  // No `Accept` header means that any content type is allowed.
  EXPECT_THAT(Server::determineMediaTypes({}, MakeRequest(std::nullopt)),
              testing::ElementsAre());
  // No `Accept` header and an empty `Accept` header are not distinguished.
  EXPECT_THAT(Server::determineMediaTypes({}, MakeRequest("")),
              testing::ElementsAre());
}

// _____________________________________________________________________________
TEST(ServerTest, chooseBestFittingMediaType) {
  auto askQuery = parseQuery("ASK {}");
  auto selectQuery = parseQuery("SELECT * {}");
  auto constructQuery = parseQuery("CONSTRUCT WHERE {}");
  using enum ad_utility::MediaType;

  auto choose = &Server::chooseBestFittingMediaType;

  // Empty case
  EXPECT_EQ(choose({}, askQuery), sparqlJson);
  EXPECT_EQ(choose({}, selectQuery), sparqlJson);
  EXPECT_EQ(choose({}, constructQuery), turtle);

  // Single matching element
  EXPECT_EQ(choose({sparqlJson}, askQuery), sparqlJson);
  EXPECT_EQ(choose({sparqlJson}, selectQuery), sparqlJson);
  EXPECT_EQ(choose({turtle}, constructQuery), turtle);
  EXPECT_EQ(choose({qleverJson}, askQuery), qleverJson);
  EXPECT_EQ(choose({qleverJson}, selectQuery), qleverJson);
  EXPECT_EQ(choose({qleverJson}, constructQuery), qleverJson);

  // Single non-matching element
  EXPECT_EQ(choose({tsv}, askQuery), sparqlJson);
  EXPECT_EQ(choose({turtle}, selectQuery), sparqlJson);
  EXPECT_EQ(choose({octetStream}, constructQuery), turtle);

  // Multiple matching elements
  EXPECT_EQ(choose({sparqlJson, qleverJson}, askQuery), sparqlJson);
  EXPECT_EQ(choose({sparqlJson, qleverJson}, selectQuery), sparqlJson);
  EXPECT_EQ(choose({turtle, qleverJson}, constructQuery), turtle);

  // One matching, one non-matching element
  EXPECT_EQ(choose({tsv, qleverJson}, askQuery), qleverJson);
  EXPECT_EQ(choose({turtle, qleverJson}, selectQuery), qleverJson);
  EXPECT_EQ(choose({octetStream, qleverJson}, constructQuery), qleverJson);

  // Multiple non-matching elements
  EXPECT_EQ(choose({tsv, csv}, askQuery), sparqlJson);
  EXPECT_EQ(choose({turtle, json}, selectQuery), sparqlJson);
  EXPECT_EQ(choose({octetStream, sparqlJson}, constructQuery), turtle);
}

// _____________________________________________________________________________
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

TEST(ServerTest, composeStatsJson) {
  Server server{9999, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
  json expectedJson{{"git-hash-index", "git short hash not set"},
                    {"git-hash-server", "git short hash not set"},
                    {"name-index", ""},
                    {"name-text-index", ""},
                    {"num-entity-occurrences", 0},
                    {"num-permutations", 2},
                    {"num-predicates-internal", 0},
                    {"num-predicates-normal", 0},
                    {"num-text-records", 0},
                    {"num-triples-internal", 0},
                    {"num-triples-normal", 0},
                    {"num-word-occurrences", 0}};
  EXPECT_THAT(server.composeStatsJson(), testing::Eq(expectedJson));
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
  ad_utility::BlankNodeManager bnm;
  auto pqs = SparqlParser::parseUpdate(&bnm, encodedIriManager(), update);
  EXPECT_THAT(pqs, testing::SizeIs(1));
  ParsedQuery pq = std::move(pqs[0]);
  QueryPlanner qp(qec, handle);
  QueryExecutionTree qet = qp.createExecutionTree(pq);
  const Server::PlannedQuery plannedQuery{std::move(pq), std::move(qet)};

  // Execute the update
  DeltaTriplesCount countBefore = deltaTriples.getCounts();
  UpdateMetadata updateMetadata = ExecuteUpdate::executeUpdate(
      index, plannedQuery.parsedQuery_, plannedQuery.queryExecutionTree_,
      deltaTriples, handle);
  updateMetadata.countBefore_ = countBefore;
  updateMetadata.countAfter_ = deltaTriples.getCounts();

  // Assertions
  ad_utility::timer::TimeTracer tracer2(
      "ServerTest::createResponseMetadata tracer2");
  tracer2.endTrace("ServerTest::createResponseMetadata tracer2");
  json metadata = Server::createResponseMetadataForUpdate(
      index, deltaTriples.getSnapshot(), plannedQuery,
      plannedQuery.queryExecutionTree_, updateMetadata, tracer2);
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

TEST(ServerTest, adjustParsedQueryLimitOffset) {
  using enum ad_utility::MediaType;
  auto makePlannedQuery = [](std::string operation) -> Server::PlannedQuery {
    ParsedQuery parsed = parseQuery(std::move(operation));
    QueryExecutionTree qet =
        QueryPlanner{ad_utility::testing::getQec(),
                     std::make_shared<ad_utility::CancellationHandle<>>()}
            .createExecutionTree(parsed);
    return {std::move(parsed), std::move(qet)};
  };
  auto expectExportLimit =
      [&makePlannedQuery](
          ad_utility::MediaType mediaType, std::optional<uint64_t> limit,
          std::string operation =
              "SELECT * WHERE { <a> <b> ?c } LIMIT 10 OFFSET 15",
          const ad_utility::url_parser::ParamValueMap& parameters = {{"send",
                                                                      {"12"}}},
          ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        auto pq = makePlannedQuery(std::move(operation));
        Server::adjustParsedQueryLimitOffset(pq, mediaType, parameters);
        EXPECT_THAT(pq.parsedQuery_._limitOffset.exportLimit_,
                    testing::Eq(limit));
      };

  std::string complexQuery{
      "SELECT * WHERE { ?a ?b ?c . FILTER(LANG(?a) = 'en') . "
      "BIND(RAND() as ?r) . } OFFSET 5"};
  // The export limit is only set for media type `qleverJson`.
  expectExportLimit(qleverJson, 12);
  expectExportLimit(qleverJson, 13, "SELECT * WHERE { <a> <b> ?c }",
                    {{"send", {"13"}}});
  expectExportLimit(qleverJson, 13, complexQuery, {{"send", {"13"}}});
  expectExportLimit(csv, std::nullopt);
  expectExportLimit(csv, std::nullopt, complexQuery);
  expectExportLimit(tsv, std::nullopt);
}

// _____________________________________________________________________________
TEST(ServerTest, configurePinnedResultWithName) {
  auto qec = ad_utility::testing::getQec();

  // Test with no pinNamed value - should not modify qec
  std::optional<std::string> noPinNamed = std::nullopt;
  Server::configurePinnedResultWithName(noPinNamed, std::nullopt, true, *qec);
  EXPECT_FALSE(qec->pinResultWithName().has_value());

  // Test with pinNamed and valid access token - should set the pin name
  std::optional<std::string> pinNamed = "test_query_name";
  Server::configurePinnedResultWithName(pinNamed, std::nullopt, true, *qec);
  EXPECT_TRUE(qec->pinResultWithName().has_value());
  EXPECT_EQ(qec->pinResultWithName().value().name_, "test_query_name");

  // Reset for next test
  qec->pinResultWithName() = std::nullopt;
  // Test with pinNamed AND pinned geo Var.
  Server::configurePinnedResultWithName(pinNamed, "geom_var", true, *qec);
  ASSERT_TRUE(qec->pinResultWithName().has_value());
  EXPECT_EQ(qec->pinResultWithName().value().name_, "test_query_name");
  EXPECT_THAT(qec->pinResultWithName().value().geoIndexVar_,
              ::testing::Optional(Variable{"?geom_var"}));

  // Reset for next test
  qec->pinResultWithName() = std::nullopt;

  // Test with pinNamed but invalid access token - should throw exception
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::configurePinnedResultWithName(pinNamed, std::nullopt, false,
                                            *qec),
      testing::HasSubstr(
          "Pinning a result with a name requires a valid access token"));

  // Verify qec was not modified when exception was thrown
  EXPECT_FALSE(qec->pinResultWithName().has_value());
}

TEST(ServerTest, checkAccessToken) {
  Server server{4321, 1, ad_utility::MemorySize::megabytes(1), "accessToken"};
  EXPECT_TRUE(server.checkAccessToken("accessToken"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      server.checkAccessToken("invalidAccessToken"),
      testing::HasSubstr("Access token was provided but it was invalid"));

  Server server2{1234, 1, ad_utility::MemorySize::megabytes(1), "", true};
  EXPECT_TRUE(server2.checkAccessToken(std::nullopt));
}
