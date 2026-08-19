// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@tf.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/beast/http.hpp>
#include <optional>

#include "./util/FileTestHelpers.h"
#include "./util/MetricsTestHelpers.h"
#include "./util/ParsedQueryTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
#include "ServerTestHelpers.h"
#include "backports/filesystem.h"
#include "engine/HttpError.h"
#include "engine/QueryPlanner.h"
#include "engine/Server.h"
#include "engine/UpdateMetadata.h"
#include "parser/SparqlParser.h"
#include "util/GTestHelpers.h"
#include "util/HttpRequestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/http/HttpUtils.h"
#include "util/http/UrlParser.h"
#include "util/json.h"
#include "util/metrics/Metrics.h"

using nlohmann::json;

namespace {
using namespace ad_utility::testing;
// Expect that `call()` throws an `HttpError` with status 403 Forbidden and
// with a message that matches `messageMatcher`.
auto expectForbiddenError = [](auto call, auto messageMatcher,
                               ad_utility::source_location l =
                                   AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  try {
    call();
    FAIL() << "Expected an `HttpError` to be thrown";
  } catch (const HttpError& e) {
    EXPECT_EQ(e.status(), boost::beast::http::status::forbidden);
    EXPECT_THAT(e.what(), messageMatcher);
  }
};
}  // namespace

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

  Server server{9999, 1, "accessToken", serverTestHelpers::getDefaultConfig()};
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

// _____________________________________________________________________________
TEST(ServerTest, createMessageSender) {
  Server server{9999, 1, "accessToken", serverTestHelpers::getDefaultConfig()};
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
    auto queryHub = std::make_shared<ad_utility::websocket::QueryHub>(
        io_context.get_executor());
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

// _____________________________________________________________________________
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
  const qlever::PlannedQuery plannedQuery{std::move(pq), std::move(qet), *qec};

  // Execute the update
  DeltaTriplesCount countBefore = deltaTriples.getCounts();
  UpdateMetadata updateMetadata = ExecuteUpdate::executeUpdate(
      index, plannedQuery.parsedQuery(), plannedQuery.queryExecutionTree(),
      deltaTriples, handle);
  updateMetadata.countBefore_ = countBefore;
  updateMetadata.countAfter_ = deltaTriples.getCounts();

  // Assertions
  ad_utility::timer::TimeTracer tracer2(
      "ServerTest::createResponseMetadata tracer2");
  tracer2.endTrace("ServerTest::createResponseMetadata tracer2");
  AD_EXPECT_THROW_WITH_MESSAGE(
      Server::createResponseMetadataForUpdate(
          *deltaTriples.getLocatedTriplesSharedStateReference(), plannedQuery,
          UpdateMetadata{}, tracer2),
      testing::HasSubstr("updateMetadata.countBefore_.has_value()"));
  json metadata = Server::createResponseMetadataForUpdate(
      *deltaTriples.getLocatedTriplesSharedStateReference(), plannedQuery,
      updateMetadata, tracer2);
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

// _____________________________________________________________________________
TEST(ServerTest, configurePinnedResultWithName) {
  auto qec = ad_utility::testing::getQec();

  // Test with no pin - should not modify qec
  Server::configurePinnedResultWithName(std::nullopt, true, *qec);
  EXPECT_FALSE(qec->pinResultWithName().has_value());

  // Test with a name and valid access token - should set the pin name
  Server::configurePinnedResultWithName(
      QueryExecutionContext::PinResultWithName{"test_query_name"}, true, *qec);
  ASSERT_TRUE(qec->pinResultWithName().has_value());
  EXPECT_EQ(qec->pinResultWithName().value().name_, "test_query_name");
  EXPECT_EQ(qec->pinResultWithName().value().geoIndexSimplificationInMeters_,
            std::nullopt);

  // Reset for next test
  qec->pinResultWithName() = std::nullopt;
  // Test with name AND pinned geo var.
  Server::configurePinnedResultWithName(
      QueryExecutionContext::PinResultWithName{"test_query_name",
                                               Variable{"?geom_var"}},
      true, *qec);
  ASSERT_TRUE(qec->pinResultWithName().has_value());
  EXPECT_EQ(qec->pinResultWithName().value().name_, "test_query_name");
  EXPECT_THAT(qec->pinResultWithName().value().geoIndexVar_,
              ::testing::Optional(Variable{"?geom_var"}));
  EXPECT_EQ(qec->pinResultWithName().value().geoIndexSimplificationInMeters_,
            std::nullopt);

  // Reset for next test
  qec->pinResultWithName() = std::nullopt;
  // Test with name, geo var, AND simplification.
  Server::configurePinnedResultWithName(
      QueryExecutionContext::PinResultWithName{"test_query_name",
                                               Variable{"?geom_var"}, 10.0},
      true, *qec);
  ASSERT_TRUE(qec->pinResultWithName().has_value());
  EXPECT_EQ(qec->pinResultWithName().value().name_, "test_query_name");
  EXPECT_THAT(qec->pinResultWithName().value().geoIndexVar_,
              ::testing::Optional(Variable{"?geom_var"}));
  EXPECT_THAT(qec->pinResultWithName().value().geoIndexSimplificationInMeters_,
              ::testing::Optional(10.0));

  // Reset for next test
  qec->pinResultWithName() = std::nullopt;

  // Pinning without a valid access token is rejected with 403 Forbidden.
  expectForbiddenError(
      [&] {
        Server::configurePinnedResultWithName(
            QueryExecutionContext::PinResultWithName{"test_query_name"}, false,
            *qec);
      },
      testing::HasSubstr(
          "Pinning a result with a name requires a valid access token"));

  // Verify qec was not modified when exception was thrown
  EXPECT_FALSE(qec->pinResultWithName().has_value());
}

// _____________________________________________________________________________
TEST(ServerTest, checkAccessToken) {
  auto config = serverTestHelpers::getDefaultConfig();
  Server server{4321, 1, "accessToken", config};
  EXPECT_TRUE(server.checkAccessToken("accessToken"));

  // An invalid access token results in a 403 Forbidden response.
  expectForbiddenError(
      [&] { server.checkAccessToken("invalidAccessToken"); },
      testing::HasSubstr("Access token was provided but it was invalid"));

  // Same when the server was started without `--access-token` at all.
  Server serverWithoutToken{4322, 1, "", config};
  expectForbiddenError(
      [&] { serverWithoutToken.checkAccessToken("someToken"); },
      testing::HasSubstr("Access token was provided but server was started "
                         "without --access-token"));

  // No access token provided at all is not an error here, it just means that
  // operations requiring one are rejected later.
  EXPECT_FALSE(serverWithoutToken.checkAccessToken(std::nullopt));

  config.persistUpdates_ = false;

  Server server2{
      1234, 1, "", config, true,
  };
  EXPECT_TRUE(server2.checkAccessToken(std::nullopt));
}

// _____________________________________________________________________________
MATCHER_P2(HeaderFieldIs, field, matcher,
           absl::StrCat(std::string{boost::beast::http::to_string(field)}, " ",
                        testing::DescribeMatcher<std::string>(matcher,
                                                              negation))) {
  auto it = arg.find(field);
  if (it == arg.end()) {
    *result_listener << "which has no " << field << " header";
    return false;
  }
  auto fieldValue = it->value();
  *result_listener << "which has " << field << " with " << fieldValue;
  return testing::ExplainMatchResult(matcher, fieldValue, result_listener);
}

// _____________________________________________________________________________
auto ContentTypeIs = [](const std::string& contentType) {
  return HeaderFieldIs(http::field::content_type, testing::StrEq(contentType));
};

// _____________________________________________________________________________
auto LocationIs = [](const auto& matcher) {
  return HeaderFieldIs(http::field::location, matcher);
};

// _____________________________________________________________________________
auto HasHeader = [](http::field field) {
  return HeaderFieldIs(field, testing::Not(testing::IsEmpty()));
};

// _____________________________________________________________________________
MATCHER_P(StatusIs, status,
          absl::StrCat("status is ", negation ? "not " : "",
                       testing::PrintToString(status))) {
  auto actualStatus = arg.base().result();
  *result_listener << "which has Status " << actualStatus;
  return actualStatus == status;
}

using namespace serverTestHelpers;

// A minimal MetricsReader that returns a fixed Prometheus-format string.
// Used for testing the `/metrics` endpoint routing without a real OTEL
// provider.
class FakeMetricsReader : public ad_utility::metrics::MetricsReader {
 public:
  std::string getMetricsText() const override {
    return "# HELP fake_counter A test counter.\nfake_counter 42\n";
  }
};

// _____________________________________________________________________________
TEST(ServerTest, metricsEndpoint) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> ."});
  auto makeServerWithMetrics =
      [&qec](
          std::shared_ptr<ad_utility::metrics::MetricsReader> metricsReader) {
        return ServerForTesting{
            1, "accessToken",
            getDefaultConfigWithName(qec->getIndex().getOnDiskBase()), false,
            std::move(metricsReader)};
      };
  auto expectMetrics = [](std::optional<std::string> accessToken,
                          ServerForTesting& server, const auto& responseMatcher,
                          const auto& bodyMatcher,
                          ad_utility::source_location l =
                              AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto request = makeGetRequest("/metrics");
    if (accessToken.has_value()) {
      request.set(http::field::authorization, "Bearer " + accessToken.value());
    }
    auto response = server.process(request);

    EXPECT_THAT(response, responseMatcher);
    EXPECT_THAT(responseBodyToString(std::move(response.body())), bodyMatcher);
  };
  auto expectRequiresAccessToken = [&](auto& server,
                                       ad_utility::source_location l =
                                           AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE(
        expectMetrics(std::nullopt, server, testing::_, testing::_),
        testing::HasSubstr("metrics requires a valid access token"));
  };
  auto UpdateRequest = [](std::string update) {
    return makeRequest(
        http::verb::post, "/",
        {{http::field::content_type, "application/sparql-update"},
         {http::field::authorization, "Bearer accessToken"}},
        std::move(update));
  };
  auto QueryRequest = [](std::string query) {
    return makeRequest(
        http::verb::post, "/",
        {{http::field::content_type, "application/sparql-query"}},
        std::move(query));
  };
  auto ExpectMetricsChange = [&makeServerWithMetrics, &expectMetrics](
                                 auto matcherBefore, auto request,
                                 auto matcherAfter,
                                 ad_utility::source_location l =
                                     AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto server = makeServerWithMetrics(ad_utility::metrics::initialize(true));
    expectMetrics("accessToken", server, StatusIs(http::status::ok),
                  matcherBefore);
    EXPECT_THAT(server.process(request), testing::_);
    expectMetrics("accessToken", server, StatusIs(http::status::ok),
                  matcherAfter);
  };
  {
    auto server = makeServerWithMetrics(nullptr);
    expectRequiresAccessToken(server);
    expectMetrics("accessToken", server, StatusIs(http::status::not_found),
                  testing::StrEq("Metrics not enabled (use --enable-metrics)"));
  }
  {
    auto server = makeServerWithMetrics(std::make_shared<FakeMetricsReader>());
    expectRequiresAccessToken(server);
    expectMetrics("accessToken", server, StatusIs(http::status::ok),
                  testing::HasSubstr("fake_counter 42"));
  }
  {
    auto server = makeServerWithMetrics(ad_utility::metrics::initialize(true));
    expectRequiresAccessToken(server);
  }
  {
    auto server = makeServerWithMetrics(ad_utility::metrics::initialize(true));
    // `qlever_build_info` is always present with a value of 1 and carries the
    // build metadata in its labels.
    expectMetrics(
        "accessToken", server, StatusIs(http::status::ok),
        testing::HasSubstr(
            "qlever_build_info{compile_time=\"time of compilation not set\","
            "compiler=\"compiler not set\","
            "compiler_version=\"compiler version not set\","
            "cxx_standard=\"c++ standard not set\","
            "git_hash=\"git short hash not set\","
            "version=\"project version not set\"} 1"));
  }
  Label update{"operation", "update"};
  Label query{"operation", "query"};
  Label syntaxError{"type", "syntax"};
  std::string_view qleverDeltaTriples = "qlever_delta_triples";
  std::string_view qleverSparqlOperationStartedTotal =
      "qlever_sparql_operation_started_total";
  std::string_view qleverSparqlOperationRunning =
      "qlever_sparql_operation_running";
  std::string_view qleverSparqlOperationErrorsTotal =
      "qlever_sparql_operation_errors_total";
  std::string_view qleverIndexRebuildInProgress =
      "qlever_index_rebuild_in_progress";
  ExpectMetricsChange(
      testing::AllOf(IsZero(qleverDeltaTriples),
                     IsZero(qleverSparqlOperationStartedTotal, update),
                     IsZero(qleverSparqlOperationStartedTotal, query),
                     IsZero(qleverSparqlOperationRunning, update),
                     IsZero(qleverSparqlOperationRunning, query)),
      UpdateRequest("INSERT DATA { <a> <b> <c> . <d> <e> <f> }"),
      testing::AllOf(MetricIs(qleverDeltaTriples, "2"),
                     MetricIs(qleverSparqlOperationStartedTotal, "1", update),
                     IsZero(qleverSparqlOperationStartedTotal, query),
                     IsZero(qleverSparqlOperationRunning, update),
                     IsZero(qleverSparqlOperationRunning, query)));
  ExpectMetricsChange(
      testing::AllOf(IsZero(qleverDeltaTriples),
                     IsZero(qleverSparqlOperationStartedTotal, update),
                     IsZero(qleverSparqlOperationStartedTotal, query),
                     IsZero(qleverSparqlOperationRunning, update),
                     IsZero(qleverSparqlOperationRunning, query)),
      QueryRequest("SELECT * WHERE { ?s ?p ?o } LIMIT 10"),
      testing::AllOf(MetricIs(qleverDeltaTriples, "0"),
                     IsZero(qleverSparqlOperationStartedTotal, update),
                     MetricIs(qleverSparqlOperationStartedTotal, "1", query),
                     IsZero(qleverSparqlOperationRunning, update),
                     IsZero(qleverSparqlOperationRunning, query)));
  // No rebuild is running during a normal query, so the rebuild-in-progress
  // gauge reads 0 both before and after.
  ExpectMetricsChange(IsZero(qleverIndexRebuildInProgress),
                      QueryRequest("SELECT * WHERE { ?s ?p ?o } LIMIT 10"),
                      IsZero(qleverIndexRebuildInProgress));
  ExpectMetricsChange(
      IsZero(qleverSparqlOperationErrorsTotal, syntaxError),
      QueryRequest("Foo"),
      MetricIs(qleverSparqlOperationErrorsTotal, "1", syntaxError));
  ExpectMetricsChange(
      IsZero(qleverSparqlOperationErrorsTotal, syntaxError),
      UpdateRequest("SELECT * WHERE { ?s ?p ?o } Limit 10"),
      MetricIs(qleverSparqlOperationErrorsTotal, "1", syntaxError));
}

// _____________________________________________________________________________
TEST(ServerTest, clearDeltaTriples) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> ."});
  auto server = makeServerForTesting(qec->getIndex().getOnDiskBase());

  auto expectCounts = [&server](DeltaTriplesCount expected) {
    EXPECT_THAT(server.deltaTriplesManager()
                    .getCurrentLocatedTriplesSharedState()
                    ->counts_,
                testing::Optional(testing::Eq(expected)));
  };

  auto insertRequest =
      makeRequest(http::verb::post, "/",
                  {{http::field::content_type, "application/sparql-update"},
                   {http::field::authorization, "Bearer accessToken"}},
                  "INSERT DATA { <d> <e> <f> }");
  EXPECT_THAT(server.process(insertRequest), StatusIs(http::status::ok));
  expectCounts(DeltaTriplesCount{1, 0});

  auto clearRequest = [](std::optional<std::string> accessToken) {
    auto request = makeGetRequest("/?cmd=clear-delta-triples");
    if (accessToken.has_value()) {
      request.set(http::field::authorization, "Bearer " + accessToken.value());
    }
    return request;
  };

  // The command requires a valid access token.
  AD_EXPECT_THROW_WITH_MESSAGE(
      server.process(clearRequest(std::nullopt)),
      testing::HasSubstr("clear-delta-triples requires a valid access token"));
  expectCounts(DeltaTriplesCount{1, 0});

  // With a valid access token, the delta triples are cleared and the response
  // reports the (now empty) resulting counts.
  auto response = server.process(clearRequest("accessToken"));
  EXPECT_THAT(response, StatusIs(http::status::ok));
  EXPECT_THAT(responseBodyAsJson(std::move(response)),
              testing::Optional(testing::Eq(
                  json{{"inserted", 0}, {"deleted", 0}, {"total", 0}})));
  expectCounts(DeltaTriplesCount{0, 0});
}

// _____________________________________________________________________________
TEST(ServerTest, vacuumDeltaTriples) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> ."});
  auto server = makeServerForTesting(qec->getIndex().getOnDiskBase());

  auto expectCounts = [&server](DeltaTriplesCount expected) {
    EXPECT_THAT(server.deltaTriplesManager()
                    .getCurrentLocatedTriplesSharedState()
                    ->counts_,
                testing::Optional(testing::Eq(expected)));
  };

  // Without this, the single block of the (tiny) test index doesn't meet the
  // minimum size for `vacuum` to process it.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::vacuumMinimumBlockSize_>(
          size_t{0});

  // Insert a triple that is already in the index; this is a redundant
  // insertion that `vacuum` should remove.
  auto insertRequest =
      makeRequest(http::verb::post, "/",
                  {{http::field::content_type, "application/sparql-update"},
                   {http::field::authorization, "Bearer accessToken"}},
                  "INSERT DATA { <a> <b> <c> }");
  EXPECT_THAT(server.process(insertRequest), StatusIs(http::status::ok));
  expectCounts(DeltaTriplesCount{1, 0});

  auto vacuumRequest = [](std::optional<std::string> accessToken) {
    auto request = makeGetRequest("/?cmd=vacuum-delta-triples");
    if (accessToken.has_value()) {
      request.set(http::field::authorization, "Bearer " + accessToken.value());
    }
    return request;
  };

  // The command requires a valid access token.
  AD_EXPECT_THROW_WITH_MESSAGE(
      server.process(vacuumRequest(std::nullopt)),
      testing::HasSubstr("vacuum-delta-triples requires a valid access token"));
  expectCounts(DeltaTriplesCount{1, 0});

  // With a valid access token, the redundant insertion is vacuumed away and
  // the response reports the resulting stats.
  auto response = server.process(vacuumRequest("accessToken"));
  EXPECT_THAT(response, StatusIs(http::status::ok));
  auto body = responseBodyAsJson(std::move(response));
  ASSERT_TRUE(body.has_value());
  EXPECT_EQ(body.value()["external"]["insertionsRemoved"], 1);
  expectCounts(DeltaTriplesCount{0, 0});
}

// _____________________________________________________________________________
TEST(ServerTest, gspHead) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> . <a> <b> <d> ."});
  // Each request runs on a fresh server, so that the sub-tests are
  // independent of each other.
  auto testHead = [&qec](
                      const std::optional<std::string>& accept,
                      ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto head = makeRequest(http::verb::head, "/?default");
    if (accept.has_value()) {
      head.set(http::field::accept, accept.value());
    }
    auto response =
        makeServerForTesting(qec->getIndex().getOnDiskBase()).process(head);
    EXPECT_THAT(response, ContentTypeIs(accept.value_or("text/turtle")));
    EXPECT_THAT(responseBodyToString(std::move(response.body())),
                testing::IsEmpty());
  };
  testHead(std::nullopt);
  testHead("text/csv");
  testHead("text/tab-separated-values");
  testHead("text/turtle");
  testHead("application/qlever-results+json");
}

// _____________________________________________________________________________
TEST(ServerTest, gspGet) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> . <a> <b> <d> ."});
  // Each request runs on a fresh server, so that the sub-tests are
  // independent of each other.
  auto testGet = [&qec](
                     const std::optional<std::string>& accept,
                     const testing::Matcher<const std::string&>& bodyMatcher,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto get = makeGetRequest("/?default");
    if (accept.has_value()) {
      get.set(http::field::accept, accept.value());
    }
    auto response =
        makeServerForTesting(qec->getIndex().getOnDiskBase()).process(get);
    EXPECT_THAT(response, ContentTypeIs(accept.value_or("text/turtle")));
    EXPECT_THAT(responseBodyToString(std::move(response.body())), bodyMatcher);
  };
  testGet(std::nullopt, testing::Eq("<a> <b> <c> .\n<a> <b> <d> .\n"));
  testGet("text/csv", testing::Eq("<a>,<b>,<c>\n<a>,<b>,<d>\n"));
  testGet("text/tab-separated-values",
          testing::Eq("<a>\t<b>\t<c>\n<a>\t<b>\t<d>\n"));
  testGet("text/turtle", testing::Eq("<a> <b> <c> .\n<a> <b> <d> .\n"));
}

// _____________________________________________________________________________
TEST(ServerTest, gspPut) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> . <a> <b> <d> ."});
  // Each request runs on a fresh server, so that the sub-tests are
  // independent of each other.
  auto testPut = [&qec](
                     const std::string& contentType, const std::string& body,
                     const std::string& graph, const auto& bodyMatcher,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto request =
        makeRequest(http::verb::put, "/?" + graph,
                    {{http::field::authorization, "Bearer accessToken"}}, body);
    request.set(http::field::content_type, contentType);
    auto response =
        makeServerForTesting(qec->getIndex().getOnDiskBase()).process(request);
    EXPECT_THAT(response, bodyMatcher);
  };
  testPut("text/turtle", "<a> <b> <c> .", "default",
          StatusIs(http::status::ok));
  testPut("text/turtle", "<a> <b> <c> .", "graph=foo",
          StatusIs(http::status::created));
}

// _____________________________________________________________________________
TEST(ServerTest, gspDelete) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> . <a> <b> <d> ."});
  // Each request runs on a fresh server, so that the sub-tests are
  // independent of each other.
  auto testDelete = [&qec](const std::string& graph, const auto& bodyMatcher,
                           ad_utility::source_location l =
                               AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto request =
        makeRequest(http::verb::delete_, "/?" + graph,
                    {{http::field::authorization, "Bearer accessToken"}});
    auto response =
        makeServerForTesting(qec->getIndex().getOnDiskBase()).process(request);
    EXPECT_THAT(response, bodyMatcher);
  };
  testDelete("default", StatusIs(http::status::ok));
  testDelete("graph=foo", StatusIs(http::status::not_found));
}

MATCHER(PairwiseUnequal, "contains no duplicate elements") {
  const auto& container = arg;
  auto begin = std::begin(container);
  auto end = std::end(container);

  for (auto it = begin; it != end; ++it) {
    for (auto jt = std::next(it); jt != end; ++jt) {
      if (*it == *jt) {
        *result_listener << "duplicate value found: "
                         << ::testing::PrintToString(*it);
        return false;
      }
    }
  }
  return true;
}

// _____________________________________________________________________________
TEST(ServerTest, gspPost) {
  // TODO<qup42> test more thoroughly including the exact delta triples state
  auto baseName = gtestCurrentTestName();
  makeTestIndex(baseName, "");
  auto serverForTesting = makeServerForTesting(baseName);
  auto expectPost = [&serverForTesting](std::string body,
                                        const auto& responseMatcher) {
    auto request =
        makeRequest(http::verb::post, "/?graph=foo",
                    {{http::field::authorization, "Bearer accessToken"},
                     {http::field::host, "example.org"},
                     {http::field::content_type, "text/turtle"}},
                    std::move(body));
    auto response = serverForTesting.process(request);
    EXPECT_THAT(response, responseMatcher);
  };
  auto NumDeltaTriples = [](const auto& matcher) {
    return testing::ResultOf(
        [](const ServerForTesting& server) {
          return server.deltaTriplesManager()
              .getCurrentLocatedTriplesSharedState()
              ->getLocatedTriplesForPermutation<false>(Permutation::PSO)
              .numTriplesForTesting();
        },
        matcher);
  };
  expectPost("", StatusIs(http::status::no_content));
  EXPECT_THAT(serverForTesting, NumDeltaTriples(testing::Eq(0)));
  expectPost("<a> <b> <c> .", StatusIs(http::status::ok));
  EXPECT_THAT(serverForTesting, NumDeltaTriples(testing::Eq(1)));
}

// _____________________________________________________________________________
TEST(ServerTest, gspPostCreateNewGraph) {
  auto testPost = [](ServerForTesting serverForTesting, auto request,
                     const auto& bodyMatcher,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto response = serverForTesting.process(request);
    EXPECT_THAT(response, bodyMatcher);
    return response;
  };

  // Insert a payload into a new graph chosen by the server 1000 times. Check
  // that the returned graph has the expected format and that no graph is
  // selected twice.
  {
    std::string basename = "ServerTest_gspPostCreateNewGraph";
    auto index = makeTestIndex(basename, "");
    auto testPostCreateNewGraph =
        [&testPost, &basename](const std::string& body, const auto& bodyMatcher,
                               ad_utility::source_location l =
                                   AD_CURRENT_SOURCE_LOC()) -> std::string {
      auto request =
          makeRequest(http::verb::post,
                      "/?graph=http%3A%2F%2Fexample.org%2Fhttp-graph-store",
                      {{http::field::authorization, "Bearer accessToken"},
                       {http::field::host, "example.org"},
                       {http::field::content_type, "text/turtle"}},
                      body);
      auto response =
          testPost(makeServerForTesting(basename), request, bodyMatcher, l);
      return response.at(http::field::location);
    };
    std::vector<std::string> locations;
    for (int i = 0; i < 10; ++i) {
      auto location = testPostCreateNewGraph(
          "<a> <b> <c>",
          testing::AllOf(
              // Check that the random part of the graph is a V4 UUID.
              LocationIs(MatchesRegex(
                  R"(http://qlever\.cs\.uni-freiburg\.de/builtin-functions/graph/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12})")),
              StatusIs(http::status::created)));
      locations.push_back(location);
    }
    EXPECT_THAT(locations, PairwiseUnequal());
  }

  // Same behavior via Direct Graph Identification: POST `/http-graph-store`
  // with no `?graph=`. The graph IRI is constructed from `Host` + path and
  // matches the instance's graph store URL, so a new graph is created.
  testPost(
      makeServerForTesting("ServerTest_gspPostCreateNewGraph"),
      makeRequest(http::verb::post, "/http-graph-store",
                  {{http::field::authorization, "Bearer accessToken"},
                   {http::field::host, "example.org"},
                   {http::field::content_type, "text/turtle"}},
                  "<a> <b> <c>"),
      testing::AllOf(
          LocationIs(MatchesRegex(
              R"(http://qlever\.cs\.uni-freiburg\.de/builtin-functions/graph/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12})")),
          StatusIs(http::status::created)));

  auto IsPostNoCreatedGraph = [](http::status status) {
    return testing::AllOf(StatusIs(status),
                          testing::Not(HasHeader(http::field::location)));
  };
  // Here we only care that logic for creating a new graph doesn't fire. The
  // updated triples are not the primary concern here.
  testPost(makeServerForTesting("ServerTest_gspPostCreateNewGraph"),
           makeRequest(http::verb::post, "/?default",
                       {{http::field::authorization, "Bearer accessToken"},
                        {http::field::host, "example.org"},
                        {http::field::content_type, "text/turtle"}},
                       "<a> <b> <c>"),
           IsPostNoCreatedGraph(http::status::ok));
  testPost(makeServerForTesting("ServerTest_gspPostCreateNewGraph"),
           makeRequest(http::verb::post, "/?graph=foo",
                       {{http::field::authorization, "Bearer accessToken"},
                        {http::field::host, "example.org"},
                        {http::field::content_type, "text/turtle"}},
                       "<a> <b> <c>"),
           IsPostNoCreatedGraph(http::status::ok));
}

// _____________________________________________________________________________
// Read a query-event-log file and parse each JSONL line.
namespace {
std::vector<json> parseEventLog(const ql::filesystem::path& path) {
  std::vector<json> events;
  for (const auto& line : ad_utility::testing::readLines(path)) {
    events.push_back(json::parse(line));
  }
  return events;
}
}  // namespace

// _____________________________________________________________________________
// A successful query writes a `start` event carrying the X-Real-IP client IP
// and an `end` event with status "ok".
TEST(ServerTest, queryEventLogRecordsOkAndClientIp) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> . <a> <b> <d> ."});
  auto base = qec->getIndex().getOnDiskBase();
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    auto serverForTesting = makeServerForTesting(base, path);

    auto request = makePostRequest("/", "application/sparql-query",
                                   "SELECT * WHERE { ?a ?b ?c }");
    request.set("X-Real-IP", "10.0.0.5");
    request.set(http::field::accept, "application/sparql-results+json");
    EXPECT_THAT(serverForTesting.process(request), StatusIs(http::status::ok));
  }  // server (hence log) destroyed → queue drained, file closed

  // The TUI byte-slices the timestamp, so every line must begin with
  // `{"ts-ms":`.
  for (const auto& line : ad_utility::testing::readLines(path)) {
    EXPECT_THAT(line, ::testing::StartsWith("{\"ts-ms\":"));
  }

  auto events = parseEventLog(path);
  ASSERT_EQ(events.size(), 2u);
  const auto& start = events.front();
  const auto& end = events.back();

  EXPECT_EQ(start.at("event").get<std::string>(), "start");
  EXPECT_GT(start.at("ts-ms").get<int64_t>(), 0);
  EXPECT_FALSE(start.at("qid").get<std::string>().empty());
  EXPECT_EQ(start.at("client-ip").get<std::string>(), "10.0.0.5");
  EXPECT_EQ(start.at("query").get<std::string>(),
            "SELECT * WHERE { ?a ?b ?c }");

  EXPECT_EQ(end.at("event").get<std::string>(), "end");
  EXPECT_EQ(end.at("status").get<std::string>(), "ok");
  // One end per start: same qid, end not before start.
  EXPECT_EQ(end.at("qid").get<std::string>(),
            start.at("qid").get<std::string>());
  EXPECT_GE(end.at("ts-ms").get<int64_t>(), start.at("ts-ms").get<int64_t>());
}

// _____________________________________________________________________________
// A query that fails during planning writes an `end` event with status
// "failed". It parses (so `start` is written), then planning throws.
TEST(ServerTest, queryEventLogRecordsFailedStatus) {
  auto qec = getQec(TestIndexConfig{"<a> <b> <c> . <a> <b> <d> ."});
  auto base = qec->getIndex().getOnDiskBase();
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  {
    auto serverForTesting = makeServerForTesting(base, path);

    auto request = makePostRequest(
        "/", "application/sparql-query",
        "SELECT * WHERE { ?text ql:contains-entity ?scientist }");
    serverForTesting.process(request);
  }

  auto events = parseEventLog(path);
  ASSERT_EQ(events.size(), 2u);
  const auto& start = events.front();
  const auto& end = events.back();

  EXPECT_EQ(start.at("event").get<std::string>(), "start");
  EXPECT_EQ(start.at("query").get<std::string>(),
            "SELECT * WHERE { ?text ql:contains-entity ?scientist }");

  EXPECT_EQ(end.at("event").get<std::string>(), "end");
  EXPECT_EQ(end.at("status").get<std::string>(), "failed");
  // One end per start: same qid.
  EXPECT_EQ(end.at("qid").get<std::string>(),
            start.at("qid").get<std::string>());
}
