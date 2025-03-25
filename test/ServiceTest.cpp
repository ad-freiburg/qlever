//  Copyright 2022 - 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <ctre-unicode.hpp>
#include <exception>
#include <regex>

#include "engine/Service.h"
#include "engine/Sort.h"
#include "engine/Values.h"
#include "global/Constants.h"
#include "global/IndexTypes.h"
#include "global/RuntimeParameters.h"
#include "gmock/gmock.h"
#include "parser/GraphPatternOperation.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/TripleComponentTestHelpers.h"
#include "util/http/HttpUtils.h"

// Fixture that sets up a test index and a factory for producing mocks for the
// `getResultFunction` needed by the `Service` operation.
class ServiceTest : public ::testing::Test {
 protected:
  // Query execution context (with small test index) and allocator for testing,
  // see `IndexTestHelpers.h`. Note that `getQec` returns a pointer to a static
  // `QueryExecutionContext`, so no need to ever delete `testQec`.
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ad_utility::AllocatorWithLimit<Id> testAllocator =
      ad_utility::testing::makeAllocator();

  // Factory for generating mocks of the `NetworkFunctions` that are used by
  // default by a `Service` operation (see the constructor in `Service.h`).
  // The `getWebSocketClient` function is not tested here and its mock just
  // returns a WebSocketClient without actually starting it.
  // Each mock of the `sendHttpOrHttpsRequest` function does the following:
  //
  // 1. It tests that the request method is POST, the content-type header is
  //    `application/sparql-query`, and the accept header is
  //    `application/sparql-results+json` (our `Service` always does this).
  //
  // 2. It tests that the host and port are as expected.
  //
  // 3. It tests that the post data is as expected.
  //
  // 4. It returns the specified JSON.
  //
  // NOTE: In a previous version of this test, we set up an actual test server.
  // The code can be found in the history of this PR.
  static auto constexpr getNetworkFunctionsFactory =
      [](std::string_view expectedUrl, std::string_view expectedSparqlQuery,
         std::string predefinedResult,
         boost::beast::http::status status = boost::beast::http::status::ok,
         std::string contentType = "application/sparql-results+json",
         std::exception_ptr mockException =
             nullptr) -> Service::NetworkFunctions {
    return {
        .getResultFunction_ =
            [=](const ad_utility::httpUtils::Url& url,
                ad_utility::SharedCancellationHandle,
                const boost::beast::http::verb& method,
                std::string_view postData, std::string_view contentTypeHeader,
                std::string_view acceptHeader,
                const std::vector<std::pair<std::string, std::string>>&) {
              // Check that the request parameters are as expected.
              //
              // NOTE: The first three are hard-coded in
              // `Service::computeResult`, but the host and port of the endpoint
              // are derived from the IRI, so the last two checks are
              // non-trivial.
              EXPECT_EQ(method, boost::beast::http::verb::post);
              EXPECT_EQ(contentTypeHeader, "application/sparql-query");
              EXPECT_EQ(acceptHeader, "application/sparql-results+json");
              EXPECT_EQ(url.asString(), expectedUrl);

              // Check that the whitespace-normalized POST data is the expected
              // query.
              //
              // NOTE: a SERVICE clause specifies only the body of a SPARQL
              // query, from which `Service::computeResult` has to construct a
              // full SPARQL query by adding `SELECT ... WHERE`, so this checks
              // something non-trivial.
              std::string whitespaceNormalizedPostData = std::regex_replace(
                  std::string{postData}, std::regex{"\\s+"}, " ");
              EXPECT_EQ(whitespaceNormalizedPostData, expectedSparqlQuery);

              if (mockException) {
                std::rethrow_exception(mockException);
              }

              auto body = [](std::string result)
                  -> cppcoro::generator<std::span<std::byte>> {
                // Randomly slice the string to make tests more robust.
                std::mt19937 rng{std::random_device{}()};

                const std::string resultStr = result;
                std::uniform_int_distribution<size_t> distribution{
                    0, resultStr.length() / 2};

                for (size_t start = 0; start < resultStr.length();) {
                  size_t size = distribution(rng);
                  std::string resultCopy{resultStr.substr(start, size)};
                  co_yield std::as_writable_bytes(std::span{resultCopy});
                  start += size;
                }
              };
              return (HttpOrHttpsResponse){.status_ = status,
                                           .contentType_ = contentType,
                                           .body_ = body(predefinedResult)};
            },
        .getRuntimeInfoClient_ =
            [](const ad_utility::httpUtils::Url&, const std::string&,
               std::function<void(const std::string&)>) {
              return ad_utility::websocket::WebSocketClientVariant();
            }};
  };

  // The following method generates a JSON result from variables and rows for
  // Testing.
  // Passing more values per row than variables are given isn't supported.
  // Generates all cells with the given values and type uri.
  static std::string genJsonResult(
      std::vector<std::string_view> vars,
      std::vector<std::vector<std::string_view>> rows) {
    nlohmann::json res;
    res["head"]["vars"] = vars;
    res["results"]["bindings"] = nlohmann::json::array();

    for (size_t i = 0; i < rows.size(); ++i) {
      nlohmann::json binding;
      for (size_t j = 0; j < std::min(rows[i].size(), vars.size()); ++j) {
        binding[vars[j]] = {{"type", "uri"}, {"value", rows[i][j]}};
      }
      res["results"]["bindings"].push_back(binding);
    }
    return res.dump();
  }

  static Service::SiblingInfo siblingInfoFromOp(std::shared_ptr<Operation> op) {
    return Service::SiblingInfo{op->getResult(),
                                op->getExternallyVisibleVariableColumns(),
                                op->getCacheKey()};
  };
};

// Test basic methods of class `Service`.
TEST_F(ServiceTest, basicMethods) {
  // Construct a parsed SERVICE clause by hand. The fourth argument is the query
  // body (empty in this case because this test is not about evaluating a
  // query). The fourth argument plays no role in our test (and isn't really
  // used in `parsedQuery::Service` either).
  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
      "PREFIX doof: <http://doof.org>",
      "{ }",
      false};
  // Create an operation from this.
  Service serviceOp{testQec, parsedServiceClause};

  // Test the basic methods.
  ASSERT_EQ(serviceOp.getDescriptor(),
            "Service with IRI <http://localhorst/api>");
  ASSERT_TRUE(serviceOp.getCacheKey().starts_with("SERVICE "))
      << serviceOp.getCacheKey();
  ASSERT_EQ(serviceOp.getResultWidth(), 2);
  ASSERT_EQ(serviceOp.getMultiplicity(0), 1);
  ASSERT_EQ(serviceOp.getMultiplicity(1), 1);
  ASSERT_EQ(serviceOp.getSizeEstimate(), 100'000);
  ASSERT_EQ(serviceOp.getCostEstimate(), 1'000'000);
  using V = Variable;
  ASSERT_THAT(serviceOp.computeVariableToColumnMap(),
              ::testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {V{"?x"}, makePossiblyUndefinedColumn(0)},
                  {V{"?y"}, makePossiblyUndefinedColumn(1)}}));
  ASSERT_FALSE(serviceOp.knownEmptyResult());
  ASSERT_TRUE(serviceOp.getChildren().empty());
}

// Tests that `computeResult` behaves as expected.
TEST_F(ServiceTest, computeResult) {
  // These tests are randomized, and there used to be an error that was found by
  // these random tests (but not always). Run the tests 10 times, this is a good
  // compromise between reasonable runtimes of the tests and a reasonable test
  // coverage.
  for (size_t i = 0; i < 10; ++i) {
    // Construct a parsed SERVICE clause by hand, see `basicMethods` test above.
    parsedQuery::Service parsedServiceClause{
        {Variable{"?x"}, Variable{"?y"}},
        TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
        "PREFIX doof: <http://doof.org>",
        "{ }",
        false};
    parsedQuery::Service parsedServiceClauseSilent{
        {Variable{"?x"}, Variable{"?y"}},
        TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
        "PREFIX doof: <http://doof.org>",
        "{ }",
        true};

    // This is the (port-normalized) URL and (whitespace-normalized) SPARQL
    // query we expect.
    std::string_view expectedUrl = "http://localhorst:80/api";
    std::string_view expectedSparqlQuery =
        "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }";

    // Shorthand to run computeResult with the test parameters given above.
    auto runComputeResult =
        [&](const std::string& result,
            boost::beast::http::status status = boost::beast::http::status::ok,
            std::string contentType = "application/sparql-results+json",
            bool silent = false) -> Result {
      Service s{testQec,
                silent ? parsedServiceClauseSilent : parsedServiceClause,
                getNetworkFunctionsFactory(expectedUrl, expectedSparqlQuery,
                                           result, status, contentType)};
      return s.computeResultOnlyForTesting();
    };

    // Compute the Result lazily for the given Service and check that the
    // resulting IdTable equals the expected IdTable-vector.
    auto checkLazyResult =
        [](Service& service,
           const std::vector<std::vector<std::string>>& expIdTableVector) {
          auto result = service.computeResultOnlyForTesting(true);

          // compute resulting idTable
          IdTable idTable{2, ad_utility::testing::makeAllocator()};
          std::vector<LocalVocab> localVocabs{};
          for (auto& pair : result.idTables()) {
            idTable.insertAtEnd(pair.idTable_);
            localVocabs.emplace_back(std::move(pair.localVocab_));
          }

          // create expected idTable
          auto get =
              [&localVocabs](
                  const std::string& s) -> std::optional<LocalVocabIndex> {
            for (const LocalVocab& localVocab : localVocabs) {
              auto index = localVocab.getIndexOrNullopt(
                  ad_utility::triple_component::LiteralOrIri::iriref(s));
              if (index.has_value()) {
                return index;
              }
            }
            return std::nullopt;
          };
          std::vector<std::vector<IntOrId>> idVector;
          std::map<std::string, Id> ids;
          size_t indexCounter = 0;
          for (auto& row : expIdTableVector) {
            auto& idVecRow = idVector.emplace_back();
            for (auto& e : row) {
              if (!ids.contains(e)) {
                auto str = absl::StrCat("<", e, ">");
                auto idx = get(str);
                ASSERT_TRUE(idx) << '\'' << str << "' not in local vocab";
                ids.insert({e, Id::makeFromLocalVocabIndex(idx.value())});
                ++indexCounter;
              }
              idVecRow.emplace_back(ids.at(e));
            }
          }
          EXPECT_EQ(indexCounter, ids.size());

          EXPECT_EQ(idTable, makeIdTableFromVector(idVector));
        };

    // Checks that a given result throws a specific error message, however when
    // the `SILENT` keyword is set it will be caught.
    auto expectThrowOrSilence =
        [&](const std::string& result, std::string_view errorMsg,
            boost::beast::http::status status = boost::beast::http::status::ok,
            std::string contentType = "application/sparql-results+json") {
          AD_EXPECT_THROW_WITH_MESSAGE(
              runComputeResult(result, status, contentType, false),
              ::testing::HasSubstr(errorMsg));
          EXPECT_NO_THROW(runComputeResult(result, status, contentType, true));
        };

    // CHECK 1: An exception shall be thrown (and maybe silenced), when
    // status-code isn't ok
    expectThrowOrSilence(
        genJsonResult({"x", "y"}, {{"bla", "bli"}, {"blu"}, {"bli", "blu"}}),
        "SERVICE responded with HTTP status code: 400, Bad Request.",
        boost::beast::http::status::bad_request,
        "application/sparql-results+json");
    // contentType doesn't match
    expectThrowOrSilence(
        genJsonResult({"x", "y"}, {{"bla", "bli"}, {"blu"}, {"bli", "blu"}}),
        "QLever requires the endpoint of a SERVICE to send "
        "the result as 'application/sparql-results+json' but "
        "the endpoint sent 'wrong/type'.",
        boost::beast::http::status::ok, "wrong/type");

    // or Result has invalid structure
    // `results` missing
    expectThrowOrSilence("{\"head\": {\"vars\": [\"x\", \"y\"]}}",
                         "results section missing");
    expectThrowOrSilence("", "results section missing");
    // `bindings` missing
    expectThrowOrSilence(
        "{\"head\": {\"vars\": [\"x\", \"y\"]},"
        "\"results\": {}}",
        "results section missing");
    // wrong `bindings` type (array expected)
    expectThrowOrSilence(
        "{\"head\": {\"vars\": [\"x\", \"y\"]},"
        "\"results\": {\"bindings\": {}}}",
        "results section missing");

    // `head`/`vars` missing
    expectThrowOrSilence(
        "{\"results\": {\"bindings\": [{\"x\": {\"type\": \"uri\", \"value\": "
        "\"a\"}, \"y\": {\"type\": \"uri\", \"value\": \"b\"}}]}}",
        "head section missing");
    expectThrowOrSilence(
        "{\"head\": {},"
        "\"results\": {\"bindings\": []}}",
        "\"head\" section is not according to the SPARQL standard.");
    // wrong variables type (array of strings expected)
    expectThrowOrSilence(
        "{\"head\": {\"vars\": [\"x\", \"y\", 3]},"
        "\"results\": {\"bindings\": []}}",
        "\"head\" section is not according to the SPARQL standard.");

    // Internal parser errors.
    expectThrowOrSilence(
        std::string(1'000'000, '0'),
        "QLever currently doesn't support SERVICE results where a single "
        "result row is larger than 1MB");

    // CHECK 1b: Even if the SILENT-keyword is set, throw local errors.
    Service serviceSilent{
        testQec, parsedServiceClauseSilent,
        getNetworkFunctionsFactory(
            expectedUrl, expectedSparqlQuery, "{}",
            boost::beast::http::status::ok, "application/sparql-results+json",
            std::make_exception_ptr(
                ad_utility::CancellationException("Mock Cancellation")))};

    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        serviceSilent.computeResultOnlyForTesting(),
        ::testing::HasSubstr("Mock Cancellation"),
        ad_utility::CancellationException);

    Service serviceSilent2{
        testQec, parsedServiceClauseSilent,
        getNetworkFunctionsFactory(
            expectedUrl, expectedSparqlQuery, "{}",
            boost::beast::http::status::ok, "application/sparql-results+json",
            std::make_exception_ptr(
                ad_utility::detail::AllocationExceedsLimitException(2_B,
                                                                    1_B)))};

    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        serviceSilent2.computeResultOnlyForTesting(),
        ::testing::HasSubstr("Tried to allocate"),
        ad_utility::detail::AllocationExceedsLimitException);

    // CHECK 1c: Accept the content-type regardless of it's case or additional
    // parameters.
    EXPECT_NO_THROW(runComputeResult(
        genJsonResult({"x", "y"},
                      {{"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}),
        boost::beast::http::status::ok,
        "APPLICATION/SPARQL-RESULTS+JSON;charset=utf-8"));

    // CHECK 2: Header row of returned JSON is wrong (missing expected
    // variables)
    // -> an exception should be thrown.
    expectThrowOrSilence(genJsonResult({"x"}, {{"bla"}, {"blu"}, {"bli"}}),
                         "Header row of JSON result for SERVICE query is "
                         "\"?x\", but expected \"?x ?y\".");

    // CHECK 3: A result row of the returned JSON is missing a variable's
    // value -> undefined value
    auto result3 = runComputeResult(
        genJsonResult({"x", "y"}, {{"bla", "bli"}, {"blu"}, {"bli", "blu"}}));
    EXPECT_TRUE(result3.idTable().at(1, 1).isUndefined());

    testQec->clearCacheUnpinnedOnly();

    // CHECK 4: Returned JSON has correct format matching the query -> check
    // that the result table returned by the operation corresponds to the
    // contents of the JSON and its local vocabulary are correct.
    auto result = runComputeResult(genJsonResult(
        {"x", "y"},
        {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}));

    // Check that `<x>` and `<y>` were contained in the original vocabulary
    // and that `<bla>`, `<bli>`, `<blu>` were added to the (initially
    // empty) local vocabulary. On the way, obtain their IDs, which we then
    // need below.
    auto getId = ad_utility::testing::makeGetId(testQec->getIndex());
    Id idX = getId("<x>");
    Id idY = getId("<y>");
    const auto& localVocab = result.localVocab();
    EXPECT_EQ(localVocab.size(), 3);
    auto get = [&localVocab](const std::string& s) {
      return localVocab.getIndexOrNullopt(
          ad_utility::triple_component::LiteralOrIri::iriref(s));
    };
    std::optional<LocalVocabIndex> idxBla = get("<bla>");
    std::optional<LocalVocabIndex> idxBli = get("<bli>");
    std::optional<LocalVocabIndex> idxBlu = get("<blu>");
    ASSERT_TRUE(idxBli.has_value());
    ASSERT_TRUE(idxBla.has_value());
    ASSERT_TRUE(idxBlu.has_value());
    Id idBli = Id::makeFromLocalVocabIndex(idxBli.value());
    Id idBla = Id::makeFromLocalVocabIndex(idxBla.value());
    Id idBlu = Id::makeFromLocalVocabIndex(idxBlu.value());

    // Check that the result table corresponds to the contents of the JSON.
    IdTable expectedIdTable = makeIdTableFromVector(
        {{idX, idY}, {idBla, idBli}, {idBlu, idBla}, {idBli, idBlu}});
    EXPECT_EQ(result.idTable(), expectedIdTable);

    // Check 5: When a siblingTree with variables common to the Service
    // Clause is passed, the Service Operation shall use the siblings result
    // to reduce its Query complexity by injecting them as Values Clause

    auto iri = ad_utility::testing::iri;
    using TC = TripleComponent;

    auto sibling = std::make_shared<Values>(
        testQec, (parsedQuery::SparqlValues){
                     {Variable{"?x"}, Variable{"?y"}, Variable{"?z"}},
                     {{TC(iri("<x>")), TC(iri("<y>")), TC(iri("<z>"))},
                      {TC(iri("<x>")), TC(iri("<y>")), TC(iri("<z2>"))},
                      {TC(iri("<blu>")), TC(iri("<bla>")), TC(iri("<blo>"))},
                      // This row will be ignored in the created Values Clause
                      // as it contains a blank node.
                      {TC(Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0))),
                       TC(iri("<bl>")), TC(iri("<ank>"))}}});

    auto parsedServiceClause5 = parsedServiceClause;
    parsedServiceClause5.graphPatternAsString_ =
        "{ ?x <ble> ?y . ?y <is-a> ?z2 . }";
    parsedServiceClause5.visibleVariables_.emplace_back("?z2");

    std::string_view expectedSparqlQuery5 =
        "PREFIX doof: <http://doof.org> SELECT ?x ?y ?z2 "
        "WHERE { VALUES (?x ?y) { (<x> <y>) (<blu> <bla>) } . ?x <ble> ?y "
        ". ?y "
        "<is-a> ?z2 . }";

    Service serviceOperation5{
        testQec, parsedServiceClause5,
        getNetworkFunctionsFactory(
            expectedUrl, expectedSparqlQuery5,
            genJsonResult({"x", "y", "z2"}, {{"x", "y", "y"},
                                             {"bla", "bli", "y"},
                                             {"blu", "bla", "y"},
                                             {"bli", "blu", "y"}}))};

    serviceOperation5.siblingInfo_.emplace(siblingInfoFromOp(sibling));
    EXPECT_NO_THROW(serviceOperation5.computeResultOnlyForTesting());

    // Check 7: Lazy computation
    Service lazyService{
        testQec, parsedServiceClause,
        getNetworkFunctionsFactory(
            expectedUrl, expectedSparqlQuery,
            genJsonResult({"x", "y"},
                          {{"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}),
            boost::beast::http::status::ok, "application/sparql-results+json")};

    checkLazyResult(lazyService,
                    {{"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}});

    // Check 8: LazyJsonParser Error
    Service service8{
        testQec, parsedServiceClause,
        getNetworkFunctionsFactory(
            expectedUrl, expectedSparqlQuery, std::string(1'000'000, '0'),
            boost::beast::http::status::ok, "application/sparql-results+json")};
    AD_EXPECT_THROW_WITH_MESSAGE(
        service8.computeResultOnlyForTesting(),
        ::testing::HasSubstr("Parser failed with error"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        checkLazyResult(service8, {}),
        ::testing::HasSubstr("Parser failed with error"));

    Service service8b{
        testQec, parsedServiceClause,
        getNetworkFunctionsFactory(
            expectedUrl, expectedSparqlQuery,
            R"({"head": {"vars": ["a"]}, "results": {"bindings": [{"a": break}]}})",
            boost::beast::http::status::ok, "application/sparql-results+json")};
    AD_EXPECT_THROW_WITH_MESSAGE(
        service8b.computeResultOnlyForTesting(),
        ::testing::HasSubstr("Parser failed with error"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        checkLazyResult(service8b, {}),
        ::testing::HasSubstr("Parser failed with error"));
  }
}

// _____________________________________________________________________________
TEST_F(ServiceTest, computeResultWrapSubqueriesWithSibling) {
  auto iri = ad_utility::testing::iri;
  using TC = TripleComponent;

  auto sibling = std::make_shared<Values>(
      testQec,
      (parsedQuery::SparqlValues){{Variable{"?a"}}, {{TC(iri("<a>"))}}});

  parsedQuery::Service parsedServiceClause{
      {Variable{"?a"}},
      TripleComponent::Iri::fromIriref("<http://localhost/api>"),
      "",
      "{ SELECT ?obj WHERE { ?a ?b ?c } }",
      false};

  std::string_view expectedSparqlQuery =
      " SELECT ?a WHERE { VALUES (?a) { (<a>) } . { SELECT ?obj WHERE { ?a ?b "
      "?c } } }";

  Service serviceOperation{
      testQec, parsedServiceClause,
      getNetworkFunctionsFactory("http://localhost:80/api", expectedSparqlQuery,
                                 genJsonResult({"a"}, {{"a"}}))};

  serviceOperation.siblingInfo_.emplace(siblingInfoFromOp(sibling));
  EXPECT_NO_THROW(serviceOperation.computeResultOnlyForTesting());
}

TEST_F(ServiceTest, getCacheKey) {
  // Base query to check cache-keys against.
  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
      "PREFIX doof: <http://doof.org>",
      "{ }",
      false};

  Service service1{
      testQec, parsedServiceClause,
      getNetworkFunctionsFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult(
              {"x", "y"},
              {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}))};

  Service service2{
      testQec, parsedServiceClause,
      getNetworkFunctionsFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult(
              {"x", "y"},
              {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}))};

  // Identically constructed services should have have unique cache keys.
  // Because a remote endpoint cannot be considered cached.
  EXPECT_NE(service1.getCacheKey(), service2.getCacheKey());
}

// Test that bindingToTripleComponent behaves as expected.
TEST_F(ServiceTest, bindingToTripleComponent) {
  ad_utility::HashMap<std::string, Id> blankNodeMap;
  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
      "PREFIX doof: <http://doof.org>",
      "{ }",
      false};
  Service service{testQec, parsedServiceClause};
  LocalVocab localVocab{};

  auto bTTC = [&service, &blankNodeMap,
               &localVocab](const nlohmann::json& binding) -> TripleComponent {
    return service.bindingToTripleComponent(binding, blankNodeMap, &localVocab);
  };

  // Missing type or value.
  AD_EXPECT_THROW_WITH_MESSAGE(bTTC({{"type", "literal"}}),
                               ::testing::HasSubstr("Missing type or value"));
  AD_EXPECT_THROW_WITH_MESSAGE(bTTC({{"value", "v"}}),
                               ::testing::HasSubstr("Missing type or value"));

  EXPECT_EQ(
      bTTC({{"type", "literal"}, {"value", "42"}, {"datatype", XSD_INT_TYPE}}),
      42);

  EXPECT_EQ(
      bTTC({{"type", "literal"}, {"value", "Hallo Welt"}, {"xml:lang", "de"}}),
      TripleComponent::Literal::literalWithoutQuotes("Hallo Welt", "@de"));

  EXPECT_EQ(bTTC({{"type", "literal"}, {"value", "Hello World"}}),
            TripleComponent::Literal::literalWithoutQuotes("Hello World"));

  // Test literals with escape characters (there used to be a bug for those)
  EXPECT_EQ(
      bTTC({{"type", "literal"}, {"value", "Hello \\World"}}),
      TripleComponent::Literal::fromEscapedRdfLiteral("\"Hello \\\\World\""));

  EXPECT_EQ(
      bTTC(
          {{"type", "literal"}, {"value", "Hallo \\Welt"}, {"xml:lang", "de"}}),
      TripleComponent::Literal::fromEscapedRdfLiteral("\"Hallo \\\\Welt\"",
                                                      "@de"));

  EXPECT_EQ(bTTC({{"type", "literal"}, {"value", "a\"b\"c"}}),
            TripleComponent::Literal::fromEscapedRdfLiteral("\"a\\\"b\\\"c\""));

  EXPECT_EQ(bTTC({{"type", "uri"}, {"value", "http://doof.org"}}),
            TripleComponent::Iri::fromIrirefWithoutBrackets("http://doof.org"));

  // Blank Nodes.
  EXPECT_EQ(blankNodeMap.size(), 0);

  Id a =
      bTTC({{"type", "bnode"}, {"value", "A"}}).toValueIdIfNotString().value();
  Id b =
      bTTC({{"type", "bnode"}, {"value", "B"}}).toValueIdIfNotString().value();
  EXPECT_EQ(a.getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_EQ(b.getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_NE(a, b);

  EXPECT_EQ(blankNodeMap.size(), 2);

  // This BlankNode exists already, known Id will be used.
  Id a2 =
      bTTC({{"type", "bnode"}, {"value", "A"}}).toValueIdIfNotString().value();
  EXPECT_EQ(a, a2);

  // Invalid type -> throw.
  AD_EXPECT_THROW_WITH_MESSAGE(
      bTTC({{"type", "INVALID_TYPE"}, {"value", "v"}}),
      ::testing::HasSubstr("Type INVALID_TYPE is undefined."));
}

// ____________________________________________________________________________
TEST_F(ServiceTest, idToValueForValuesClause) {
  auto idToVc = Service::idToValueForValuesClause;
  LocalVocab localVocab{};
  auto index = ad_utility::testing::makeIndexWithTestSettings();

  // blanknode -> nullopt
  EXPECT_EQ(idToVc(index, Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0)),
                   localVocab),
            std::nullopt);

  EXPECT_EQ(idToVc(index, Id::makeUndefined(), localVocab), "UNDEF");

  // simple datatypes -> implicit string representation
  EXPECT_EQ(idToVc(index, Id::makeFromInt(42), localVocab), "42");
  EXPECT_EQ(idToVc(index, Id::makeFromDouble(3.14), localVocab), "3.14");
  EXPECT_EQ(idToVc(index, Id::makeFromBool(true), localVocab), "true");

  // Escape Quotes within literals.
  auto str = LocalVocabEntry(
      ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(
          "a\"b\"c"));
  EXPECT_EQ(idToVc(index, Id::makeFromLocalVocabIndex(&str), localVocab),
            "\"a\\\"b\\\"c\"");

  // value with xsd-type
  EXPECT_EQ(
      idToVc(index, Id::makeFromGeoPoint(GeoPoint(70.5, 130.2)), localVocab)
          .value(),
      absl::StrCat("\"POINT(130.200000 70.500000)\"^^<", GEO_WKT_LITERAL, ">"));
}

// ____________________________________________________________________________
TEST_F(ServiceTest, precomputeSiblingResult) {
  auto service = std::make_shared<Service>(
      testQec,
      parsedQuery::Service{
          {Variable{"?x"}, Variable{"?y"}},
          TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
          "PREFIX doof: <http://doof.org>",
          "{ }",
          true},
      getNetworkFunctionsFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json"));

  // auto service2 = std::make_shared<Service>(*service);
  auto service2 = std::make_shared<Service>(
      testQec,
      parsedQuery::Service{
          {Variable{"?x"}, Variable{"?y"}},
          TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
          "PREFIX doof: <http://doof.org>",
          "{ }",
          true},
      getNetworkFunctionsFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json"));

  // Adaptation of the Values class, allowing to compute lazy Results.
  class MockValues : public Values {
   public:
    MockValues(QueryExecutionContext* qec,
               parsedQuery::SparqlValues parsedValues)
        : Values(qec, parsedValues) {}

    Result computeResult([[maybe_unused]] bool requestLaziness) override {
      Result res = Values::computeResult(false);

      if (!requestLaziness) {
        return Result(Result::IdTableVocabPair(res.idTable().clone(),
                                               res.localVocab().clone()),
                      res.sortedBy());
      }

      // yield each row individually
      return {[&](IdTable clone) -> Result::Generator {
                IdTable idt{clone.numColumns(),
                            ad_utility::makeUnlimitedAllocator<IdTable>()};
                for (size_t i = 0; i < clone.size(); ++i) {
                  idt.push_back(clone[i]);
                  Result::IdTableVocabPair pair{std::move(idt), LocalVocab{}};
                  co_yield pair;
                  idt.clear();
                }
              }(res.idTable().clone()),
              res.sortedBy()};
    }
  };

  auto iri = ad_utility::testing::iri;
  using TC = TripleComponent;
  auto siblingOperation = std::make_shared<MockValues>(
      testQec, parsedQuery::SparqlValues{{Variable{"?x"}, Variable{"?y"}},
                                         {{TC(iri("<x>")), TC(iri("<y>"))},
                                          {TC(iri("<z>")), TC(iri("<a>"))}}});
  auto sibling = std::make_shared<Sort>(
      testQec, std::make_shared<QueryExecutionTree>(testQec, siblingOperation),
      std::vector<ColumnIndex>{});

  // Reset the computed results, to reuse the mock-operations.
  auto reset = [&]() {
    service->siblingInfo_.reset();
    service->precomputedResultBecauseSiblingOfService().reset();
    service2->precomputedResultBecauseSiblingOfService().reset();
    siblingOperation->precomputedResultBecauseSiblingOfService().reset();
    testQec->clearCacheUnpinnedOnly();
  };

  // Right requested but it is not a Service -> no computation
  Service::precomputeSiblingResult(service, sibling, true, false);
  EXPECT_FALSE(
      siblingOperation->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_FALSE(service->siblingInfo_.has_value());
  EXPECT_FALSE(service->precomputedResultBecauseSiblingOfService().has_value());
  reset();

  // Two Service operations -> no computation
  Service::precomputeSiblingResult(service, service2, false, false);
  EXPECT_FALSE(
      service2->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_FALSE(service->siblingInfo_.has_value());
  EXPECT_FALSE(service->precomputedResultBecauseSiblingOfService().has_value());
  reset();

  // Right requested and two Service operations -> compute
  Service::precomputeSiblingResult(service, service2, true, false);
  EXPECT_TRUE(service->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_TRUE(service2->siblingInfo_.has_value());
  EXPECT_FALSE(
      service2->precomputedResultBecauseSiblingOfService().has_value());
  reset();

  // Right requested and it is a service -> sibling result is computed and
  // shared with service
  Service::precomputeSiblingResult(sibling, service, true, false);
  ASSERT_TRUE(
      siblingOperation->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_TRUE(siblingOperation->precomputedResultBecauseSiblingOfService()
                  .value()
                  ->isFullyMaterialized());
  EXPECT_TRUE(service->siblingInfo_.has_value());
  EXPECT_FALSE(service->precomputedResultBecauseSiblingOfService().has_value());
  reset();

  // Compute (large) sibling -> sibling result is computed
  const auto maxValueRowsDefault =
      RuntimeParameters().get<"service-max-value-rows">();
  RuntimeParameters().set<"service-max-value-rows">(0);
  Service::precomputeSiblingResult(sibling, service, true, false);
  ASSERT_TRUE(
      siblingOperation->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_TRUE(siblingOperation->precomputedResultBecauseSiblingOfService()
                  .value()
                  ->isFullyMaterialized());
  EXPECT_FALSE(service->siblingInfo_.has_value());
  EXPECT_FALSE(service->precomputedResultBecauseSiblingOfService().has_value());
  RuntimeParameters().set<"service-max-value-rows">(maxValueRowsDefault);
  reset();

  // Lazy compute (small) sibling -> sibling result is fully materialized and
  // shared with service
  Service::precomputeSiblingResult(service, sibling, false, true);
  ASSERT_TRUE(
      siblingOperation->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_TRUE(siblingOperation->precomputedResultBecauseSiblingOfService()
                  .value()
                  ->isFullyMaterialized());
  EXPECT_TRUE(service->siblingInfo_.has_value());
  EXPECT_FALSE(service->precomputedResultBecauseSiblingOfService().has_value());
  reset();

  // Lazy compute (large) sibling -> partially materialized result is passed
  // back to sibling
  RuntimeParameters().set<"service-max-value-rows">(0);
  Service::precomputeSiblingResult(service, sibling, false, true);
  ASSERT_TRUE(
      siblingOperation->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_FALSE(siblingOperation->precomputedResultBecauseSiblingOfService()
                   .value()
                   ->isFullyMaterialized());
  EXPECT_FALSE(service->siblingInfo_.has_value());
  EXPECT_FALSE(service->precomputedResultBecauseSiblingOfService().has_value());
  RuntimeParameters().set<"service-max-value-rows">(maxValueRowsDefault);

  // consume the sibling result-generator
  for ([[maybe_unused]] auto& _ :
       siblingOperation->precomputedResultBecauseSiblingOfService()
           .value()
           ->idTables()) {
  }
}

// ____________________________________________________________________________
TEST_F(ServiceTest, clone) {
  Service service{
      testQec,
      parsedQuery::Service{
          {Variable{"?x"}, Variable{"?y"}},
          TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
          "PREFIX doof: <http://doof.org>",
          "{ }",
          true},
      getNetworkFunctionsFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json")};

  auto clone = service.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(service, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), service.getDescriptor());
}

// ____________________________________________________________________________
TEST_F(ServiceTest, ChildRuntimeInformation) {
  Service service{testQec,
                  parsedQuery::Service{{Variable{"?x"}, Variable{"?y"}},
                                       TripleComponent::Iri::fromIriref(
                                           "<http://localhorst/api>"),
                                       "PREFIX doof: <http://doof.org>",
                                       "{ }",
                                       true},
                  {}};

  // 0. Check overridden `getRuntimeInfoChildren()` function.
  auto& childRTI = service.childRuntimeInformation_;
  EXPECT_EQ(childRTI.get(), nullptr);
  EXPECT_TRUE(service.getRuntimeInfoChildren().empty());

  childRTI = std::make_shared<RuntimeInformation>();
  childRTI->totalTime_ = 3ms;

  auto serviceRTI = service.getRuntimeInfoPointer();
  serviceRTI->children_.push_back(childRTI);

  EXPECT_EQ(service.getRuntimeInfoChildren().size(), 1);
  EXPECT_EQ(service.getRuntimeInfoChildren().at(0)->totalTime_,
            childRTI->totalTime_);

  // 1. Handle update of the `childRuntimeInformation_`.
  service.childRuntimeInfoAllowWrite_ = true;
  EXPECT_NO_THROW(service.handleChildRuntimeInfoUpdate("invalid-json"));

  service.handleChildRuntimeInfoUpdate(R"({"description": "child"})");
  EXPECT_EQ(service.childRuntimeInformation_->descriptor_, "child");

  service.childRuntimeInfoAllowWrite_ = false;
  service.handleChildRuntimeInfoUpdate(R"({"description": "child2"})");
  EXPECT_NE(service.childRuntimeInformation_->descriptor_, "child2");
}
