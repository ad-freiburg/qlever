//  Copyright 2022 - 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <ctre-unicode.hpp>
#include <exception>
#include <regex>

#include "./util/LocalVocabTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "engine/MaterializedViews.h"
#include "engine/NamedResultCache.h"
#include "engine/ValuesForTesting.h"
#include "engine/binaryExport/BinaryExport.h"
#include "engine/operation/Join.h"
#include "engine/operation/Minus.h"
#include "engine/operation/OptionalJoin.h"
#include "engine/operation/Service.h"
#include "engine/operation/Sort.h"
#include "engine/operation/StripColumns.h"
#include "engine/operation/Values.h"
#include "global/Constants.h"
#include "global/IndexTypes.h"
#include "global/RuntimeParameters.h"
#include "index/TripleComponentConversions.h"
#include "parser/BlankNodeAdder.h"
#include "parser/GraphPatternOperation.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/ColumnStrippingTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/HttpClientTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/LocalVocabEntryTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"
#include "util/TripleComponentTestHelpers.h"
#include "util/VariableToColumnMapTestHelpers.h"
#include "util/http/HttpUtils.h"

using ad_utility::testing::makeLocalVocab;

using ad_utility::testing::colIndices;
using ad_utility::testing::iriLocalVocabEntry;

// Fixture that sets up a test index and a factory for producing mocks for the
// `getResultFunction` needed by the `Service` operation.
class ServiceTest : public ::testing::Test {
 protected:
  // Disable the binary service export for these tests, as the test mocks
  // expect a plain JSON accept header.
  decltype(setRuntimeParameterForTest<
           &RuntimeParameters::binaryServiceEnabled_>(
      false)) disableBinaryService_ =
      setRuntimeParameterForTest<&RuntimeParameters::binaryServiceEnabled_>(
          false);

  // Query execution context (with small test index) and allocator for testing,
  // see `IndexTestHelpers.h`. Note that `getQec` returns a pointer to a static
  // `QueryExecutionContext`, so no need to ever delete `testQec`.
  QueryExecutionContext* testQec = ad_utility::testing::getQec();

  // Factory for generating mocks of the `sendHttpOrHttpsRequest` function that
  // is used by default by a `Service` operation (see the constructor in
  // `Service.h`). Each mock does the following:
  //
  // 1. It tests that the request method is POST, the content-type header is
  //    `application/sparql-query`, and the accept header is
  //    `text/tab-separated-values` (our `Service` always does this).
  //
  // 2. It tests that the host and port are as expected.
  //
  // 3. It tests that the post data is as expected.
  //
  // 4. It returns the specified JSON.
  //
  // NOTE: In a previous version of this test, we set up an actual test server.
  // The code can be found in the history of this PR.
  static auto constexpr getResultFunctionFactory =
      [](std::string_view expectedUrl, std::string_view expectedSparqlQuery,
         std::string predefinedResult,
         boost::beast::http::status status = boost::beast::http::status::ok,
         std::string contentType = "application/sparql-results+json",
         std::exception_ptr mockException = nullptr,
         ad_utility::source_location loc =
             AD_CURRENT_SOURCE_LOC()) -> SendRequestType {
    // Check that the request parameters are as expected.
    //
    // NOTE: Method, Content-Type and Accept are hard-coded in
    // `Service::computeResult`, but the host and port of the endpoint are
    // derived from the IRI, so url and post data are non-trivial.
    httpClientTestHelpers::RequestMatchers matchers{
        .url_ = testing::Eq(expectedUrl),
        .method_ = testing::Eq(boost::beast::http::verb::post),
        // Check that the whitespace-normalized POST data is the expected query.
        //
        // NOTE: a SERVICE clause specifies only the body of a SPARQL query,
        // from which `Service::computeResult` has to construct a full SPARQL
        // query by adding `SELECT ... WHERE`, so this checks something
        // non-trivial.
        .postData_ = testing::ResultOf(
            [](std::string_view postData) {
              return std::regex_replace(std::string{postData},
                                        std::regex{"\\s+"}, " ");
            },
            testing::Eq(expectedSparqlQuery)),
        .contentType_ = testing::Eq("application/sparql-query"),
        .accept_ = testing::Eq("application/sparql-results+json")};
    return httpClientTestHelpers::getResultFunctionFactory(
        predefinedResult, contentType, status, matchers, mockException, loc);
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
                                std::string{op->getCacheKey()}};
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
  ASSERT_TRUE(ql::starts_with(serviceOp.getCacheKey(), "SERVICE "))
      << serviceOp.getCacheKey();
  ASSERT_EQ(serviceOp.getResultWidth(), 2);
  ASSERT_EQ(serviceOp.getMultiplicity(0), 1);
  ASSERT_EQ(serviceOp.getMultiplicity(1), 1);
  ASSERT_EQ(serviceOp.getSizeEstimate(), 100'000);
  ASSERT_EQ(serviceOp.getCostEstimate(), 1'000'000);
  using V = Variable;
  ASSERT_THAT(serviceOp.computeVariableToColumnMap(),
              ::testing::UnorderedElementsAreArray(makeVariableToColumnMap(
                  {{V{"?x"}, makePossiblyUndefinedColumn(0)},
                   {V{"?y"}, makePossiblyUndefinedColumn(1)}})));
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
        "PREFIX doof: <http://doof.org> SELECT ?x ?y { }";

    // Shorthand to run computeResult with the test parameters given above.
    auto runComputeResult =
        [&](const std::string& result,
            boost::beast::http::status status = boost::beast::http::status::ok,
            std::string contentType = "application/sparql-results+json",
            bool silent = false, bool requestLaziness = false,
            ad_utility::source_location loc =
                AD_CURRENT_SOURCE_LOC()) -> Result {
      Service s{
          testQec, silent ? parsedServiceClauseSilent : parsedServiceClause,
          getResultFunctionFactory(expectedUrl, expectedSparqlQuery, result,
                                   status, contentType, nullptr, loc)};
      return s.computeResultOnlyForTesting(requestLaziness);
    };

    // Compute the Result lazily for the given Service and check that the
    // resulting IdTable equals the expected IdTable-vector.
    auto checkLazyResult =
        [this](Service& service,
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
          auto get = [this, &localVocabs](
                         std::string_view s) -> std::optional<LocalVocabIndex> {
            for (const LocalVocab& localVocab : localVocabs) {
              auto index = localVocab.getIndexOrNullopt(
                  iriLocalVocabEntry(s, testQec->getLocalVocabContext()));
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
            std::string contentType = "application/sparql-results+json",
            ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
          auto g = generateLocationTrace(loc);
          AD_EXPECT_THROW_WITH_MESSAGE(
              runComputeResult(result, status, contentType, false),
              ::testing::HasSubstr(errorMsg));
          EXPECT_NO_THROW(runComputeResult(result, status, contentType, true));

          // With `SILENT`, a lazily requested result is computed eagerly, so
          // that the error is silenced, too, and yields the neutral element.
          auto silentLazyResult =
              runComputeResult(result, status, contentType, true, true);
          ASSERT_TRUE(silentLazyResult.isFullyMaterialized());
          EXPECT_EQ(silentLazyResult.idTableView(),
                    makeIdTableFromVector(
                        {{Id::makeUndefined(), Id::makeUndefined()}}));

          // In the syntax test mode, all services (so also the failing ones)
          // return the neutral result.
          auto cleanup =
              setRuntimeParameterForTest<&RuntimeParameters::syntaxTestMode_>(
                  true);
          EXPECT_NO_THROW(runComputeResult(result, status, contentType, false));
        };

    // CHECK 1: An exception shall be thrown (and maybe silenced), when
    // status-code isn't ok
    expectThrowOrSilence(
        genJsonResult({"x", "y"}, {{"bla", "bli"}, {"blu"}, {"bli", "blu"}}),
        "SERVICE responded with HTTP status code: 400, Bad Request",
        boost::beast::http::status::bad_request,
        "application/sparql-results+json");
    // contentType doesn't match
    expectThrowOrSilence(
        genJsonResult({"x", "y"}, {{"bla", "bli"}, {"blu"}, {"bli", "blu"}}),
        "QLever requires the endpoint of a SERVICE to send "
        "the result as 'application/sparql-results+json' but "
        "the endpoint sent 'wrong/type'",
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
        "\"head\" section is not according to the SPARQL standard");
    // wrong variables type (array of strings expected)
    expectThrowOrSilence(
        "{\"head\": {\"vars\": [\"x\", \"y\", 3]},"
        "\"results\": {\"bindings\": []}}",
        "\"head\" section is not according to the SPARQL standard");

    // Internal parser errors.
    expectThrowOrSilence(
        std::string(1'000'000, '0'),
        "QLever currently doesn't support SERVICE results where a single "
        "result row is larger than 1MB");

    // CHECK 1b: Even if the SILENT-keyword is set, throw local errors.
    Service serviceSilent{
        testQec, parsedServiceClauseSilent,
        getResultFunctionFactory(
            expectedUrl, expectedSparqlQuery, "{}",
            boost::beast::http::status::ok, "application/sparql-results+json",
            std::make_exception_ptr(ad_utility::CancellationException(
                ad_utility::CancellationState::MANUAL, "Mock Cancellation")))};

    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        serviceSilent.computeResultOnlyForTesting(),
        ::testing::HasSubstr("Mock Cancellation"),
        ad_utility::CancellationException);

    Service serviceSilent2{
        testQec, parsedServiceClauseSilent,
        getResultFunctionFactory(
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
    EXPECT_TRUE(result3.idTableView()(1, 1).isUndefined());

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
    const auto& localVocabContext = testQec->getLocalVocabContext();
    auto get = [&localVocab, &localVocabContext](std::string_view s) {
      return localVocab.getIndexOrNullopt(
          iriLocalVocabEntry(s, localVocabContext));
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
    EXPECT_EQ(result.idTableView(), expectedIdTable);

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
        "{ ?x <ble> ?y . ?y <is-a> ?z2 . VALUES (?x ?y) { (<x> <y>) "
        "(<blu> <bla>) } }";

    Service serviceOperation5{
        testQec, parsedServiceClause5,
        getResultFunctionFactory(
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
        getResultFunctionFactory(
            expectedUrl, expectedSparqlQuery,
            genJsonResult({"x", "y"},
                          {{"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}),
            boost::beast::http::status::ok, "application/sparql-results+json")};

    checkLazyResult(lazyService,
                    {{"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}});

    // Check 8: LazyJsonParser Error
    Service service8{
        testQec, parsedServiceClause,
        getResultFunctionFactory(
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
        getResultFunctionFactory(
            expectedUrl, expectedSparqlQuery,
            R"({"head": {"vars": ["a"]}, "results": {"bindings": [{"a": break}]}})",
            boost::beast::http::status::ok, "application/sparql-results+json")};
    AD_EXPECT_THROW_WITH_MESSAGE(
        service8b.computeResultOnlyForTesting(),
        ::testing::HasSubstr("Parser failed with error"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        checkLazyResult(service8b, {}),
        ::testing::HasSubstr("Parser failed with error"));

    // Check 8c: A truncated result must lead to an error, and not silently to
    // only the rows before the truncation. This is how QLever truncates its
    // result when the computation fails while the result is being sent (for
    // example, because of a timeout).
    const std::string completeResult = genJsonResult(
        {"x", "y"}, {{"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}});
    const std::string truncatedResult = absl::StrCat(
        completeResult.substr(0, completeResult.rfind("blu")),
        "\n !!!!>># An error has occurred while exporting the query result. "
        "The error message was:\nOperation timed out.");
    Service service8c{testQec, parsedServiceClause,
                      getResultFunctionFactory(expectedUrl, expectedSparqlQuery,
                                               truncatedResult)};
    AD_EXPECT_THROW_WITH_MESSAGE(
        service8c.computeResultOnlyForTesting(),
        ::testing::AllOf(::testing::HasSubstr("Parser failed with error"),
                         ::testing::HasSubstr("ended before it was complete"),
                         ::testing::HasSubstr("Operation timed out.'")));
    AD_EXPECT_THROW_WITH_MESSAGE(
        checkLazyResult(service8c, {{"bla", "bli"}, {"blu", "bla"}}),
        ::testing::HasSubstr("ended before it was complete"));
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
      " SELECT ?a { { SELECT ?obj WHERE { ?a ?b ?c } } VALUES (?a) { (<a>) } }";

  Service serviceOperation{
      testQec, parsedServiceClause,
      getResultFunctionFactory("http://localhost:80/api", expectedSparqlQuery,
                               genJsonResult({"a"}, {{"a"}}))};

  serviceOperation.siblingInfo_.emplace(siblingInfoFromOp(sibling));
  EXPECT_NO_THROW(serviceOperation.computeResultOnlyForTesting());
}

// _____________________________________________________________________________
TEST_F(ServiceTest, pushDownValuesPlacesValuesAtEnd) {
  // Normal body: VALUES clause appears after the body content.
  EXPECT_EQ(
      Service::pushDownValues("{ ?x <ble> ?y . }", "VALUES (?x) { (<a>) } "),
      "{\n ?x <ble> ?y . \nVALUES (?x) { (<a>) } \n}");

  // Subquery body: subquery is wrapped in braces and VALUES appears after.
  EXPECT_EQ(Service::pushDownValues("{ SELECT ?a WHERE { ?a ?b ?c } }",
                                    "VALUES (?a) { (<a>) } "),
            "{\n{ SELECT ?a WHERE { ?a ?b ?c } }\nVALUES (?a) { (<a>) } \n}");

  // BIND body: VALUES is placed AFTER the BIND so the remote endpoint does not
  // see the variable as already bound when it evaluates the BIND expression.
  // Reproducer: SELECT * WHERE { VALUES ?x { 1 }
  //             SERVICE <...> { BIND (1 AS ?x) } }
  EXPECT_EQ(
      Service::pushDownValues("{ BIND (1 AS ?x) }", "VALUES (?x) { (1) } "),
      "{\n BIND (1 AS ?x) \nVALUES (?x) { (1) } \n}");
}

// Test that columns of the sibling that contain `UNDEF` are not pushed down,
// as their rows would match overlapping sets of remote rows (see #4010).
TEST_F(ServiceTest, siblingValuesClauseSkipsColumnsWithUndef) {
  auto iri = ad_utility::testing::iri;
  using TC = TripleComponent;
  auto valuesClause = [&](parsedQuery::SparqlValues siblingValues,
                          bool markAllPossiblyUndefined = false) {
    Service service{
        testQec,
        parsedQuery::Service{
            {Variable{"?x"}, Variable{"?y"}},
            TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
            "",
            "{ ?x <p> ?y }",
            false},
        getResultFunctionFactory("http://localhorst:80/api", "", "")};
    auto siblingInfo = siblingInfoFromOp(
        std::make_shared<Values>(testQec, std::move(siblingValues)));
    if (markAllPossiblyUndefined) {
      for (auto& [var, info] : siblingInfo.variables_) {
        info.mightContainUndef_ = ColumnIndexAndTypeInfo::PossiblyUndefined;
      }
    }
    service.siblingInfo_.emplace(std::move(siblingInfo));
    service.runtimeInfo().addDetail("optimized-with-sibling-result", "yes");
    auto result = service.getSiblingValuesClause();
    EXPECT_EQ(service.runtimeInfo().details_["optimized-with-sibling-result"],
              result.has_value() ? "yes" : "no");
    return result;
  };

  // Only `?y` contains `UNDEF`, so only `?x` is pushed down, and the rows that
  // are then duplicates are removed.
  EXPECT_EQ(valuesClause({{Variable{"?x"}, Variable{"?y"}},
                          {{TC(iri("<x>")), TC::UNDEF()},
                           {TC(iri("<x>")), TC(iri("<y>"))},
                           {TC(iri("<x2>")), TC(iri("<y>"))}}}),
            "VALUES (?x) { (<x>) (<x2>) } ");

  // If every common column contains `UNDEF`, no `VALUES` clause is sent.
  EXPECT_EQ(valuesClause({{Variable{"?x"}, Variable{"?z"}},
                          {{TC::UNDEF(), TC(iri("<z>"))},
                           {TC(iri("<x>")), TC(iri("<z>"))}}}),
            std::nullopt);

  // Without `UNDEF`, all common columns are pushed down.
  EXPECT_EQ(valuesClause({{Variable{"?x"}, Variable{"?y"}},
                          {{TC(iri("<x>")), TC(iri("<y>"))}}}),
            "VALUES (?x ?y) { (<x> <y>) } ");

  // A column that is only marked as possibly undefined is still pushed down.
  EXPECT_EQ(valuesClause({{Variable{"?x"}, Variable{"?y"}},
                          {{TC(iri("<x>")), TC(iri("<y>"))}}},
                         true),
            "VALUES (?x ?y) { (<x> <y>) } ");
}

// _____________________________________________________________________________
TEST_F(ServiceTest, computeResultNoVariables) {
  parsedQuery::Service parsedServiceClause{
      {},
      TripleComponent::Iri::fromIriref("<http://localhost/api>"),
      "",
      "{ <a> <b> <c> }",
      false};

  std::string_view expectedSparqlQuery = " SELECT * { <a> <b> <c> }";

  Service serviceOperation{
      testQec, parsedServiceClause,
      getResultFunctionFactory("http://localhost:80/api", expectedSparqlQuery,
                               genJsonResult({}, {{}}))};

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
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult(
              {"x", "y"},
              {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}))};

  Service service2{
      testQec, parsedServiceClause,
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult(
              {"x", "y"},
              {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}))};

  // Identically constructed services should have have unique cache keys.
  // Because a remote endpoint cannot be considered cached.
  EXPECT_NE(service1.getCacheKey(), service2.getCacheKey());
}

// _____________________________________________________________________________
TEST_F(ServiceTest, getCacheKeyWithCaching) {
  using namespace ::testing;
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::cacheServiceResults_>(
          true);
  {
    parsedQuery::Service parsedServiceClause{
        {Variable{"?x"}, Variable{"?y"}},
        TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
        "PREFIX doof: <http://doof.org>",
        "{ }",
        false};

    Service service{
        testQec, parsedServiceClause,
        getResultFunctionFactory(
            "http://localhorst:80/api",
            "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
            genJsonResult(
                {"x", "y"},
                {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}))};

    EXPECT_THAT(service.getCacheKey(),
                AllOf(StartsWith("SERVICE"), Not(HasSubstr("SILENT")),
                      HasSubstr("<http://localhorst/api>"),
                      HasSubstr("PREFIX doof: <http://doof.org>"),
                      HasSubstr("SELECT ?x ?y"),
                      HasSubstr(parsedServiceClause.graphPatternAsString_)));
  }
  {
    parsedQuery::Service parsedServiceClause{
        {Variable{"?x"}, Variable{"?y"}},
        TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
        "PREFIX doof: <http://doof.org>",
        "{ }",
        true};

    Service service{
        testQec, parsedServiceClause,
        getResultFunctionFactory(
            "http://localhorst:80/api",
            "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
            genJsonResult(
                {"x", "y"},
                {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}))};

    EXPECT_THAT(service.getCacheKey(),
                AllOf(StartsWith("SERVICE"), HasSubstr("SILENT"),
                      HasSubstr("<http://localhorst/api>"),
                      HasSubstr("PREFIX doof: <http://doof.org>"),
                      HasSubstr("SELECT ?x ?y"),
                      HasSubstr(parsedServiceClause.graphPatternAsString_)));
  }
  {
    SendRequestType getResultFunction = getResultFunctionFactory(
        "http://localhorst:80/api",
        "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
        genJsonResult(
            {"x", "y"},
            {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}));

    Service service1{
        testQec,
        {{Variable{"?x"}, Variable{"?y"}, Variable{"?z"}},
         TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
         "PREFIX doof: <http://doof.org>",
         "{ }",
         false},
        getResultFunction};

    Service service2{
        testQec,
        {{Variable{"?x"}, Variable{"?y"}},
         TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
         "PREFIX doof: <http://doof.org>",
         "{ }",
         false},
        getResultFunction};

    // Two `Service`s that differ only in their visible variables have
    // different cache keys.
    EXPECT_NE(service1.getCacheKey(), service2.getCacheKey());
  }
  {
    parsedQuery::Service parsedServiceClause{
        {},
        TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
        "PREFIX doof: <http://doof.org>",
        "{ }",
        false};
    Service service{
        testQec, parsedServiceClause,
        getResultFunctionFactory("http://localhorst:80/api", "", "{}")};
    EXPECT_THAT(service.getCacheKey(), HasSubstr("SELECT * "));
  }
}

// Test that bindingToTripleComponent behaves as expected.
TEST_F(ServiceTest, bindingToTripleComponent) {
  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
      "PREFIX doof: <http://doof.org>",
      "{ }",
      false};
  Service service{testQec, parsedServiceClause};
  BlankNodeAdder blankNodeAdder{testQec->getIndex().getBlankNodeManager(),
                                testQec->getAllocator()};

  auto bTTC = [&service, &blankNodeAdder](
                  const nlohmann::json& binding) -> TripleComponent {
    return service.bindingToTripleComponent(binding, blankNodeAdder);
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

  // See the comment in `src/engine/operation/Service.cpp` regarding the support
  // of the deprecated `typed-literal` type.
  EXPECT_EQ(
      bTTC({{"type", "typed-literal"},
            {"value", "Hallo Welt"},
            {"xml:lang", "de"}}),
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
  EXPECT_EQ(blankNodeAdder.map_.size(), 0);

  const EncodedIriManager encodedIriManager;
  Id a = toValueIdIfNotString(bTTC({{"type", "bnode"}, {"value", "A"}}),
                              &encodedIriManager)
             .value();
  Id b = toValueIdIfNotString(bTTC({{"type", "bnode"}, {"value", "B"}}),
                              &encodedIriManager)
             .value();
  EXPECT_EQ(a.getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_EQ(b.getDatatype(), Datatype::BlankNodeIndex);
  EXPECT_NE(a, b);

  EXPECT_EQ(blankNodeAdder.map_.size(), 2);

  // This BlankNode exists already, known Id will be used.
  Id a2 = toValueIdIfNotString(bTTC({{"type", "bnode"}, {"value", "A"}}),
                               &encodedIriManager)
              .value();
  EXPECT_EQ(a, a2);

  // Invalid type -> throw.
  AD_EXPECT_THROW_WITH_MESSAGE(
      bTTC({{"type", "INVALID_TYPE"}, {"value", "v"}}),
      ::testing::HasSubstr("Type INVALID_TYPE is undefined."));
}

// ____________________________________________________________________________
// Regression test: The labels of blank nodes are scoped to the complete result
// set of a SERVICE, but the `LazyJsonParser` splits that result set into one
// part per chunk of the response. The blank nodes used to be resolved per part,
// such that the same label yielded different blank node `Id`s in different
// parts of the same result.
TEST_F(ServiceTest, blankNodesAcrossResponseChunks) {
  // A mock for the `getResultFunction` that yields the response body in exactly
  // the given `chunks`. In contrast to the `getResultFunctionFactory` of the
  // fixture, which slices the body randomly, this gives us control over the
  // parts that the `LazyJsonParser` produces.
  auto getResultFunctionWithChunks =
      [](std::vector<std::string> chunks) -> SendRequestType {
    auto body = [](std::vector<std::string> chunks)
        -> cppcoro::generator<ql::span<std::byte>> {
      for (std::string& chunk : chunks) {
        co_yield ql::as_writable_bytes(ql::span{chunk});
      }
    };
    return [body, chunks = std::move(chunks)](
               const ad_utility::httpUtils::Url&,
               ad_utility::SharedCancellationHandle,
               const boost::beast::http::verb&, std::string_view,
               std::string_view, std::string_view, size_t) {
      return HttpOrHttpsResponse{
          .status_ = boost::beast::http::status::ok,
          .contentType_ = "application/sparql-results+json",
          .location_ = "",
          .body_ = body(chunks)};
    };
  };

  // The blank node `_:b` occurs in both chunks (as the 2nd and the 4th
  // binding), the blank node `_:c` only in the second chunk.
  const std::vector<std::string> chunks{
      R"({"head":{"vars":["x"]},"results":{"bindings":[)"
      R"({"x":{"type":"uri","value":"http://ex.org/1"}},)"
      R"({"x":{"type":"bnode","value":"b"}},)",
      R"({"x":{"type":"bnode","value":"c"}},)"
      R"({"x":{"type":"bnode","value":"b"}}]}})"};

  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}},
      TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
      "",
      "{ ?x <p> <o> }",
      false};

  // Check the four `Id`s of the single column, which are the same for the lazy
  // and the fully materialized result.
  auto expectIdsAreConsistent = [](const std::vector<Id>& ids) {
    ASSERT_THAT(ids, testing::SizeIs(4));
    EXPECT_THAT(ids[0].getDatatype(), testing::Ne(Datatype::BlankNodeIndex));
    for (size_t rowIdx : {1, 2, 3}) {
      EXPECT_THAT(ids[rowIdx].getDatatype(),
                  testing::Eq(Datatype::BlankNodeIndex))
          << "row " << rowIdx;
    }
    // The same label in different chunks yields the same `Id`, different
    // labels yield different `Id`s.
    EXPECT_THAT(ids[3], testing::Eq(ids[1]));
    EXPECT_THAT(ids[2], testing::Ne(ids[1]));
  };

  // The lazy result, where each part becomes its own block.
  std::vector<Id> lazyIds;
  {
    Service service{testQec, parsedServiceClause,
                    getResultFunctionWithChunks(chunks)};
    auto result = service.computeResultOnlyForTesting(true);
    size_t numBlocks = 0;
    for (auto& pair : result.idTables()) {
      ++numBlocks;
      for (size_t rowIdx = 0; rowIdx < pair.idTable_.numRows(); ++rowIdx) {
        Id id = pair.idTable_(rowIdx, 0);
        lazyIds.push_back(id);
        // The `LocalVocab` of a block has to keep all the blank nodes of that
        // block alive, also those that were created for a previous block.
        if (id.getDatatype() == Datatype::BlankNodeIndex) {
          EXPECT_THAT(pair.localVocab_.isBlankNodeIndexContained(
                          id.getBlankNodeIndex()),
                      testing::IsTrue());
        }
      }
    }
    // One block per chunk, otherwise this test wouldn't test anything.
    EXPECT_THAT(numBlocks, testing::Eq(2));
  }
  expectIdsAreConsistent(lazyIds);

  // The fully materialized result, where all the parts end up in a single
  // `IdTable`.
  {
    Service service{testQec, parsedServiceClause,
                    getResultFunctionWithChunks(chunks)};
    auto result = service.computeResultOnlyForTesting();
    const auto& idTable = result.idTableView();
    std::vector<Id> ids;
    for (size_t rowIdx = 0; rowIdx < idTable.numRows(); ++rowIdx) {
      ids.push_back(idTable(rowIdx, 0));
      if (ids.back().getDatatype() == Datatype::BlankNodeIndex) {
        EXPECT_THAT(result.localVocab().isBlankNodeIndexContained(
                        ids.back().getBlankNodeIndex()),
                    testing::IsTrue());
      }
    }
    expectIdsAreConsistent(ids);

    // Blank nodes with the same label, but from a different SERVICE operation
    // are distinct.
    EXPECT_THAT(ids[1], testing::Ne(lazyIds[1]));
  }
}

// ____________________________________________________________________________
TEST_F(ServiceTest, idToValueForValuesClause) {
  auto idToVc = Service::idToValueForValuesClause;
  LocalVocab localVocab = makeLocalVocab();
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

  // Non-finite doubles have no bare SPARQL syntax -> typed literal.
  auto xsdDouble = absl::StrCat("\"^^<", XSD_DOUBLE_TYPE, ">");
  EXPECT_EQ(idToVc(index, Id::makeFromDouble(INFINITY), localVocab),
            absl::StrCat("\"INF", xsdDouble));
  EXPECT_EQ(idToVc(index, Id::makeFromDouble(-INFINITY), localVocab),
            absl::StrCat("\"-INF", xsdDouble));
  EXPECT_EQ(idToVc(index, Id::makeFromDouble(NAN), localVocab),
            absl::StrCat("\"NaN", xsdDouble));

  // Escape Quotes within literals.
  auto getLocalVocabId = ad_utility::testing::makeGetLocalVocabId();
  auto str = LocalVocabEntry::literalWithoutQuotes(
      "a\"b\"c", index.getLocalVocabContext());
  EXPECT_EQ(idToVc(index, getLocalVocabId(str), localVocab), "\"a\\\"b\\\"c\"");

  // value with xsd-type
  EXPECT_EQ(
      idToVc(index, Id::makeFromGeoPoint(GeoPoint(70.5, 130.2)), localVocab)
          .value(),
      absl::StrCat("\"POINT(130.200000 70.500000)\"^^<", GEO_WKT_LITERAL, ">"));
}

// ____________________________________________________________________________
TEST_F(ServiceTest, precomputeSiblingResultDoesNotWorkWithCaching) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::cacheServiceResults_>(
          true);
  auto service = std::make_shared<Service>(
      testQec,
      parsedQuery::Service{
          {Variable{"?x"}, Variable{"?y"}},
          TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
          "PREFIX doof: <http://doof.org>",
          "{ }",
          true},
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json"));

  auto sibling = std::make_shared<AlwaysFailOperation>(testQec, Variable{"?x"});

  EXPECT_NO_THROW(
      Service::precomputeSiblingResult(sibling, service, true, false));
  EXPECT_FALSE(service->siblingInfo_.has_value());
}

// ____________________________________________________________________________
TEST_F(ServiceTest, precomputeSiblingResultDoesNotWorkWithLimit) {
  std::array limitsAndOffsets{
      LimitOffsetClause{1},
      LimitOffsetClause{std::nullopt, 1},
      LimitOffsetClause{1, 1},
  };
  for (const LimitOffsetClause& limitOffset : limitsAndOffsets) {
    auto service = std::make_shared<Service>(
        testQec,
        parsedQuery::Service{
            {Variable{"?x"}, Variable{"?y"}},
            TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
            "PREFIX doof: <http://doof.org>",
            "{ }",
            true},
        getResultFunctionFactory(
            "http://localhorst:80/api",
            "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
            genJsonResult({"x", "y"}, {{"a", "b"}}),
            boost::beast::http::status::ok, "application/sparql-results+json"));

    service->applyLimitOffset(limitOffset, false);

    auto sibling =
        std::make_shared<AlwaysFailOperation>(testQec, Variable{"?x"});

    EXPECT_NO_THROW(
        Service::precomputeSiblingResult(sibling, service, true, false));
    EXPECT_FALSE(service->siblingInfo_.has_value());
  }
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
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json"));

  auto service2 = std::make_shared<Service>(*service);

  // Adaptation of the Values class, allowing to compute lazy Results.
  class MockValues : public Values {
   public:
    MockValues(QueryExecutionContext* qec,
               parsedQuery::SparqlValues parsedValues)
        : Operation{qec}, Values(qec, parsedValues) {}

    Result computeResult([[maybe_unused]] bool requestLaziness) override {
      Result res = Values::computeResult(false);

      if (!requestLaziness) {
        return Result(Result::IdTableVocabPair(res.cloneIdTable(),
                                               res.localVocab().clone()),
                      res.sortedBy());
      }

      // yield each row individually
      return {[&](IdTable clone) -> Result::Generator {
                IdTable idt{clone.numColumns(),
                            ad_utility::makeUnlimitedAllocator<IdTable>()};
                for (size_t i = 0; i < clone.size(); ++i) {
                  idt.push_back(clone[i]);
                  Result::IdTableVocabPair pair{std::move(idt),
                                                makeLocalVocab()};
                  co_yield pair;
                  idt.clear();
                }
              }(res.cloneIdTable()),
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
      colIndices({}));

  // Reset the computed results, to reuse the mock-operations.
  auto reset = [&]() {
    service->siblingInfo_.reset();
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
  EXPECT_TRUE(service2->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_TRUE(service->siblingInfo_.has_value());
  EXPECT_FALSE(service->precomputedResultBecauseSiblingOfService().has_value());
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
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::serviceMaxValueRows_>(0);
    Service::precomputeSiblingResult(sibling, service, true, false);
    ASSERT_TRUE(siblingOperation->precomputedResultBecauseSiblingOfService()
                    .has_value());
    EXPECT_TRUE(siblingOperation->precomputedResultBecauseSiblingOfService()
                    .value()
                    ->isFullyMaterialized());
    EXPECT_FALSE(service->siblingInfo_.has_value());
    EXPECT_FALSE(
        service->precomputedResultBecauseSiblingOfService().has_value());
  }
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
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::serviceMaxValueRows_>(0);
    Service::precomputeSiblingResult(service, sibling, false, true);
    ASSERT_TRUE(siblingOperation->precomputedResultBecauseSiblingOfService()
                    .has_value());
    EXPECT_FALSE(siblingOperation->precomputedResultBecauseSiblingOfService()
                     .value()
                     ->isFullyMaterialized());
    EXPECT_FALSE(service->siblingInfo_.has_value());
    EXPECT_FALSE(
        service->precomputedResultBecauseSiblingOfService().has_value());
  }

  // consume the sibling result-generator
  for ([[maybe_unused]] auto& _ :
       siblingOperation->precomputedResultBecauseSiblingOfService()
           .value()
           ->idTables()) {
  }
}

// ____________________________________________________________________________
// The query planner puts a `StripColumns` (and a `Sort`) on top of a `Service`
// when not all of its variables are needed further up, e.g. for a `GROUP BY`.
// The sibling optimization must look through these operations (see #2967).
TEST_F(ServiceTest, precomputeSiblingResultWithStripColumns) {
  auto makeService = [&]() {
    return std::make_shared<Service>(
        testQec,
        parsedQuery::Service{
            {Variable{"?x"}, Variable{"?y"}},
            TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
            "PREFIX doof: <http://doof.org>",
            "{ }",
            true},
        getResultFunctionFactory(
            "http://localhorst:80/api",
            "PREFIX doof: <http://doof.org> SELECT ?x ?y { }",
            genJsonResult({"x", "y"}, {{"a", "b"}}),
            boost::beast::http::status::ok, "application/sparql-results+json"));
  };
  auto iri = ad_utility::testing::iri;
  using TC = TripleComponent;
  auto makeValues = [&]() {
    return std::make_shared<Values>(
        testQec, parsedQuery::SparqlValues{{Variable{"?x"}, Variable{"?y"}},
                                           {{TC(iri("<x>")), TC(iri("<y>"))}}});
  };
  // Wrap `op` into a `StripColumns` that only keeps `?x`, and that into a
  // `Sort`, which is the shape the query planner produces.
  auto makeTree = [&](std::shared_ptr<Operation> op) {
    return std::make_shared<QueryExecutionTree>(testQec, std::move(op));
  };
  auto wrap = [&](std::shared_ptr<Operation> op,
                  std::optional<LimitOffsetClause> limitForStripColumns =
                      std::nullopt) -> std::shared_ptr<Operation> {
    auto strip = std::make_shared<StripColumns>(
        testQec, makeTree(std::move(op)), std::set<Variable>{Variable{"?x"}});
    if (limitForStripColumns.has_value()) {
      strip->applyLimitOffset(limitForStripColumns.value(), false);
    }
    return std::make_shared<Sort>(testQec, makeTree(std::move(strip)),
                                  colIndices({0}));
  };
  // The `VALUES` clause that is expected when only `?x` is pushed down.
  const std::string valuesClauseX = "VALUES (?x) { (<x>) } ";

  // `Sort(StripColumns(Service))` on the right: the `Service` is found, the
  // sibling result is precomputed, and only `?x`, which is visible above the
  // `StripColumns`, is used for the `VALUES` clause (`?y` is hidden, so the
  // sibling's `?y` is a different variable).
  {
    auto service = makeService();
    auto sibling = makeValues();
    Service::precomputeSiblingResult(sibling, wrap(service), true, false);
    ASSERT_TRUE(service->siblingInfo_.has_value());
    EXPECT_EQ(service->getSiblingValuesClause(), valuesClauseX);
    EXPECT_TRUE(
        sibling->precomputedResultBecauseSiblingOfService().has_value());
  }

  // Same, but the sibling is wrapped (`?y` hidden on the sibling's side), once
  // with the `Service` on the left and once on the right.
  {
    auto service = makeService();
    auto sibling = makeValues();
    Service::precomputeSiblingResult(service, wrap(sibling), false, false);
    ASSERT_TRUE(service->siblingInfo_.has_value());
    EXPECT_EQ(service->getSiblingValuesClause(), valuesClauseX);
    EXPECT_TRUE(
        sibling->precomputedResultBecauseSiblingOfService().has_value());
  }
  {
    auto service = makeService();
    auto sibling = makeValues();
    Service::precomputeSiblingResult(wrap(sibling), service, true, false);
    ASSERT_TRUE(service->siblingInfo_.has_value());
    EXPECT_EQ(service->getSiblingValuesClause(), valuesClauseX);
    EXPECT_TRUE(
        sibling->precomputedResultBecauseSiblingOfService().has_value());
  }

  // A `LIMIT` on a skipped operation disables the optimization, on either
  // side.
  {
    auto service = makeService();
    auto sibling = makeValues();
    Service::precomputeSiblingResult(
        sibling, wrap(service, LimitOffsetClause{1}), true, false);
    EXPECT_FALSE(service->siblingInfo_.has_value());
    EXPECT_FALSE(
        sibling->precomputedResultBecauseSiblingOfService().has_value());
  }
  {
    auto service = makeService();
    auto sibling = makeValues();
    Service::precomputeSiblingResult(wrap(service, LimitOffsetClause{1}),
                                     sibling, false, false);
    EXPECT_FALSE(service->siblingInfo_.has_value());
    EXPECT_FALSE(
        sibling->precomputedResultBecauseSiblingOfService().has_value());
  }

  // Also for nested `Sort`s (as for a subquery with `ORDER BY ... LIMIT`),
  // where the `LIMIT` sits on the inner `Sort`.
  {
    auto service = makeService();
    auto sibling = makeValues();
    auto innerSort =
        std::make_shared<Sort>(testQec, makeTree(service), colIndices({0}));
    innerSort->applyLimitOffset(LimitOffsetClause{1}, false);
    auto outerSort =
        std::make_shared<Sort>(testQec, makeTree(innerSort), colIndices({0}));
    Service::precomputeSiblingResult(sibling, outerSort, true, false);
    EXPECT_FALSE(service->siblingInfo_.has_value());
    EXPECT_FALSE(
        sibling->precomputedResultBecauseSiblingOfService().has_value());
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
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json")};

  // SERVICE performs a network request and is therefore non-deterministic.
  EXPECT_FALSE(service.isDeterministic());
  auto clone = service.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(service, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), service.getDescriptor());
}

// _____________________________________________________________________________
TEST_F(ServiceTest, serviceAllowedIriPrefixes) {
  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://localhost/api>"),
      "PREFIX doof: <http://doof.org>",
      "{ }",
      false};
  parsedQuery::Service parsedServiceClauseSilent{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://localhost/api>"),
      "PREFIX doof: <http://doof.org>",
      "{ }",
      true};

  std::string_view expectedUrl = "http://localhost:80/api";
  std::string_view expectedSparqlQuery =
      "PREFIX doof: <http://doof.org> SELECT ?x ?y { }";
  auto result = genJsonResult({"x", "y"}, {{"a", "b"}});

  auto makeService = [&](const parsedQuery::Service& clause) {
    return Service{
        testQec, clause,
        getResultFunctionFactory(expectedUrl, expectedSparqlQuery, result)};
  };

  // With an empty whitelist (default), all URLs should be allowed.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::serviceAllowedIriPrefixes_>(
        std::vector<std::string>{});
    auto s = makeService(parsedServiceClause);
    EXPECT_NO_THROW(s.computeResultOnlyForTesting());
  }

  // With a matching prefix, the IRI should be allowed.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::serviceAllowedIriPrefixes_>(
        std::vector<std::string>{"http://localhost/"});
    auto s = makeService(parsedServiceClause);
    EXPECT_NO_THROW(s.computeResultOnlyForTesting());
  }

  // With a non-matching prefix, the IRI should be rejected.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::serviceAllowedIriPrefixes_>(
        std::vector<std::string>{"http://example.org/"});
    auto s = makeService(parsedServiceClause);
    AD_EXPECT_THROW_WITH_MESSAGE(s.computeResultOnlyForTesting(),
                                 ::testing::HasSubstr("not allowed"));
  }

  // With SILENT keyword, the rejected URL should be silenced.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::serviceAllowedIriPrefixes_>(
        std::vector<std::string>{"http://example.org/"});
    auto s = makeService(parsedServiceClauseSilent);
    EXPECT_NO_THROW(s.computeResultOnlyForTesting());
  }

  // With multiple prefixes, matching any should allow.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::serviceAllowedIriPrefixes_>(
        std::vector<std::string>{"http://example.org/", "http://localhost/"});
    auto s = makeService(parsedServiceClause);
    EXPECT_NO_THROW(s.computeResultOnlyForTesting());
  }

  // With multiple prefixes, none matching should reject.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::serviceAllowedIriPrefixes_>(
        std::vector<std::string>{"http://example.org/", "http://other.org/"});
    auto s = makeService(parsedServiceClause);
    AD_EXPECT_THROW_WITH_MESSAGE(s.computeResultOnlyForTesting(),
                                 ::testing::HasSubstr("not allowed"));
  }
}

// Test that a `Service` operation correctly passes the `maxRedirects` parameter
// to the HTTP client. The actual redirect handling is tested in `HttpTest.cpp`.
TEST_F(ServiceTest, redirectsIntegration) {
  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://example.com/api>"),
      "",
      "{ }",
      false};
  auto result = genJsonResult({"x", "y"}, {{"a", "b"}});

  // Test with default setting for `maxRedirects`, which is 1.
  {
    httpClientTestHelpers::RequestMatchers matchers{.maxRedirects_ =
                                                        testing::Eq(1)};
    Service service{testQec, parsedServiceClause,
                    httpClientTestHelpers::getResultFunctionFactory(
                        result, "application/sparql-results+json",
                        boost::beast::http::status::ok, matchers)};
    EXPECT_NO_THROW(service.computeResultOnlyForTesting());
  }

  // Test with custom setting for `maxRedirects`.
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::serviceMaxRedirects_>(5);
    httpClientTestHelpers::RequestMatchers matchers{.maxRedirects_ =
                                                        testing::Eq(5)};
    Service service{testQec, parsedServiceClause,
                    httpClientTestHelpers::getResultFunctionFactory(
                        result, "application/sparql-results+json",
                        boost::beast::http::status::ok, matchers)};
    EXPECT_NO_THROW(service.computeResultOnlyForTesting());
  }
}

// Test that a `Service` requests QLever's binary format if the runtime
// parameter `binary-service-enabled` is set, and that it correctly reads a
// result in that format.
TEST_F(ServiceTest, binaryFormat) {
  auto enableBinaryService =
      setRuntimeParameterForTest<&RuntimeParameters::binaryServiceEnabled_>(
          true);
  const auto& binaryMediaType =
      ad_utility::toString(ad_utility::MediaType::binaryQleverExport);

  // The result of the remote endpoint, which has the columns in a different
  // order than the `Service`.
  auto remoteResult = [this]() {
    QueryExecutionTree qet{
        testQec,
        std::make_shared<ValuesForTesting>(
            testQec, makeIdTableFromVector({{1, 2}, {3, 4}}, &Id::makeFromInt),
            std::vector<std::optional<Variable>>{Variable{"?y"},
                                                 Variable{"?x"}})};
    auto allocator = ad_utility::testing::makeAllocator();
    parsedQuery::SelectClause selectClause{allocator};
    selectClause.setSelected(
        qlm::vector<Variable>{{Variable{"?y"}, Variable{"?x"}}, allocator});
    std::string bytes;
    for (std::string_view chunk : qlever::binary_export::exportAsQLeverBinary(
             qet, selectClause, {},
             std::make_shared<ad_utility::CancellationHandle<>>())) {
      bytes.append(chunk);
    }
    return bytes;
  }();

  parsedQuery::Service parsedServiceClause{
      {Variable{"?x"}, Variable{"?y"}},
      TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
      "",
      "{ }",
      false};
  httpClientTestHelpers::RequestMatchers matchers{
      .accept_ = testing::Eq(absl::StrCat(
          binaryMediaType, ",application/sparql-results+json;q=0.9"))};
  Service service{testQec, parsedServiceClause,
                  httpClientTestHelpers::getResultFunctionFactory(
                      remoteResult, binaryMediaType,
                      boost::beast::http::status::ok, matchers)};
  auto result = service.computeResultOnlyForTesting();
  EXPECT_EQ(result.idTableView(),
            makeIdTableFromVector({{2, 1}, {4, 3}}, &Id::makeFromInt));

  // A different media type is not accepted.
  Service serviceWithWrongType{
      testQec, parsedServiceClause,
      httpClientTestHelpers::getResultFunctionFactory(
          remoteResult, absl::StrCat(binaryMediaType, "x"),
          boost::beast::http::status::ok, matchers)};
  AD_EXPECT_THROW_WITH_MESSAGE(
      serviceWithWrongType.computeResultOnlyForTesting(),
      ::testing::HasSubstr("QLever requires the endpoint of a SERVICE"));
}

// Test that the unconsumed lazy result of a `Service` sibling, which is
// destroyed together with the sibling, does not call back into the already
// destroyed sibling (this used to crash).
TEST_F(ServiceTest, unconsumedLazySiblingResultDoesNotCrash) {
  // A `Service` whose request fails, joined with a sibling whose lazy result is
  // precomputed, exceeds `serviceMaxValueRows_` and is therefore handed back to
  // the sibling, where the `Join` never consumes it.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::serviceMaxValueRows_>(0);
  auto service = std::make_shared<Service>(
      testQec,
      parsedQuery::Service{
          {Variable{"?x"}, Variable{"?y"}},
          TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
          "PREFIX doof: <http://doof.org>",
          "{ }",
          false},
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y { }", "service failed",
          boost::beast::http::status::internal_server_error, "text/plain"));
  auto sibling = std::make_shared<ValuesForTesting>(
      testQec, makeIdTableFromVector({{1, 2}, {3, 4}}),
      std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}});
  auto join = std::make_shared<Join>(
      testQec, std::make_shared<QueryExecutionTree>(testQec, service),
      std::make_shared<QueryExecutionTree>(testQec, sibling), 0, 0, true,
      false);

  // The `Service` is the left child, so it is computed (and fails) before the
  // sibling's precomputed result is consumed.
  EXPECT_ANY_THROW(join->computeResult(true));

  // Destroying the sibling destroys its unconsumed lazy result, whose cleanup
  // callback must not touch the operation anymore.
  join.reset();
  sibling.reset();
}

// Test that `OptionalJoin` and `Minus` with two join columns work next to a
// `Service` whose sibling result is not pushed down. They request fully
// materialized inputs, even if their own result is requested lazily (see
// #4025).
TEST_F(ServiceTest, precomputeSiblingResultForMultiColumnOptionalAndMinus) {
  // The sibling result is too large to be pushed down, so it is handed back to
  // the sibling.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::serviceMaxValueRows_>(0);
  auto makeTree = [&](std::shared_ptr<Operation> op) {
    return std::make_shared<QueryExecutionTree>(testQec, std::move(op));
  };
  auto makeService = [&]() {
    return makeTree(std::make_shared<Service>(
        testQec,
        parsedQuery::Service{
            {Variable{"?x"}, Variable{"?y"}},
            TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
            "PREFIX doof: <http://doof.org>",
            "{ }",
            false},
        getResultFunctionFactory(
            "http://localhorst:80/api",
            "PREFIX doof: <http://doof.org> SELECT ?x ?y { }",
            genJsonResult({"x", "y"}, {{"a", "b"}}),
            boost::beast::http::status::ok,
            "application/sparql-results+json")));
  };
  auto makeSibling = [&]() {
    return makeTree(std::make_shared<ValuesForTesting>(
        testQec, makeIdTableFromVector({{1, 2}, {3, 4}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false, std::vector<ColumnIndex>{0, 1}));
  };
  auto expected = makeIdTableFromVector({{1, 2}, {3, 4}});

  OptionalJoin optionalJoin{testQec, makeSibling(), makeService()};
  auto optionalResult = optionalJoin.computeResultOnlyForTesting(true);
  ASSERT_TRUE(optionalResult.isFullyMaterialized());
  EXPECT_EQ(optionalResult.idTableView(), expected);

  Minus minus{testQec, makeSibling(), makeService()};
  auto minusResult = minus.computeResultOnlyForTesting(true);
  ASSERT_TRUE(minusResult.isFullyMaterialized());
  EXPECT_EQ(minusResult.idTableView(), expected);
}

// Test that a lazy precomputed sibling result is materialized when the caller
// requests a fully materialized result, and passed on as is when the caller
// requests a lazy result. Without the former, e.g. a `Sort` that is skipped by
// `precomputeSiblingResult` and requests a materialized input breaks (#2375).
TEST_F(ServiceTest, lazyPrecomputedSiblingResultIsMaterializedOnRequest) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::serviceMaxValueRows_>(0);
  auto service = std::make_shared<Service>(
      testQec,
      parsedQuery::Service{
          {Variable{"?x"}, Variable{"?y"}},
          TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
          "PREFIX doof: <http://doof.org>",
          "{ }",
          false},
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json"));
  auto makeSibling = [&]() {
    std::vector<IdTable> chunks;
    chunks.push_back(makeIdTableFromVector({{1, 2}}));
    chunks.push_back(makeIdTableFromVector({{3, 4}}));
    return std::make_shared<ValuesForTesting>(
        testQec, std::move(chunks),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false, std::vector<ColumnIndex>{0, 1});
  };

  // Requested fully materialized: the chunks are concatenated.
  auto sibling = makeSibling();
  Service::precomputeSiblingResult(sibling, service, true, true);
  ASSERT_TRUE(sibling->precomputedResultBecauseSiblingOfService().has_value());
  ASSERT_FALSE(sibling->precomputedResultBecauseSiblingOfService()
                   .value()
                   ->isFullyMaterialized());
  auto result = sibling->getResult();
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTableView(), makeIdTableFromVector({{1, 2}, {3, 4}}));
  EXPECT_THAT(result->sortedBy(), ::testing::ElementsAre(0, 1));
  EXPECT_FALSE(sibling->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_EQ(
      sibling->runtimeInfo().details_["lazy-result-materialized-for-parent"],
      true);

  // Requested lazily: the lazy result is passed on as is. The consumed lazy
  // result above was cached, so clear the cache.
  testQec->clearCacheUnpinnedOnly();
  sibling = makeSibling();
  Service::precomputeSiblingResult(sibling, service, true, true);
  result = sibling->getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
  ASSERT_FALSE(result->isFullyMaterialized());
  EXPECT_FALSE(sibling->runtimeInfo().details_.contains(
      "lazy-result-materialized-for-parent"));
  std::vector<IdTable> chunks;
  for (auto& pair : result->idTables()) {
    chunks.push_back(pair.idTable_.clone());
  }
  EXPECT_THAT(chunks,
              ::testing::ElementsAre(matchesIdTableFromVector({{1, 2}}),
                                     matchesIdTableFromVector({{3, 4}})));
}

// Test that an error during the conversion of a lazy precomputed sibling
// result (here because of the memory limit) is propagated, and that the
// incomplete result is neither cached nor kept for later.
TEST_F(ServiceTest, failedMaterializationOfLazyPrecomputedSiblingResult) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::serviceMaxValueRows_>(0);
  // Each chunk is 1.6 MB, so the concatenation of both exceeds the 2 MB limit.
  // The chunks are allocated without a limit, so only the conversion fails.
  auto index = std::make_shared<Index>(ad_utility::testing::makeTestIndex(
      ad_utility::testing::TestIndexConfig{}));
  QueryResultCache cache;
  NamedResultCache namedCache;
  QueryExecutionContext qec{
      index,
      &cache,
      ad_utility::testing::makeAllocator(ad_utility::MemorySize::megabytes(2)),
      &namedCache,
      std::make_shared<MaterializedViewsManager>(),
      ad_utility::testing::singleThreadExecutor()};

  auto service = std::make_shared<Service>(
      &qec,
      parsedQuery::Service{
          {Variable{"?x"}, Variable{"?y"}},
          TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
          "PREFIX doof: <http://doof.org>",
          "{ }",
          false},
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y { }",
          genJsonResult({"x", "y"}, {{"a", "b"}}),
          boost::beast::http::status::ok, "application/sparql-results+json"));
  std::vector<IdTable> chunks;
  for (size_t i = 0; i < 2; ++i) {
    IdTable chunk{2, ad_utility::makeUnlimitedAllocator<Id>()};
    chunk.resize(100'000);
    ql::ranges::fill(chunk.getColumn(0), Id::makeFromInt(1));
    ql::ranges::fill(chunk.getColumn(1), Id::makeFromInt(2));
    chunks.push_back(std::move(chunk));
  }
  auto sibling = std::make_shared<ValuesForTesting>(
      &qec, std::move(chunks),
      std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}});

  Service::precomputeSiblingResult(sibling, service, true, true);
  ASSERT_TRUE(sibling->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_THROW(sibling->getResult(),
               ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_FALSE(sibling->precomputedResultBecauseSiblingOfService().has_value());
  EXPECT_EQ(
      sibling->runtimeInfo().details_["lazy-result-materialized-for-parent"],
      true);
  EXPECT_EQ(sibling->getResult(false, ComputationMode::ONLY_IF_CACHED),
            nullptr);
}

// Test that `makeTreeWithStrippedColumns` returns a `Service` with only the
// requested variables, which requests only these from the remote endpoint, and
// `std::nullopt` if nothing is to be stripped.
TEST_F(ServiceTest, makeTreeWithStrippedColumns) {
  using ::column_stripping_test_helpers::hasVariables;
  using ::column_stripping_test_helpers::isStrippedColumns;
  using V = Variable;

  // A `SERVICE` with the visible variables `?x`, `?y` and `?z`.
  parsedQuery::Service parsedServiceClause{
      {V{"?x"}, V{"?y"}, V{"?z"}},
      TripleComponent::Iri::fromIriref("<http://localhorst/api>"),
      "PREFIX doof: <http://doof.org>",
      "{ }",
      false};
  auto strip = [&](const std::set<V>& requestedVariables) {
    Service service{testQec, parsedServiceClause};
    return service.makeTreeWithStrippedColumns(requestedVariables);
  };

  // Only `?x` and `?z` requested: `?y` is stripped.
  {
    auto tree = strip({V{"?x"}, V{"?z"}});
    ASSERT_TRUE(tree.has_value());
    EXPECT_THAT(tree->get(), hasVariables({V{"?x"}, V{"?z"}}));
  }

  // A requested variable that the `Service` does not have is ignored.
  {
    auto tree = strip({V{"?x"}, V{"?z"}, V{"?notIncluded"}});
    ASSERT_TRUE(tree.has_value());
    EXPECT_THAT(tree->get(), hasVariables({V{"?x"}, V{"?z"}}));
  }

  // All variables requested: nothing is stripped.
  EXPECT_EQ(strip({V{"?x"}, V{"?y"}, V{"?z"}}), std::nullopt);
  EXPECT_EQ(strip({V{"?x"}, V{"?y"}, V{"?z"}, V{"?notIncluded"}}),
            std::nullopt);

  // None of the variables requested: the `Service` is kept, since a `SELECT`
  // without variables cannot be sent to the remote endpoint.
  EXPECT_EQ(strip({}), std::nullopt);
  EXPECT_EQ(strip({V{"?notIncluded"}}), std::nullopt);

  // The stripped `Service` requests only the remaining variables from the
  // remote endpoint.
  {
    Service service{testQec, parsedServiceClause,
                    getResultFunctionFactory(
                        "http://localhorst:80/api",
                        "PREFIX doof: <http://doof.org> SELECT ?y { }",
                        genJsonResult({"y"}, {{"bla"}, {"bli"}, {"blu"}}))};
    auto tree = service.makeTreeWithStrippedColumns({V{"?y"}});
    ASSERT_TRUE(tree.has_value());
    EXPECT_THAT(tree->get(),
                isStrippedColumns<Service>({V{"?y"}}, ::testing::SizeIs(3)));
  }
}
