//  Copyright 2022 - 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <ctre-unicode.hpp>
#include <regex>

#include "engine/Service.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
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
         std::string predefinedResult) -> Service::GetResultFunction {
    return [=](const ad_utility::httpUtils::Url& url,
               ad_utility::SharedCancellationHandle,
               const boost::beast::http::verb& method,
               std::string_view postData, std::string_view contentTypeHeader,
               std::string_view acceptHeader) {
      // Check that the request parameters are as expected.
      //
      // NOTE: The first three are hard-coded in `Service::computeResult`, but
      // the host and port of the endpoint are derived from the IRI, so the last
      // two checks are non-trivial.
      EXPECT_EQ(method, boost::beast::http::verb::post);
      EXPECT_EQ(contentTypeHeader, "application/sparql-query");
      EXPECT_EQ(acceptHeader, "application/sparql-results+json");
      EXPECT_EQ(url.asString(), expectedUrl);

      // Check that the whitespace-normalized POST data is the expected query.
      //
      // NOTE: a SERVICE clause specifies only the body of a SPARQL query, from
      // which `Service::computeResult` has to construct a full SPARQL query by
      // adding `SELECT ... WHERE`, so this checks something non-trivial.
      std::string whitespaceNormalizedPostData =
          std::regex_replace(std::string{postData}, std::regex{"\\s+"}, " ");
      EXPECT_EQ(whitespaceNormalizedPostData, expectedSparqlQuery);
      return
          [](std::string result) -> cppcoro::generator<std::span<std::byte>> {
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
          }(predefinedResult);
    };
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
};

// Test basic methods of class `Service`.
TEST_F(ServiceTest, basicMethods) {
  // Construct a parsed SERVICE clause by hand. The fourth argument is the query
  // body (empty in this case because this test is not about evaluating a
  // query). The fourth argument plays no role in our test (and isn't really
  // used in `parsedQuery::Service` either).
  parsedQuery::Service parsedServiceClause{{Variable{"?x"}, Variable{"?y"}},
                                           Iri{"<http://localhorst/api>"},
                                           "PREFIX doof: <http://doof.org>",
                                           "{ }"};
  // Create an operation from this.
  Service serviceOp{testQec, parsedServiceClause};

  // Test the basic methods.
  ASSERT_EQ(serviceOp.getDescriptor(),
            "Service with IRI <http://localhorst/api>");
  ASSERT_TRUE(
      serviceOp.getCacheKey().starts_with("SERVICE <http://localhorst/api>"))
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
  // Construct a parsed SERVICE clause by hand, see `basicMethods` test above.
  parsedQuery::Service parsedServiceClause{{Variable{"?x"}, Variable{"?y"}},
                                           Iri{"<http://localhorst/api>"},
                                           "PREFIX doof: <http://doof.org>",
                                           "{ }"};

  // This is the (port-normalized) URL and (whitespace-normalized) SPARQL query
  // we expect.
  std::string_view expectedUrl = "http://localhorst:80/api";
  std::string_view expectedSparqlQuery =
      "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }";

  // Shorthand to run computeResult with the test parameters given above.
  auto runComputeResult =
      [&](const std::string& result) -> std::shared_ptr<const Result> {
    Service s{
        testQec, parsedServiceClause,
        getResultFunctionFactory(expectedUrl, expectedSparqlQuery, result)};
    return s.getResult();
  };

  // CHECK 1: Returned Result is no JSON, is empty or has invalid structure
  // -> an exception should be thrown.
  ASSERT_ANY_THROW(
      runComputeResult("<?xml version=\"1.0\"?><sparql "
                       "xmlns=\"http://www.w3.org/2005/sparql-results#\">"));
  ASSERT_ANY_THROW(runComputeResult("{}"));
  ASSERT_ANY_THROW(runComputeResult("{\"invalid\": \"structure\"}"));
  ASSERT_ANY_THROW(
      runComputeResult("{\"head\": {\"vars\": [1, 2, 3]},"
                       "\"results\": {\"bindings\": {}}}"));

  // CHECK 2: Header row of returned JSON is wrong (variables in wrong order) ->
  // an exception should be thrown.
  ASSERT_ANY_THROW(runComputeResult(genJsonResult(
      {"y", "x"}, {{"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}})));

  // CHECK 3: A result row of the returned JSON is missing a variable's value ->
  // undefined value
  auto result3 = runComputeResult(
      genJsonResult({"x", "y"}, {{"bla", "bli"}, {"blu"}, {"bli", "blu"}}));
  EXPECT_TRUE(result3);
  EXPECT_TRUE(result3->idTable().at(1, 1).isUndefined());

  testQec->clearCacheUnpinnedOnly();

  // CHECK 4: Returned JSON has correct format matching the query -> check that
  // the result table returned by the operation corresponds to the contents of
  // the JSON and its local vocabulary are correct.
  std::shared_ptr<const Result> result = runComputeResult(genJsonResult(
      {"x", "y"},
      {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}}));

  // Check that `<x>` and `<y>` were contained in the original vocabulary and
  // that `<bla>`, `<bli>`, `<blu>` were added to the (initially empty) local
  // vocabulary. On the way, obtain their IDs, which we then need below.
  auto getId = ad_utility::testing::makeGetId(testQec->getIndex());
  Id idX = getId("<x>");
  Id idY = getId("<y>");
  const auto& localVocab = result->localVocab();
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
  EXPECT_TRUE(result);
  IdTable expectedIdTable = makeIdTableFromVector(
      {{idX, idY}, {idBla, idBli}, {idBlu, idBla}, {idBli, idBlu}});
  EXPECT_EQ(result->idTable(), expectedIdTable);

  // Check 5: When a siblingTree with variables common to the Service Clause is
  // passed, the Service Operation shall use the siblings result to reduce
  // its Query complexity by injecting them as Value Clause
  auto iri = ad_utility::testing::iri;
  using TC = TripleComponent;
  auto siblingTree = std::make_shared<QueryExecutionTree>(
      testQec,
      std::make_shared<Values>(
          testQec,
          (parsedQuery::SparqlValues){
              {Variable{"?x"}, Variable{"?y"}, Variable{"?z"}},
              {{TC(iri("<x>")), TC(iri("<y>")), TC(iri("<z>"))},
               {TC(iri("<x>")), TC(iri("<y>")), TC(iri("<z2>"))},
               {TC(iri("<blu>")), TC(iri("<bla>")), TC(iri("<blo>"))}}}));

  auto parsedServiceClause5 = parsedServiceClause;
  parsedServiceClause5.graphPatternAsString_ =
      "{ ?x <ble> ?y . ?y <is-a> ?z2 . }";
  parsedServiceClause5.visibleVariables_.emplace_back("?z2");

  std::string_view expectedSparqlQuery5 =
      "PREFIX doof: <http://doof.org> SELECT ?x ?y ?z2 "
      "WHERE { VALUES (?x ?y) { (<x> <y>) (<blu> <bla>) } . ?x <ble> ?y . ?y "
      "<is-a> ?z2 . }";

  Service serviceOperation5{
      testQec, parsedServiceClause5,
      getResultFunctionFactory(
          expectedUrl, expectedSparqlQuery5,
          genJsonResult({"x", "y", "z2"}, {{"x", "y", "y"},
                                           {"bla", "bli", "y"},
                                           {"blu", "bla", "y"},
                                           {"bli", "blu", "y"}})),
      siblingTree};
  EXPECT_NO_THROW(serviceOperation5.getResult());

  // Check 6: SiblingTree's rows exceed maxValue
  const auto maxValueRowsDefault =
      RuntimeParameters().get<"service-max-value-rows">();
  RuntimeParameters().set<"service-max-value-rows">(0);
  testQec->getQueryTreeCache().clearAll();
  std::string_view expectedSparqlQuery6 =
      "PREFIX doof: <http://doof.org> SELECT ?x ?y ?z2 "
      "WHERE { ?x <ble> ?y . ?y <is-a> ?z2 . }";
  Service serviceOperation6{
      testQec, parsedServiceClause5,
      getResultFunctionFactory(
          expectedUrl, expectedSparqlQuery6,
          genJsonResult({"x", "y", "z2"}, {{"x", "y", "y"},
                                           {"bla", "bli", "y"},
                                           {"blue", "bla", "y"},
                                           {"bli", "blu", "y"}})),
      siblingTree};
  EXPECT_NO_THROW(serviceOperation6.getResult());
  RuntimeParameters().set<"service-max-value-rows">(maxValueRowsDefault);
}

TEST_F(ServiceTest, getCacheKey) {
  parsedQuery::Service parsedServiceClause{{Variable{"?x"}, Variable{"?y"}},
                                           Iri{"<http://localhorst/api>"},
                                           "PREFIX doof: <http://doof.org>",
                                           "{ }"};

  // The cacheKey of the Service Operation has to depend on the cacheKey of
  // the siblingTree, as it might alter the Service Query.

  Service service(
      testQec, parsedServiceClause,
      getResultFunctionFactory(
          "http://localhorst:80/api",
          "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }",
          genJsonResult(
              {"x", "y"},
              {{"x", "y"}, {"bla", "bli"}, {"blu", "bla"}, {"bli", "blu"}})));

  auto ck_noSibling = service.getCacheKey();

  auto iri = ad_utility::testing::iri;
  using TC = TripleComponent;
  auto siblingTree = std::make_shared<QueryExecutionTree>(
      testQec,
      std::make_shared<Values>(
          testQec,
          (parsedQuery::SparqlValues){
              {Variable{"?x"}, Variable{"?y"}, Variable{"?z"}},
              {{TC(iri("<x>")), TC(iri("<y>")), TC(iri("<z>"))},
               {TC(iri("<blu>")), TC(iri("<bla>")), TC(iri("<blo>"))}}}));
  service.setSiblingTree(siblingTree);

  auto ck_sibling = service.getCacheKey();
  EXPECT_NE(ck_noSibling, ck_sibling);

  auto siblingTree2 = std::make_shared<QueryExecutionTree>(
      testQec,
      std::make_shared<Values>(
          testQec, (parsedQuery::SparqlValues){
                       {Variable{"?x"}, Variable{"?y"}, Variable{"?z"}},
                       {{TC(iri("<x>")), TC(iri("<y>")), TC(iri("<z>"))}}}));

  service.setSiblingTree(siblingTree2);

  auto ck_changedSibling = service.getCacheKey();
  EXPECT_NE(ck_sibling, ck_changedSibling);
}

// Test that bindingToValueId behaves as expected.
TEST_F(ServiceTest, bindingToTripleComponent) {
  Index::Vocab vocabulary;
  nlohmann::json binding;

  // Missing type or value.
  EXPECT_ANY_THROW(Service::bindingToTripleComponent({{"type", "literal"}}));
  EXPECT_ANY_THROW(Service::bindingToTripleComponent({{"value", "v"}}));

  EXPECT_EQ(
      Service::bindingToTripleComponent(
          {{"type", "literal"}, {"value", "42"}, {"datatype", XSD_INT_TYPE}}),
      42);

  EXPECT_EQ(
      Service::bindingToTripleComponent(
          {{"type", "literal"}, {"value", "Hallo Welt"}, {"xml:lang", "de"}}),
      TripleComponent::Literal::literalWithoutQuotes("Hallo Welt", "@de"));

  EXPECT_EQ(Service::bindingToTripleComponent(
                {{"type", "literal"}, {"value", "Hello World"}}),
            TripleComponent::Literal::literalWithoutQuotes("Hello World"));

  EXPECT_EQ(Service::bindingToTripleComponent(
                {{"type", "uri"}, {"value", "http://doof.org"}}),
            TripleComponent::Iri::fromIrirefWithoutBrackets("http://doof.org"));

  // Blank Node not supported yet.
  EXPECT_ANY_THROW(
      Service::bindingToTripleComponent({{"type", "bnode"}, {"value", "b"}}));

  EXPECT_ANY_THROW(Service::bindingToTripleComponent(
      {{"type", "INVALID_TYPE"}, {"value", "v"}}));
}
