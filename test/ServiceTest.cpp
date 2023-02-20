//  Copyright 2022 - 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <regex>

#include "./HttpTestHelpers.h"
#include "./IndexTestHelpers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "ctre/ctre.h"
#include "engine/Service.h"
#include "parser/GraphPatternOperation.h"
#include "util/http/HttpUtils.h"

// Fixture that sets up a test index and a factory for producing mocks for the
// `getTsvFunction` needed by the `Service` operation.
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
  // 4. It returns the specified TSV.
  //
  // NOTE: In a previous version of this test, we set up an actual test server.
  // The code can be found in the history of this PR.
  static auto constexpr getTsvFunctionFactory =
      [](const std::string& expectedUrl, const std::string& expectedSparqlQuery,
         const std::string& predefinedResult) -> Service::GetTsvFunction {
    return [=](ad_utility::httpUtils::Url url,
               const boost::beast::http::verb& method,
               std::string_view postData, std::string_view contentTypeHeader,
               std::string_view acceptHeader) -> std::istringstream {
      // Check that the request parameters are as expected.
      //
      // NOTE: The first three are hard-coded in `Service::computeResult`, but
      // the host and port of the endpoint are derived from the IRI, so the last
      // two checks are non-trivial.
      EXPECT_EQ(method, http::verb::post);
      EXPECT_EQ(contentTypeHeader, "application/sparql-query");
      EXPECT_EQ(acceptHeader, "text/tab-separated-values");
      EXPECT_EQ(url.asString(), expectedUrl);

      // Check that the (whitespace-normalized) post data contains the expected
      // query.
      //
      // NOTE: a SERVICE clause specifies only the body of a SPARQL query, from
      // which `Service::computeResult` has to construct a full SPARQL query by
      // adding `SELECT ... WHERE`, so this checks something non-trivial.
      EXPECT_EQ(
          std::regex_replace(std::string{postData}, std::regex{"\\s+"}, " "),
          expectedSparqlQuery);

      return std::istringstream{predefinedResult};
    };
  };
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
                                           "{ }",
                                           parsedQuery::GraphPattern{}};
  // Create an operation from this.
  Service serviceOp{testQec, parsedServiceClause};

  // Test the basic methods.
  ASSERT_EQ(serviceOp.getDescriptor(),
            "Service with IRI <http://localhorst/api>");
  ASSERT_EQ(serviceOp.getResultWidth(), 2);
  ASSERT_EQ(serviceOp.getMultiplicity(0), 1);
  ASSERT_EQ(serviceOp.getMultiplicity(1), 1);
  ASSERT_EQ(serviceOp.getSizeEstimate(), 100'000);
  ASSERT_EQ(serviceOp.getCostEstimate(), 1'000'000);
  VariableToColumnMap varToColMap = serviceOp.computeVariableToColumnMap();
  ASSERT_EQ(varToColMap.size(), 2);
  ASSERT_EQ(varToColMap.at(Variable{"?x"}), 0);
  ASSERT_EQ(varToColMap.at(Variable{"?y"}), 1);
  ASSERT_FALSE(serviceOp.knownEmptyResult());
  ASSERT_EQ(serviceOp.getChildren(), std::vector<QueryExecutionTree*>{});
}

// Tests that `computeResult` behaves as expected.
TEST_F(ServiceTest, computeResult) {
  // Construct a parsed SERVICE clause by hand, see `basicMethods` test above.
  parsedQuery::Service parsedServiceClause{{Variable{"?x"}, Variable{"?y"}},
                                           Iri{"<http://localhorst/api>"},
                                           "PREFIX doof: <http://doof.org>",
                                           "{ }",
                                           parsedQuery::GraphPattern{}};

  // This is the (port-normalized) URL and (whitespace-normalized) SPARQL query
  // we expect.
  std::string expectedUrl = "http://localhorst:80/api";
  std::string expectedSparqlQuery =
      "PREFIX doof: <http://doof.org> SELECT ?x ?y WHERE { }";

  // CHECK 1: Returned TSV is empty -> an exception should be thrown.
  Service serviceOperation1{
      testQec, parsedServiceClause,
      getTsvFunctionFactory(expectedUrl, expectedSparqlQuery, "")};
  ASSERT_THROW(serviceOperation1.getResult(), ad_utility::AbortException);

  // CHECK 2: Header row of returned TSV is wrong (variables in wrong order) ->
  // an exception should be thrown.
  Service serviceOperation2{
      testQec, parsedServiceClause,
      getTsvFunctionFactory(
          expectedUrl, expectedSparqlQuery,
          "?y\t?x\n<x>\t<y>\n<bla>\t<bli>\n<blu>\t<bla>\n<bli>\t<blu>\n")};
  ASSERT_THROW(serviceOperation2.getResult(), ad_utility::AbortException);

  // CHECK 3: Returned TSV has correct format matching the query -> check that
  // the result table returned by the operation corresponds to the contents of
  // the TSV and its local vocabulary are correct.
  Service serviceOperation3{
      testQec, parsedServiceClause,
      getTsvFunctionFactory(
          expectedUrl, expectedSparqlQuery,
          "?x\t?y\n<x>\t<y>\n<bla>\t<bli>\n<blu>\t<bla>\n<bli>\t<blu>\n")};
  std::shared_ptr<const ResultTable> result = serviceOperation3.getResult();

  // Check that `<x>` and `<y>` were contained in the original vocabulary and
  // that `<bla>`, `<bli>`, `<blu>` were added to the (initially empty) local
  // vocabulary.
  VocabIndex idxX;
  VocabIndex idxY;
  EXPECT_TRUE(testQec->getIndex().getVocab().getId("<x>", &idxX));
  EXPECT_TRUE(testQec->getIndex().getVocab().getId("<y>", &idxY));
  LocalVocabIndex idxBla = LocalVocabIndex::make(0);
  LocalVocabIndex idxBli = LocalVocabIndex::make(1);
  LocalVocabIndex idxBlu = LocalVocabIndex::make(2);
  EXPECT_EQ(result->localVocab().getWord(idxBla), "<bla>");
  EXPECT_EQ(result->localVocab().getWord(idxBli), "<bli>");
  EXPECT_EQ(result->localVocab().getWord(idxBlu), "<blu>");

  // Check that the result table corresponds to the contents of the TSV.
  EXPECT_TRUE(result);
  EXPECT_EQ(result->_idTable.numColumns(), 2);
  EXPECT_EQ(result->_idTable.numRows(), 4);
  Id idX = Id::makeFromVocabIndex(idxX);
  Id idY = Id::makeFromVocabIndex(idxY);
  Id idBla = Id::makeFromLocalVocabIndex(idxBla);
  Id idBli = Id::makeFromLocalVocabIndex(idxBli);
  Id idBlu = Id::makeFromLocalVocabIndex(idxBlu);
  auto checkRow = [&](size_t rowIdx, Id id1, Id id2) {
    EXPECT_EQ(result->_idTable(rowIdx, 0), id1) << "at row " << rowIdx;
    EXPECT_EQ(result->_idTable(rowIdx, 1), id2) << "at row " << rowIdx;
  };
  checkRow(0, idX, idY);
  checkRow(1, idBla, idBli);
  checkRow(2, idBlu, idBla);
  checkRow(3, idBli, idBlu);
}
