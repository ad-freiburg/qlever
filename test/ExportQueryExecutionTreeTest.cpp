//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryPlanner.h"
#include "parser/SparqlParser.h"

std::string runTSVQuery(const std::string& kg, const std::string& query) {
  auto qec = ad_utility::testing::getQec(kg);
  QueryPlanner qp{qec};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  auto tsvGenerator = ExportQueryExecutionTrees::queryToStreamableGenerator(
      pq, qet, ad_utility::MediaType::tsv);
  std::string result;
  for (const auto& block : tsvGenerator) {
    result += block;
  }
  return result;
}

// TODO<joka921> This is a lot of duplication from the TSV function
std::string runCSVQuery(const std::string& kg, const std::string& query) {
  auto qec = ad_utility::testing::getQec(kg);
  QueryPlanner qp{qec};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  auto generator = ExportQueryExecutionTrees::queryToStreamableGenerator(
      pq, qet, ad_utility::MediaType::csv);
  std::string result;
  for (const auto& block : generator) {
    result += block;
  }
  return result;
}

std::string kgInt =
    "<s> <p> 42 . <s> <p> -42019234865781 . <s> <p> 4012934858173560";
std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";

struct ExportTestCase {
  std::string kg;
  std::string query;
  std::string resultTsv;
  std::string resultCsv;
  // TODO<joka921> Add the otherFormats.
};

ExportTestCase testCaseInt{kgInt, objectQuery,
                           "?o\n42\n4012934858173560\n-42019234865781\n",
                           "?o\n42\n4012934858173560\n-42019234865781\n"};

TEST(ExportQueryExecutionTree, SelectQueryTSV) {
  ASSERT_EQ(runTSVQuery(testCaseInt.kg, testCaseInt.query),
            testCaseInt.resultTsv);
  ASSERT_EQ(runCSVQuery(testCaseInt.kg, testCaseInt.query),
            testCaseInt.resultCsv);
}