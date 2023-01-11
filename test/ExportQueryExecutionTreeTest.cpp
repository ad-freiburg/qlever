//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryPlanner.h"
#include "parser/SparqlParser.h"
#include "./util/GTestHelpers.h"

using namespace std::string_literals;

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

nlohmann::json runQLeverJSONQuery(const std::string& kg,
                                  const std::string& query) {
  auto qec = ad_utility::testing::getQec(kg);
  QueryPlanner qp{qec};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer;
  return ExportQueryExecutionTrees::queryToJSON(
      pq, qet, timer, 200, ad_utility::MediaType::qleverJson);
}

// TODO<joka921> A lot of duplication from the QLeverJSON function above.
nlohmann::json runSparqlJSONQuery(const std::string& kg,
                                  const std::string& query) {
  auto qec = ad_utility::testing::getQec(kg);
  QueryPlanner qp{qec};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer;
  return ExportQueryExecutionTrees::queryToJSON(
      pq, qet, timer, 200, ad_utility::MediaType::sparqlJson);
}


struct TestCaseSelectQuery {
  std::string kg;
  std::string query;
  size_t resultSize;
  std::string resultTsv;
  std::string resultCsv;
  nlohmann::json resultQLeverJSON;
  nlohmann::json resultSparqlJSON;
  // TODO<joka921> Add the otherFormats.
};

nlohmann::json makeJSONBinding(const std::optional<std::string>& datatype, const std::string& type, const std::string& value) {
  std::unordered_map<std::string, std::string> m;
  if (datatype.has_value()) {
    m["datatype"] = datatype.value();
  }
  m["type"] = type;
  m["value"] = value;
  return m;
}

void runSelectQueryTestCase(const TestCaseSelectQuery& testCase, ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l, "runSelectQueryTestCase");
  EXPECT_EQ(runTSVQuery(testCase.kg, testCase.query),
            testCase.resultTsv);
  EXPECT_EQ(runCSVQuery(testCase.kg, testCase.query),
            testCase.resultCsv);
  auto qleverJSONResult = runQLeverJSONQuery(testCase.kg, testCase.query);
  ASSERT_EQ(qleverJSONResult["query"], testCase.query);
  ASSERT_EQ(qleverJSONResult["resultsize"], testCase.resultSize);
  EXPECT_EQ(qleverJSONResult["res"], testCase.resultQLeverJSON);

  auto sparqlJSONResult = runSparqlJSONQuery(testCase.kg, testCase.query);
  EXPECT_EQ(sparqlJSONResult, testCase.resultSparqlJSON);

}

TEST(ExportQueryExecutionTree, Integers) {
  std::string kgInt =
      "<s> <p> 42 . <s> <p> -42019234865781 . <s> <p> 4012934858173560";
  std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCaseInt{
      kgInt,
      objectQuery,
      3,
      // TODO<joka921> the ORDER BY of negative numbers is incorrect.
      "?o\n42\n4012934858173560\n-42019234865781\n",
      "?o\n42\n4012934858173560\n-42019234865781\n",
      []() {
        nlohmann::json j;
        j.push_back(
            std::vector{"\"42\"^^<http://www.w3.org/2001/XMLSchema#int>"s});
        j.push_back(
            std::vector{"\"4012934858173560\"^^<http://www.w3.org/2001/XMLSchema#int>"s});
        j.push_back(
            std::vector{"\"-42019234865781\"^^<http://www.w3.org/2001/XMLSchema#int>"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        auto& res = j["results"]["bindings"];
        res.emplace_back();
        res.back()["o"] = (makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal", "42"));
        res.emplace_back();
        res.back()["o"] = makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal", "4012934858173560");
        res.emplace_back();
        res.back()["o"] = makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal", "-42019234865781");
        return j;
      }(),
  };
  runSelectQueryTestCase(testCaseInt);
}

TEST(ExportQueryExecutionTree, Floats) {
  std::string kgFloat =
      "<s> <p> 42.2 . <s> <p> -42019234865.781e12 . <s> <p> 4.012934858173560e-12";
  std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCaseFloat{
      kgFloat,
      objectQuery,
      3,
      // TODO<joka921> The sorting is wrong, and the formatting of the negative
      // number is strange.
      "?o\n4.01293e-12\n42.2\n-42019234865780982022144\n",
      "?o\n4.01293e-12\n42.2\n-42019234865780982022144\n",
      []() {
        nlohmann::json j;
        j.push_back(
            std::vector{"\"4.01293e-12\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s});
        j.push_back(
            std::vector{"\"42.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s});
        j.push_back(
            std::vector{"\"-42019234865780982022144\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        auto& res = j["results"]["bindings"];
        res.emplace_back();
        res.back()["o"] = (makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal", "literal", "4.01293e-12"));
        res.emplace_back();
        res.back()["o"] = makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal", "literal", "42.2");
        res.emplace_back();
        res.back()["o"] = makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal", "literal", "-42019234865780982022144");
        return j;
      }(),
  };
  runSelectQueryTestCase(testCaseFloat);
}

TEST(ExportQueryExecutionTree, Dates) {
  std::string kgDate =
      "<s> <p> \"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>.";
  std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCaseFloat{
      kgDate,
      objectQuery,
      1,
      "?o\n\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>\n",
      // Note: the duplicate quotes are due to the escaping for CSV.
      "?o\n\"\"\"1950-01-01T00:00:00\"\"^^<http://www.w3.org/2001/XMLSchema#dateTime>\"\n",
      []() {
        nlohmann::json j;
        j.push_back(
            std::vector{"\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        auto& res = j["results"]["bindings"];
        res.emplace_back();
        res.back()["o"] = (makeJSONBinding("http://www.w3.org/2001/XMLSchema#dateTime", "literal", "1950-01-01T00:00:00"));
        return j;
      }(),
  };
  runSelectQueryTestCase(testCaseFloat);
}

TEST(ExportQueryExecutionTree, Entities) {
  // TODO<joka921> Also add a test for an entity that comes from the local vocab.
  std::string kgEntity =
      "PREFIX qlever: <http://qlever.com/> \n <s> <p> qlever:o";
  std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCaseFloat{
      kgEntity,
      objectQuery,
      1,
      "?o\n<http://qlever.com/o>\n",
      "?o\n<http://qlever.com/o>\n",
      []() {
        nlohmann::json j;
        j.push_back(
            std::vector{"<http://qlever.com/o>"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        auto& res = j["results"]["bindings"];
        res.emplace_back();
        res.back()["o"] = (makeJSONBinding(std::nullopt, "uri", "http://qlever.com/o"));
        return j;
      }(),
  };
  runSelectQueryTestCase(testCaseFloat);
}

// TODO<joka921> Missing tests:
/* 1. Literals (language tags, escaping for TSV/CSV/JSON)
 * 2. Undefined/Null-values
 * 3. Multiple variables (important for TSV/CSV test coverage.
 * 4. CONSTRUCT queries (especially for the Turtle export).
 */