//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryPlanner.h"
#include "parser/SparqlParser.h"

using namespace std::string_literals;

// Run the given SPARQL `query` on the given Turtle `kg` and export the result
// as the `mediaType`. `mediaType` must be TSV or CSV.
std::string runQueryStreamableResult(const std::string& kg,
                                     const std::string& query,
                                     ad_utility::MediaType mediaType) {
  auto qec = ad_utility::testing::getQec(kg);
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  QueryPlanner qp{qec};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  auto tsvGenerator =
      ExportQueryExecutionTrees::queryToStreamableGenerator(pq, qet, mediaType);
  std::string result;
  for (const auto& block : tsvGenerator) {
    result += block;
  }
  return result;
}

// Run the given SPARQL `query` on the given Turtle `kg` and export the result
// as JSON. `mediaType` must be `sparqlJSON` or `qleverJSON`.
nlohmann::json runJSONQuery(const std::string& kg, const std::string& query,
                            ad_utility::MediaType mediaType) {
  auto qec = ad_utility::testing::getQec(kg);
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  QueryPlanner qp{qec};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer;
  return ExportQueryExecutionTrees::queryToJSON(pq, qet, timer, 200, mediaType);
}

// A test case that tests the correct execution and exporting of a SELECT query
// in various formats.
struct TestCaseSelectQuery {
  std::string kg;                   // The knowledge graph (TURTLE)
  std::string query;                // The query (SPARQL)
  size_t resultSize;                // The expected number of results.
  std::string resultTsv;            // The expected result in TSV format.
  std::string resultCsv;            // The expected result in CSV format
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSOn format.
                                    // Note: this member only contains the inner
                                    // result array with the bindings and NOT
                                    // the metadata.
  nlohmann::json resultSparqlJSON;  // The expected result in SparqlJSON format.
};

// Run a single test case for a select query.
void runSelectQueryTestCase(
    const TestCaseSelectQuery& testCase,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l, "runSelectQueryTestCase");
  using enum ad_utility::MediaType;
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, tsv),
            testCase.resultTsv);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, csv),
            testCase.resultCsv);
  auto qleverJSONResult = runJSONQuery(testCase.kg, testCase.query, qleverJson);
  ASSERT_EQ(qleverJSONResult["query"], testCase.query);
  ASSERT_EQ(qleverJSONResult["resultsize"], testCase.resultSize);
  EXPECT_EQ(qleverJSONResult["res"], testCase.resultQLeverJSON);

  auto sparqlJSONResult = runJSONQuery(testCase.kg, testCase.query, sparqlJson);
  EXPECT_EQ(sparqlJSONResult, testCase.resultSparqlJSON);
}

// Create a `json` that can be used as the `resultQLeverJSON` of a
// `TestCaseSelectQuery`. This function can only be used when there is a single
// variable in the result. The `values` then become the bindings of that
// variable.
nlohmann::json makeExpectedQLeverJSON(const std::vector<std::string>& values) {
  nlohmann::json j;
  for (const auto& value : values) {
    j.push_back(std::vector{value});
  }
  return j;
}

// Create a single binding in the `SparqlJSON` format from the given `datatype`
// `type`, `value` and `langtag`. `datatype` and `langtag` are not always
// present, so those arguments are `optional`.
nlohmann::json makeJSONBinding(
    const std::optional<std::string>& datatype, const std::string& type,
    const std::string& value,
    const std::optional<std::string>& langtag = std::nullopt) {
  std::unordered_map<std::string, std::string> m;
  if (datatype.has_value()) {
    m["datatype"] = datatype.value();
  }
  m["type"] = type;
  m["value"] = value;
  if (langtag.has_value()) {
    m["xml:lang"] = langtag.value();
  }
  return m;
}

// Create a `json` that can be used as the `resultSparqlJSON` member of a
// `TestCaseSelectQuery`. This function can only be used when there is a single
// variable called `?o` in the result. The `bindings` then become the bindings
// of that variable. These bindings typically are created via the
// `makeJSONBinding` function.
nlohmann::json makeExpectedSparqlJSON(
    const std::vector<nlohmann::json>& bindings) {
  nlohmann::json j;
  j["head"]["vars"].push_back("o");
  auto& res = j["results"]["bindings"];
  for (const auto& binding : bindings) {
    res.emplace_back();
    res.back()["o"] = binding;
  }
  return j;
}

TEST(ExportQueryExecutionTree, Integers) {
  std::string kg =
      "<s> <p> 42 . <s> <p> -42019234865781 . <s> <p> 4012934858173560";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCase{
      kg,
      query,
      3,
      // TODO<joka921> the ORDER BY of negative numbers is incorrect.
      "?o\n42\n4012934858173560\n-42019234865781\n",
      "?o\n42\n4012934858173560\n-42019234865781\n",
      makeExpectedQLeverJSON(
          {"\"42\"^^<http://www.w3.org/2001/XMLSchema#int>"s,
           "\"4012934858173560\"^^<http://www.w3.org/2001/XMLSchema#int>"s,
           "\"-42019234865781\"^^<http://www.w3.org/2001/XMLSchema#int>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "42"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "4012934858173560"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "-42019234865781")}),
  };
  runSelectQueryTestCase(testCase);
}

TEST(ExportQueryExecutionTree, Floats) {
  std::string kg =
      "<s> <p> 42.2 . <s> <p> -42019234865.781e12 . <s> <p> "
      "4.012934858173560e-12";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCaseFloat{
      kg,
      query,
      3,
      // TODO<joka921> The sorting is wrong, and the formatting of the negative
      // number is strange.
      "?o\n4.01293e-12\n42.2\n-42019234865780982022144\n",
      "?o\n4.01293e-12\n42.2\n-42019234865780982022144\n",
      makeExpectedQLeverJSON(
          {"\"4.01293e-12\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"42.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"-42019234865780982022144\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "4.01293e-12"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "42.2"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "-42019234865780982022144")}),
  };
  runSelectQueryTestCase(testCaseFloat);
}

TEST(ExportQueryExecutionTree, Dates) {
  std::string kg =
      "<s> <p> "
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCase{
      kg,
      query,
      1,
      "?o\n\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/"
      "XMLSchema#dateTime>\n",
      // Note: the duplicate quotes are due to the escaping for CSV.
      "?o\n\"\"\"1950-01-01T00:00:00\"\"^^<http://www.w3.org/2001/"
      "XMLSchema#dateTime>\"\n",
      makeExpectedQLeverJSON(
          {"\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#dateTime",
                           "literal", "1950-01-01T00:00:00")}),
  };
  runSelectQueryTestCase(testCase);
}

TEST(ExportQueryExecutionTree, Entities) {
  std::string kg = "PREFIX qlever: <http://qlever.com/> \n <s> <p> qlever:o";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCase{
      kg,
      query,
      1,
      "?o\n<http://qlever.com/o>\n",
      "?o\n<http://qlever.com/o>\n",
      makeExpectedQLeverJSON({"<http://qlever.com/o>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "uri", "http://qlever.com/o")}),
  };
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "PREFIX qlever: <http://qlever.com/> \n SELECT ?o WHERE {VALUES ?o "
      "{qlever:o}} ORDER BY ?o";
  runSelectQueryTestCase(testCase);
}

TEST(ExportQueryExecutionTree, LiteralWithLanguageTag) {
  std::string kg = "<s> <p> \"\"\"Some\"Where\tOver,\"\"\"@en-ca.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  TestCaseSelectQuery testCase{
      kg,
      query,
      1,
      "?o\n\"Some\"Where Over,\"@en-ca\n",
      "?o\n\"\"\"Some\"\"Where\tOver,\"\"@en-ca\"\n",
      makeExpectedQLeverJSON({"\"Some\"Where\tOver,\"@en-ca"s}),
      makeExpectedSparqlJSON({makeJSONBinding(std::nullopt, "literal",
                                              "Some\"Where\tOver,", "en-ca")})};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "SELECT ?o WHERE { VALUES ?o {\"\"\"Some\"Where\tOver,\"\"\"@en-ca}} "
      "ORDER BY ?o";
  runSelectQueryTestCase(testCase);
}

TEST(ExportQueryExecutionTree, UndefinedValues) {
  std::string kg = "<s> <p> <o>";
  std::string query =
      "SELECT ?o WHERE {?s <p> <o> OPTIONAL {?s <p2> ?o}} ORDER BY ?o";
  TestCaseSelectQuery testCase{
      kg,
      query,
      1,
      "?o\n\n",
      "?o\n\n",
      nlohmann::json{std::vector{std::vector{nullptr}}},
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        j["results"]["bindings"].push_back(nullptr);
        return j;
      }()};
  runSelectQueryTestCase(testCase);
}

TEST(ExportQueryExecutionTree, BlankNode) {
  std::string kg = "<s> <p> _:blank";
  std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o } ORDER BY ?o";
  TestCaseSelectQuery testCaseBlankNode{
      kg,
      objectQuery,
      1,
      "?o\n_:u_blank\n",
      "?o\n_:u_blank\n",
      makeExpectedQLeverJSON({"_:u_blank"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "bnode", "u_blank")})};
  runSelectQueryTestCase(testCaseBlankNode);
  // Note: Blank nodes cannot be introduced in a VALUES clause, so there is
  // nothing to test here.
}

TEST(ExportQueryExecutionTree, MultipleVariables) {
  // TODO<joka921> When we use the "ususal" `<s> <p> <o>` knowledge graph, then
  // it gets reused from the above `Undefined` unit test which currently leads
  // to wrong results. I currently suspect that this is a bug inside the
  // `getQec` function, but I have yet to track it. As soon as this is done, we
  // can switch back here.
  std::string kg = "<x> <y> <z>";
  std::string objectQuery = "SELECT ?p ?o WHERE {<x> ?p ?o } ORDER BY ?p ?o";
  TestCaseSelectQuery testCaseBlankNode{
      kg,
      objectQuery,
      1,
      "?p\t?o\n<y>\t<z>\n",
      "?p,?o\n<y>,<z>\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<y>"s, "<z>"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("p");
        j["head"]["vars"].push_back("o");
        auto& bindings = j["results"]["bindings"];
        bindings.emplace_back();
        bindings.back()["p"] = makeJSONBinding(std::nullopt, "uri", "y");
        bindings.back()["o"] = makeJSONBinding(std::nullopt, "uri", "z");
        return j;
      }()};
  runSelectQueryTestCase(testCaseBlankNode);
  // Note: Blank nodes cannot be introduced in a VALUES clause, so there is
  // nothing to test here.
}

// TODO<joka921> There are tests missing for the CONSTRUCT query export, but
// that module has to be restructured anyway.