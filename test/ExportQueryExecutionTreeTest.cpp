//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
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
      ExportQueryExecutionTrees::computeResultAsStream(pq, qet, mediaType);
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
  ad_utility::Timer timer{ad_utility::Timer::Started};
  return ExportQueryExecutionTrees::computeResultAsJSON(pq, qet, timer, 200,
                                                        mediaType);
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
  std::string resultXml;
};

struct TestCaseConstructQuery {
  std::string kg;                   // The knowledge graph (TURTLE)
  std::string query;                // The query (SPARQL)
  size_t resultSize;                // The expected number of results.
  std::string resultTsv;            // The expected result in TSV format.
  std::string resultCsv;            // The expected result in CSV format
  std::string resultTurtle;         // The expected result in Turtle format
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSOn format.
                                    // Note: this member only contains the inner
                                    // result array with the bindings and NOT
                                    // the metadata.
};

// Run a single test case for a SELECT query.
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
  // TODO<joka921> Test other members of the JSON result (e.g. the selected
  // variables).
  ASSERT_EQ(qleverJSONResult["query"], testCase.query);
  ASSERT_EQ(qleverJSONResult["resultsize"], testCase.resultSize);
  EXPECT_EQ(qleverJSONResult["res"], testCase.resultQLeverJSON);

  auto sparqlJSONResult = runJSONQuery(testCase.kg, testCase.query, sparqlJson);
  EXPECT_EQ(sparqlJSONResult, testCase.resultSparqlJSON);

  // TODO<joka921> Use this for proper testing etc.
  auto xmlAsString =
      runQueryStreamableResult(testCase.kg, testCase.query, sparqlXml);
  EXPECT_EQ(testCase.resultXml, xmlAsString);
}

// Run a single test case for a CONSTRUCT query.
void runConstructQueryTestCase(
    const TestCaseConstructQuery& testCase,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l, "runConstructQueryTestCase");
  using enum ad_utility::MediaType;
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, tsv),
            testCase.resultTsv);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, csv),
            testCase.resultCsv);
  auto qleverJSONResult = runJSONQuery(testCase.kg, testCase.query, qleverJson);
  ASSERT_EQ(qleverJSONResult["query"], testCase.query);
  ASSERT_EQ(qleverJSONResult["resultsize"], testCase.resultSize);
  EXPECT_EQ(qleverJSONResult["res"], testCase.resultQLeverJSON);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, turtle),
            testCase.resultTurtle);
}

// Create a `json` that can be used as the `resultQLeverJSON` of a
// `TestCaseSelectQuery`. This function can only be used when there is a single
// variable in the result. The `values` then become the bindings of that
// variable.
nlohmann::json makeExpectedQLeverJSON(
    const std::vector<std::optional<std::string>>& values) {
  nlohmann::json j;
  for (const auto& value : values) {
    if (value.has_value()) {
      j.push_back(std::vector{value.value()});
    } else {
      j.emplace_back();
      j.back().push_back(nullptr);
    }
  }
  return j;
}

// Create a single binding in the `SparqlJSON` format from the given `datatype`
// `type`, `value` and `langtag`. `datatype` and `langtag` are not always
// present, so those arguments are of type `std::optional`.
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
// of that variable. These bindings are typically created via the
// `makeJSONBinding` function.
nlohmann::json makeExpectedSparqlJSON(
    const std::vector<nlohmann::json>& bindings) {
  nlohmann::json j;
  j["head"]["vars"].push_back("o");
  auto& res = j["results"]["bindings"];
  res = std::vector<std::string>{};
  for (const auto& binding : bindings) {
    res.emplace_back();
    res.back()["o"] = binding;
  }
  return j;
}

// Return a header of a SPARQL XML export including the given variables until
// the opening `<results>` tag.
static std::string makeXMLHeader(
    std::vector<std::string> varsWithoutQuestionMark) {
  std::string result = R"(<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
<head>)";
  for (const auto& var : varsWithoutQuestionMark) {
    absl::StrAppend(&result, "\n  <variable name=\"", var, R"("/>)");
  }
  absl::StrAppend(&result, "\n</head>\n<results>");
  return result;
}

// The end of a SPARQL XML export.
static const std::string xmlTrailer = "\n</results>\n</sparql>";

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, Integers) {
  std::string kg =
      "<s> <p> 42 . <s> <p> -42019234865781 . <s> <p> 4012934858173560";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#int">-42019234865781</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#int">42</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#int">4012934858173560</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 3,
      // TSV
      "?o\n"
      "-42019234865781\n"
      "42\n"
      "4012934858173560\n",
      // CSV
      "o\n"
      "-42019234865781\n"
      "42\n"
      "4012934858173560\n",
      makeExpectedQLeverJSON(
          {"\"-42019234865781\"^^<http://www.w3.org/2001/XMLSchema#int>"s,
           "\"42\"^^<http://www.w3.org/2001/XMLSchema#int>"s,
           "\"4012934858173560\"^^<http://www.w3.org/2001/XMLSchema#int>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "-42019234865781"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "42"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#int", "literal",
                           "4012934858173560")}),
      expectedXml};
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 3,
      // TSV
      "<s>\t<p>\t-42019234865781\n"
      "<s>\t<p>\t42\n"
      "<s>\t<p>\t4012934858173560\n",
      // CSV
      "<s>,<p>,-42019234865781\n"
      "<s>,<p>,42\n"
      "<s>,<p>,4012934858173560\n",
      // Turtle
      "<s> <p> -42019234865781 .\n"
      "<s> <p> 42 .\n"
      "<s> <p> 4012934858173560 .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "-42019234865781"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "42"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "4012934858173560"s});
        return j;
      }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, Bool) {
  std::string kg = "<s> <p> true . <s> <p> false.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">false</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">true</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 2,
      // TSV
      "?o\n"
      "false\n"
      "true\n",
      // CSV
      "o\n"
      "false\n"
      "true\n",
      makeExpectedQLeverJSON(
          {"\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s,
           "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "false"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "true")}),
      expectedXml};
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 2,
      // TSV
      "<s>\t<p>\tfalse\n"
      "<s>\t<p>\ttrue\n",
      // CSV
      "<s>,<p>,false\n"
      "<s>,<p>,true\n",
      // Turtle
      "<s> <p> false .\n"
      "<s> <p> true .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "false"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "true"s});
        return j;
      }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, UnusedVariable) {
  std::string kg = "<s> <p> true . <s> <p> false.";
  std::string query = "SELECT ?o WHERE {?s ?p ?x} ORDER BY ?s";
  std::string expectedXml = makeXMLHeader({"o"}) + R"(
  <result>
  </result>
  <result>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 2,
      // TSV
      "?o\n"
      "\n"
      "\n",
      // CSV
      "o\n"
      "\n"
      "\n",
      makeExpectedQLeverJSON({std::nullopt, std::nullopt}),
      makeExpectedSparqlJSON({}), expectedXml};
  runSelectQueryTestCase(testCase);

  // If we use a variable that is always unbound in a CONSTRUCT triple, then
  // the result for this triple will be empty.
  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?x ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 0,
      // TSV
      "",
      // CSV
      "",
      // Turtle
      "", []() { return nlohmann::json::parse("[]"); }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, Floats) {
  std::string kg =
      "<s> <p> 42.2 . <s> <p> -42019234865.781e12 . <s> <p> "
      "4.012934858173560e-12";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">-42019234865780982022144</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">4.01293e-12</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">42.2</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseFloat{
      kg, query, 3,
      // TSV
      "?o\n"
      "-42019234865780982022144\n"
      "4.01293e-12\n"
      "42.2\n",
      // CSV
      "o\n"
      "-42019234865780982022144\n"
      "4.01293e-12\n"
      "42.2\n",
      makeExpectedQLeverJSON(
          {"\"-42019234865780982022144\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"4.01293e-12\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"42.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "-42019234865780982022144"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "4.01293e-12"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "42.2")}),
      expectedXml};
  runSelectQueryTestCase(testCaseFloat);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 3,
      // TSV
      "<s>\t<p>\t-42019234865780982022144\n"
      "<s>\t<p>\t4.01293e-12\n"
      "<s>\t<p>\t42.2\n",
      // CSV
      "<s>,<p>,-42019234865780982022144\n"
      "<s>,<p>,4.01293e-12\n"
      "<s>,<p>,42.2\n",
      // Turtle
      "<s> <p> -42019234865780982022144 .\n"
      "<s> <p> 4.01293e-12 .\n"
      "<s> <p> 42.2 .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "-42019234865780982022144"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "4.01293e-12"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "42.2"s});
        return j;
      }()};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, Dates) {
  std::string kg =
      "<s> <p> "
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#dateTime">1950-01-01T00:00:00</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "1950-01-01T00:00:00\n",
      // should be
      // "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>\n",
      // but that is a bug in the TSV export for another PR. Note: the duplicate
      // quotes are due to the escaping for CSV.
      "o\n"
      "1950-01-01T00:00:00\n",
      makeExpectedQLeverJSON(
          {"\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#dateTime",
                           "literal", "1950-01-01T00:00:00")}),
      expectedXml};
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/"
      "XMLSchema#dateTime>\n",  // missing
                                // "^^<http://www.w3.org/2001/XMLSchema#dateTime>\n",
      // CSV
      // TODO<joka921> This format is wrong, but this is is due to the way that
      // CONSTRUCT queries are currently exported. This has to be fixed in a
      // different PR.
      "<s>,<p>,\"\"\"1950-01-01T00:00:00\"\"^^<http://www.w3.org/2001/"
      "XMLSchema#dateTime>\"\n",
      // Turtle
      "<s> <p> "
      "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> "
      ".\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{
            "<s>"s, "<p>"s,
            "\"1950-01-01T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, Entities) {
  std::string kg = "PREFIX qlever: <http://qlever.com/> \n <s> <p> qlever:o";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><uri>http://qlever.com/o</uri></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "<http://qlever.com/o>\n",
      // CSV
      "o\n"
      "http://qlever.com/o\n",
      makeExpectedQLeverJSON({"<http://qlever.com/o>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "uri", "http://qlever.com/o")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "PREFIX qlever: <http://qlever.com/> \n SELECT ?o WHERE {VALUES ?o "
      "{qlever:o}} ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t<http://qlever.com/o>\n",
      // CSV
      "<s>,<p>,<http://qlever.com/o>\n",
      // Turtle
      "<s> <p> <http://qlever.com/o> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "<http://qlever.com/o>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, LiteralWithLanguageTag) {
  std::string kg = "<s> <p> \"\"\"Some\"Where\tOver,\"\"\"@en-ca.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal xml:lang="en-ca">Some&quot;Where)" +
                            "\t" + R"(Over,</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "\"Some\"Where Over,\"@en-ca\n",
      // CSV
      "o\n"
      "\"Some\"\"Where\tOver,\"\n",
      makeExpectedQLeverJSON({"\"Some\"Where\tOver,\"@en-ca"s}),
      makeExpectedSparqlJSON({makeJSONBinding(std::nullopt, "literal",
                                              "Some\"Where\tOver,", "en-ca")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "SELECT ?o WHERE { VALUES ?o {\"\"\"Some\"Where\tOver,\"\"\"@en-ca}} "
      "ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t\"Some\"Where Over,\"@en-ca\n",
      // CSV
      "<s>,<p>,\"\"\"Some\"\"Where\tOver,\"\"@en-ca\"\n",
      // Turtle
      "<s> <p> \"Some\\\"Where\tOver,\"@en-ca .\n",
      []() {
        nlohmann::json j;
        j.push_back(
            std::vector{"<s>"s, "<p>"s, "\"Some\"Where\tOver,\"@en-ca"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, LiteralWithDatatype) {
  std::string kg = "<s> <p> \"something\"^^<www.example.org/bim>";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="www.example.org/bim">something</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "\"something\"^^<www.example.org/bim>\n",
      // CSV
      "o\n"
      "something\n",
      makeExpectedQLeverJSON({"\"something\"^^<www.example.org/bim>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("www.example.org/bim", "literal", "something")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "SELECT ?o WHERE { VALUES ?o {\"something\"^^<www.example.org/bim>}} "
      "ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t\"something\"^^<www.example.org/bim>\n",
      // CSV
      "<s>,<p>,\"\"\"something\"\"^^<www.example.org/bim>\"\n",
      // Turtle
      "<s> <p> \"something\"^^<www.example.org/bim> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s,
                                "\"something\"^^<www.example.org/bim>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, UndefinedValues) {
  std::string kg = "<s> <p> <o>";
  std::string query =
      "SELECT ?o WHERE {?s <p> <o> OPTIONAL {?s <p2> ?o}} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg,
      query,
      1,
      "?o\n\n",
      "o\n\n",
      nlohmann::json{std::vector{std::vector{nullptr}}},
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        j["results"]["bindings"].push_back(nullptr);
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCase);

  // In CONSTRUCT queries, results with undefined values in the exported
  // variables are filtered out, so the result is empty.
  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s <pred> ?o} WHERE {?s <p> <o> OPTIONAL {?s <p2> ?o}} ORDER "
      "BY ?o",
      0,
      "",
      "",
      "",
      std::vector<std::string>{}};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, BlankNode) {
  std::string kg = "<s> <p> _:blank";
  std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o } ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><bnode>u_blank</bnode></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseBlankNode{kg, objectQuery, 1,
                                        // TSV
                                        "?o\n"
                                        "_:u_blank\n",
                                        // CSV
                                        "o\n"
                                        "_:u_blank\n",
                                        makeExpectedQLeverJSON({"_:u_blank"s}),
                                        makeExpectedSparqlJSON({makeJSONBinding(
                                            std::nullopt, "bnode", "u_blank")}),
                                        expectedXml};
  runSelectQueryTestCase(testCaseBlankNode);
  // Note: Blank nodes cannot be introduced in a `VALUES` clause, so they can
  // never be part of the local vocabulary. For this reason we don't need a
  // `VALUES` clause in the test query like in the test cases above.
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, MultipleVariables) {
  std::string kg = "<s> <p> <o>";
  std::string objectQuery = "SELECT ?p ?o WHERE {<s> ?p ?o } ORDER BY ?p ?o";
  std::string expectedXml = makeXMLHeader({"p", "o"}) +
                            R"(
  <result>
    <binding name="p"><uri>p</uri></binding>
    <binding name="o"><uri>o</uri></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseMultipleVariables{
      kg, objectQuery, 1,
      // TSV
      "?p\t?o\n"
      "<p>\t<o>\n",
      // CSV
      "p,o\n"
      "p,o\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<p>"s, "<o>"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("p");
        j["head"]["vars"].push_back("o");
        auto& bindings = j["results"]["bindings"];
        bindings.emplace_back();
        bindings.back()["p"] = makeJSONBinding(std::nullopt, "uri", "p");
        bindings.back()["o"] = makeJSONBinding(std::nullopt, "uri", "o");
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCaseMultipleVariables);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, BinaryExport) {
  std::string kg = "<s> <p> 31 . <s> <o> 42";
  std::string query = "SELECT ?p ?o WHERE {<s> ?p ?o } ORDER BY ?p ?o";
  std::string result =
      runQueryStreamableResult(kg, query, ad_utility::MediaType::octetStream);
  ASSERT_EQ(4 * sizeof(Id), result.size());
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  auto p = getId("<p>");
  auto o = getId("<o>");

  Id id0, id1, id2, id3;
  // TODO<joka921, C++20> Use `std::bit_cast`
  std::memcpy(&id0, result.data(), sizeof(Id));
  std::memcpy(&id1, result.data() + sizeof(Id), sizeof(Id));
  std::memcpy(&id2, result.data() + 2 * sizeof(Id), sizeof(Id));
  std::memcpy(&id3, result.data() + 3 * sizeof(Id), sizeof(Id));

  // The result is "p, 31" (first row) "o, 42" (second row)
  ASSERT_EQ(o, id0);
  ASSERT_EQ(ad_utility::testing::IntId(42), id1);
  ASSERT_EQ(p, id2);
  ASSERT_EQ(ad_utility::testing::IntId(31), id3);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTree, CornerCases) {
  std::string kg = "<s> <p> <o>";
  std::string query = "SELECT ?p ?o WHERE {<s> ?p ?o } ORDER BY ?p ?o";
  std::string constructQuery =
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o } ORDER BY ?p ?o";

  // JSON is not streamable.
  ASSERT_THROW(
      runQueryStreamableResult(kg, query, ad_utility::MediaType::qleverJson),
      ad_utility::Exception);
  // Turtle is not supported for SELECT queries.
  ASSERT_THROW(
      runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
      ad_utility::Exception);
  // TSV is not a `JSON` format
  ASSERT_THROW(runJSONQuery(kg, query, ad_utility::MediaType::tsv),
               ad_utility::Exception);
  // SPARQL JSON is not supported for construct queries.
  ASSERT_THROW(
      runJSONQuery(kg, constructQuery, ad_utility::MediaType::sparqlJson),
      ad_utility::Exception);
  // XML is currently not supported for construct queries.
  AD_EXPECT_THROW_WITH_MESSAGE(
      runQueryStreamableResult(kg, constructQuery,
                               ad_utility::MediaType::sparqlXml),
      ::testing::ContainsRegex(
          "XML export is currently not supported for CONSTRUCT"));

  // Binary export is not supported for CONSTRUCT queries.
  ASSERT_THROW(runQueryStreamableResult(kg, constructQuery,
                                        ad_utility::MediaType::octetStream),
               ad_utility::Exception);

  // A SparqlJSON query where none of the variables is even visible in the
  // query body is not supported.
  std::string queryNoVariablesVisible = "SELECT ?not ?known WHERE {<s> ?p ?o}";
  auto resultNoColumns = runJSONQuery(kg, queryNoVariablesVisible,
                                      ad_utility::MediaType::sparqlJson);
  ASSERT_TRUE(resultNoColumns["result"]["bindings"].empty());

  auto qec = ad_utility::testing::getQec(kg);
  AD_EXPECT_THROW_WITH_MESSAGE(
      ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), Id::max(),
                                                   LocalVocab{}),
      ::testing::ContainsRegex("should be unreachable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ExportQueryExecutionTrees::idToStringAndTypeForEncodedValue(
          ad_utility::testing::VocabId(12)),
      ::testing::ContainsRegex("should be unreachable"));
}

// TODO<joka921> Unit tests for the more complex CONSTRUCT export (combination
// between constants and stuff from the knowledge graph).

// TODO<joka921> Unit tests that also test for the export of text records from
// the text index and thus systematically fill the coverage gaps.
