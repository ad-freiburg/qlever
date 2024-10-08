//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryPlanner.h"
#include "parser/SparqlParser.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParseableDuration.h"

using namespace std::string_literals;
using namespace std::chrono_literals;
using ::testing::ElementsAre;
using ::testing::EndsWith;
using ::testing::Eq;
using ::testing::HasSubstr;

namespace {
// Run the given SPARQL `query` on the given Turtle `kg` and export the result
// as the `mediaType`. `mediaType` must be TSV or CSV.
std::string runQueryStreamableResult(const std::string& kg,
                                     const std::string& query,
                                     ad_utility::MediaType mediaType,
                                     bool useTextIndex = false) {
  auto qec =
      ad_utility::testing::getQec(kg, true, true, true, 16_B, useTextIndex);
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer(ad_utility::Timer::Started);
  auto strGenerator = ExportQueryExecutionTrees::computeResult(
      pq, qet, mediaType, timer, std::move(cancellationHandle));

  std::string result;
  for (const auto& block : strGenerator) {
    result += block;
  }
  return result;
}

// Run the given SPARQL `query` on the given Turtle `kg` and export the result
// as JSON. `mediaType` must be `sparqlJSON` or `qleverJSON`.
nlohmann::json runJSONQuery(const std::string& kg, const std::string& query,
                            ad_utility::MediaType mediaType,
                            bool useTextIndex = false) {
  auto qec =
      ad_utility::testing::getQec(kg, true, true, true, 16_B, useTextIndex);
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = SparqlParser::parseQuery(query);
  auto qet = qp.createExecutionTree(pq);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string resStr;
  for (auto c : ExportQueryExecutionTrees::computeResult(
           pq, qet, mediaType, timer, std::move(cancellationHandle))) {
    resStr += c;
  }
  return nlohmann::json::parse(resStr);
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
    const TestCaseSelectQuery& testCase, bool useTextIndex = false,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l, "runSelectQueryTestCase");
  using enum ad_utility::MediaType;
  EXPECT_EQ(
      runQueryStreamableResult(testCase.kg, testCase.query, tsv, useTextIndex),
      testCase.resultTsv);
  EXPECT_EQ(
      runQueryStreamableResult(testCase.kg, testCase.query, csv, useTextIndex),
      testCase.resultCsv);

  auto qleverJSONResult = nlohmann::json::parse(runQueryStreamableResult(
      testCase.kg, testCase.query, qleverJson, useTextIndex));
  // TODO<joka921> Test other members of the JSON result (e.g. the selected
  // variables).
  ASSERT_EQ(qleverJSONResult["query"], testCase.query);
  ASSERT_EQ(qleverJSONResult["resultsize"], testCase.resultSize);
  EXPECT_EQ(qleverJSONResult["res"], testCase.resultQLeverJSON);

  EXPECT_EQ(nlohmann::json::parse(runQueryStreamableResult(
                testCase.kg, testCase.query, sparqlJson, useTextIndex)),
            testCase.resultSparqlJSON);

  // TODO<joka921> Use this for proper testing etc.
  auto xmlAsString = runQueryStreamableResult(testCase.kg, testCase.query,
                                              sparqlXml, useTextIndex);
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
  auto qleverJSONStreamResult = nlohmann::json::parse(
      runQueryStreamableResult(testCase.kg, testCase.query, qleverJson));
  ASSERT_EQ(qleverJSONStreamResult["query"], testCase.query);
  ASSERT_EQ(qleverJSONStreamResult["resultsize"], testCase.resultSize);
  EXPECT_EQ(qleverJSONStreamResult["res"], testCase.resultQLeverJSON);
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

// Helper function for easier testing of the `IdTable` generator.
std::vector<IdTable> convertToVector(
    cppcoro::generator<const IdTable&> generator) {
  std::vector<IdTable> result;
  for (const IdTable& idTable : generator) {
    result.push_back(idTable.clone());
  }
  return result;
}

// match the contents of a `vector<IdTable>` to the given `tables`.
auto matchesIdTables(const auto&... tables) {
  return ElementsAre(matchesIdTable(tables)...);
}

// Template is only required because inner class is not visible
template <typename T>
std::vector<IdTable> convertToVector(cppcoro::generator<T> generator) {
  std::vector<IdTable> result;
  for (const auto& [idTable, range] : generator) {
    result.emplace_back(idTable.numColumns(), idTable.getAllocator());
    result.back().insertAtEnd(idTable.begin() + *range.begin(),
                              idTable.begin() + *(range.end() - 1) + 1);
  }
  return result;
}

std::chrono::milliseconds toChrono(std::string_view string) {
  EXPECT_THAT(string, EndsWith("ms"));
  return ad_utility::ParseableDuration<std::chrono::milliseconds>::fromString(
      string);
}
}  // namespace

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, Integers) {
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
TEST(ExportQueryExecutionTrees, Bool) {
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
TEST(ExportQueryExecutionTrees, UnusedVariable) {
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
TEST(ExportQueryExecutionTrees, Floats) {
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
TEST(ExportQueryExecutionTrees, Dates) {
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
TEST(ExportQueryExecutionTrees, GeoPoints) {
  std::string kg =
      "<s> <p> "
      "\"POINT(50.0 "
      "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>.";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.opengis.net/ont/geosparql#wktLiteral">POINT(50.000000 50.000000)</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 1,
      // TSV
      "?o\n"
      "POINT(50.000000 50.000000)\n",
      // should be
      // "\"POINT(50.000000 50.000000)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>\n",
      // but that is a bug in the TSV export for another PR. Note: the duplicate
      // quotes are due to the escaping for CSV.
      "o\n"
      "POINT(50.000000 50.000000)\n",
      makeExpectedQLeverJSON(
          {"\"POINT(50.000000 50.000000)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.opengis.net/ont/geosparql#wktLiteral",
                           "literal", "POINT(50.000000 50.000000)")}),
      expectedXml};
  runSelectQueryTestCase(testCase);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, Entities) {
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
TEST(ExportQueryExecutionTrees, LiteralWithLanguageTag) {
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
TEST(ExportQueryExecutionTrees, LiteralWithDatatype) {
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
TEST(ExportQueryExecutionTrees, LiteralPlain) {
  std::string kg = "<s> <p> \"something\"";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal>something</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{kg, query, 1,
                               // TSV
                               "?o\n"
                               "\"something\"\n",
                               // CSV
                               "o\n"
                               "something\n",
                               makeExpectedQLeverJSON({"\"something\""s}),
                               makeExpectedSparqlJSON({makeJSONBinding(
                                   std::nullopt, "literal", "something")}),
                               expectedXml};
  runSelectQueryTestCase(testCase);
  testCase.kg = "<s> <x> <y>";
  testCase.query =
      "SELECT ?o WHERE { VALUES ?o {\"something\"}} "
      "ORDER BY ?o";
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t\"something\"\n",
      // CSV
      "<s>,<p>,\"\"\"something\"\"\"\n",
      // Turtle
      "<s> <p> \"something\" .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "\"something\""s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, TestWithIriEscaped) {
  std::string kg = "<s> <p> <https://\\u0009:\\u0020)\\u000AtestIriKg>";
  std::string objectQuery = "SELECT ?o WHERE { ?s ?p ?o }";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><uri>https://)" +
                            "\x09" + R"(: )
testIriKg</uri></binding>
  </result>)" + xmlTrailer;

  TestCaseSelectQuery testCaseTextIndex{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "<https:// : )\\ntestIriKg>\n",
      // CSV
      "o\n"
      "\"https://\t: )\ntestIriKg\"\n",
      makeExpectedQLeverJSON({"<https://\t: )\ntestIriKg>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "uri", "https://\t: )\ntestIriKg")}),
      expectedXml};
  runSelectQueryTestCase(testCaseTextIndex);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t<https:// : )\\ntestIriKg>\n",
      // CSV
      "<s>,<p>,\"<https://\t: )\ntestIriKg>\"\n",
      // Turtle
      "<s> <p> <https://\t: )\ntestIriKg> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "<https://\t: )\ntestIriKg>"s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

TEST(ExportQueryExecutionTrees, TestWithIriExtendedEscaped) {
  std::string kg =
      "<s> <p>"
      "<iriescaped\\u0001o\\u0002e\\u0003i\\u0004o\\u0005u\\u0006e\\u00"
      "07g\\u0008c\\u0009u\\u000Ae\\u000Be\\u000Ca\\u000Dd\\u000En\\u000F?"
      "\\u0010u\\u0011u\\u0012u\\u0013###\\u0020d>";
  std::string objectQuery = "SELECT ?o WHERE { ?s ?p ?o }";
  std::string expectedXml =
      makeXMLHeader({"o"}) +
      R"(
  <result>
    <binding name="o"><uri>)" +
      "iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d" +
      R"(</uri></binding>
  </result>)" +
      xmlTrailer;

  TestCaseSelectQuery testCaseTextIndex{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "<iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc u\\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>\n",
      // CSV
      "o\n"
      "\"iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d\"\n",
      makeExpectedQLeverJSON(
          {"<iriescaped\u0001o\u0002e\u0003i\u0004o\u0005u\u0006e\u0007"
           "g\u0008c\u0009u\u000Ae\u000Be\u000Ca\u000Dd\u000En\u000F?"
           "\u0010u\u0011u\u0012u\u0013### d>"s}),
      makeExpectedSparqlJSON({makeJSONBinding(
          std::nullopt, "uri",
          "iriescaped\u0001o\u0002e\u0003i\u0004o\u0005u\u0006e"
          "\u0007"
          "g\u0008c\u0009u\u000Ae\u000Be\u000Ca\u000Dd\u000En\u000F?"
          "\u0010u\u0011u\u0012u\u0013### d")}),
      expectedXml};
  runSelectQueryTestCase(testCaseTextIndex);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t<iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc u\\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>\n",
      // CSV
      "<s>,<p>,\"<iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>\"\n",
      // Turtle
      "<s> <p> <iriescaped\x01o\x02"
      "e\x03i\x04o\x05u\x06"
      "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{
            "<s>"s, "<p>"s,
            "<iriescaped\x01o\x02"
            "e\x03i\x04o\x05u\x06"
            "e\ag\bc\tu\ne\ve\fa\rd\x0En\x0F?\x10u\x11u\x12u\x13### d>"s});
        return j;
      }(),
  };
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, TestIriWithEscapedIriString) {
  std::string kg = "<s> <p> \" hallo\\n\\t welt\"";
  std::string objectQuery =
      "SELECT ?o WHERE { "
      "BIND(IRI(\" hallo\\n\\t welt\") AS ?o) }";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><uri> hallo
)" + "\t" + R"( welt</uri></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseTextIndex{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "< hallo\\n  welt>\n",
      // CSV
      "o\n"
      "\" hallo\n\t welt\"\n",
      makeExpectedQLeverJSON({"< hallo\n\t welt>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding(std::nullopt, "uri", " hallo\n\t welt")}),
      expectedXml};
  runSelectQueryTestCase(testCaseTextIndex);

  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o",
      1,
      // TSV
      "<s>\t<p>\t\" hallo\\n  welt\"\n",
      // CSV
      "<s>,<p>,\"\"\" hallo\n\t welt\"\"\"\n",
      // Turtle
      "<s> <p> \" hallo\\n\t welt\" .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "\" hallo\n\t welt\""s});
        return j;
      }(),
  };
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, UndefinedValues) {
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
TEST(ExportQueryExecutionTrees, BlankNode) {
  std::string kg = "<s> <p> _:blank";
  std::string objectQuery = "SELECT ?o WHERE {?s ?p ?o } ORDER BY ?o";
  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><bnode>bn0</bnode></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseBlankNode{
      kg, objectQuery, 1,
      // TSV
      "?o\n"
      "_:bn0\n",
      // CSV
      "o\n"
      "_:bn0\n",
      makeExpectedQLeverJSON({"_:bn0"s}),
      makeExpectedSparqlJSON({makeJSONBinding(std::nullopt, "bnode", "bn0")}),
      expectedXml};
  runSelectQueryTestCase(testCaseBlankNode);
  // Note: Blank nodes cannot be introduced in a `VALUES` clause, so they can
  // never be part of the local vocabulary. For this reason we don't need a
  // `VALUES` clause in the test query like in the test cases above.
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, TextIndex) {
  std::string kg = "<s> <p> \"alpha beta\". <s2> <p2> \"alphax betax\". ";
  std::string objectQuery =
      "SELECT ?o WHERE {<s> <p> ?t. ?text ql:contains-entity ?t .?text "
      "ql:contains-word \"alph*\" BIND (?ql_matchingword_text_alph AS ?o)}";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal>alpha</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseTextIndex{kg, objectQuery, 1,
                                        // TSV
                                        "?o\n"
                                        "alpha\n",
                                        // CSV
                                        "o\n"
                                        "alpha\n",
                                        makeExpectedQLeverJSON({"alpha"s}),
                                        makeExpectedSparqlJSON({makeJSONBinding(
                                            std::nullopt, "literal", "alpha")}),
                                        expectedXml};
  runSelectQueryTestCase(testCaseTextIndex, true);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, MultipleVariables) {
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
TEST(ExportQueryExecutionTrees, LimitOffset) {
  std::string kg = "<a> <b> <c> . <d> <e> <f> . <g> <h> <i> . <j> <k> <l>";
  std::string objectQuery =
      "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 2 OFFSET 1";
  std::string expectedXml = makeXMLHeader({"s"}) +
                            R"(
  <result>
    <binding name="s"><uri>d</uri></binding>
  </result>
  <result>
    <binding name="s"><uri>g</uri></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseLimitOffset{
      kg, objectQuery, 2,
      // TSV
      "?s\n"
      "<d>\n"
      "<g>\n",
      // CSV
      "s\n"
      "d\n"
      "g\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{
            "<d>"s,
        });
        j.push_back(std::vector{
            "<g>"s,
        });
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("s");
        auto& bindings = j["results"]["bindings"];
        bindings.emplace_back();
        bindings.back()["s"] = makeJSONBinding(std::nullopt, "uri", "d");
        bindings.emplace_back();
        bindings.back()["s"] = makeJSONBinding(std::nullopt, "uri", "g");
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCaseLimitOffset);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, BinaryExport) {
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
TEST(ExportQueryExecutionTrees, CornerCases) {
  std::string kg = "<s> <p> <o>";
  std::string query = "SELECT ?p ?o WHERE {<s> ?p ?o } ORDER BY ?p ?o";
  std::string constructQuery =
      "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o } ORDER BY ?p ?o";

  // Turtle is not supported for SELECT queries.
  ASSERT_THROW(
      runQueryStreamableResult(kg, query, ad_utility::MediaType::turtle),
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
  ASSERT_TRUE(resultNoColumns["results"]["bindings"].empty());

  auto qec = ad_utility::testing::getQec(kg);
  AD_EXPECT_THROW_WITH_MESSAGE(
      ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), Id::max(),
                                                   LocalVocab{}),
      ::testing::ContainsRegex("should be unreachable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
          qec->getIndex(), Id::max(), LocalVocab{}),
      ::testing::ContainsRegex("should be unreachable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ExportQueryExecutionTrees::idToStringAndTypeForEncodedValue(
          ad_utility::testing::VocabId(12)),
      ::testing::ContainsRegex("should be unreachable"));
}

using enum ad_utility::MediaType;

// ____________________________________________________________________________
class StreamableMediaTypesFixture
    : public ::testing::Test,
      public ::testing::WithParamInterface<ad_utility::MediaType> {};

TEST_P(StreamableMediaTypesFixture, CancellationCancelsStream) {
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  auto* qec = ad_utility::testing::getQec(
      "<s> <p> 42 . <s> <p> -42019234865781 . <s> <p> 4012934858173560");
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = SparqlParser::parseQuery(
      GetParam() == turtle ? "CONSTRUCT { ?x ?y ?z } WHERE { ?x ?y ?z }"
                           : "SELECT * WHERE { ?x ?y ?z }");
  auto qet = qp.createExecutionTree(pq);

  cancellationHandle->cancel(ad_utility::CancellationState::MANUAL);
  ad_utility::Timer timer(ad_utility::Timer::Started);
  EXPECT_ANY_THROW(([&]() {
    [[maybe_unused]] auto generator = ExportQueryExecutionTrees::computeResult(
        pq, qet, GetParam(), timer, std::move(cancellationHandle));
  }()));
}

INSTANTIATE_TEST_SUITE_P(StreamableMediaTypes, StreamableMediaTypesFixture,
                         ::testing::Values(turtle, sparqlXml, tsv, csv,
                                           octetStream, sparqlJson,
                                           qleverJson));

// TODO<joka921> Unit tests for the more complex CONSTRUCT export (combination
// between constants and stuff from the knowledge graph).

// TODO<joka921> Unit tests that also test for the export of text records from
// the text index and thus systematically fill the coverage gaps.

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, getIdTablesReturnsSingletonIterator) {
  auto idTable = makeIdTableFromVector({{42}, {1337}});

  Result result{idTable.clone(), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getIdTables(result);

  EXPECT_THAT(convertToVector(std::move(generator)), matchesIdTables(idTable));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, getIdTablesMirrorsGenerator) {
  IdTable idTable1 = makeIdTableFromVector({{1}, {2}, {3}});
  IdTable idTable2 = makeIdTableFromVector({{42}, {1337}});
  auto tableGenerator = [](IdTable idTableA,
                           IdTable idTableB) -> cppcoro::generator<IdTable> {
    co_yield idTableA;

    co_yield idTableB;
  }(idTable1.clone(), idTable2.clone());

  Result result{std::move(tableGenerator), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getIdTables(result);

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(idTable1, idTable2));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ensureCorrectSlicingOfSingleIdTable) {
  auto tableGenerator = []() -> cppcoro::generator<IdTable> {
    IdTable idTable1 = makeIdTableFromVector({{1}, {2}, {3}});
    co_yield idTable1;
  }();

  Result result{std::move(tableGenerator), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 1, ._offset = 1}, result);

  auto referenceTable = makeIdTableFromVector({{2}});
  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(referenceTable));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstIsSkipped) {
  auto tableGenerator = []() -> cppcoro::generator<IdTable> {
    IdTable idTable1 = makeIdTableFromVector({{1}, {2}, {3}});
    co_yield idTable1;

    IdTable idTable2 = makeIdTableFromVector({{4}, {5}});
    co_yield idTable2;
  }();

  Result result{std::move(tableGenerator), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = std::nullopt, ._offset = 3}, result);

  auto referenceTable1 = makeIdTableFromVector({{4}, {5}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(referenceTable1));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenLastIsSkipped) {
  auto tableGenerator = []() -> cppcoro::generator<IdTable> {
    IdTable idTable1 = makeIdTableFromVector({{1}, {2}, {3}});
    co_yield idTable1;

    IdTable idTable2 = makeIdTableFromVector({{4}, {5}});
    co_yield idTable2;
  }();

  Result result{std::move(tableGenerator), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 3}, result);

  auto referenceTable1 = makeIdTableFromVector({{1}, {2}, {3}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(referenceTable1));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstAndSecondArePartial) {
  auto tableGenerator = []() -> cppcoro::generator<IdTable> {
    IdTable idTable1 = makeIdTableFromVector({{1}, {2}, {3}});
    co_yield idTable1;

    IdTable idTable2 = makeIdTableFromVector({{4}, {5}});
    co_yield idTable2;
  }();

  Result result{std::move(tableGenerator), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 3, ._offset = 1}, result);

  auto referenceTable1 = makeIdTableFromVector({{2}, {3}});
  auto referenceTable2 = makeIdTableFromVector({{4}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(referenceTable1, referenceTable2));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstAndLastArePartial) {
  auto tableGenerator = []() -> cppcoro::generator<IdTable> {
    IdTable idTable1 = makeIdTableFromVector({{1}, {2}, {3}});
    co_yield idTable1;

    IdTable idTable2 = makeIdTableFromVector({{4}, {5}});
    co_yield idTable2;

    IdTable idTable3 = makeIdTableFromVector({{6}, {7}, {8}, {9}});
    co_yield idTable3;
  }();

  Result result{std::move(tableGenerator), {}, LocalVocab{}};
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 5, ._offset = 2}, result);

  auto referenceTable1 = makeIdTableFromVector({{3}});
  auto referenceTable2 = makeIdTableFromVector({{4}, {5}});
  auto referenceTable3 = makeIdTableFromVector({{6}, {7}});

  EXPECT_THAT(
      convertToVector(std::move(generator)),
      matchesIdTables(referenceTable1, referenceTable2, referenceTable3));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ensureGeneratorIsNotConsumedWhenNotRequired) {
  {
    auto throwingGenerator = []() -> cppcoro::generator<IdTable> {
      ADD_FAILURE() << "Generator was started" << std::endl;
      throw std::runtime_error("Generator was started");
      co_return;
    }();

    Result result{std::move(throwingGenerator), {}, LocalVocab{}};
    auto generator = ExportQueryExecutionTrees::getRowIndices(
        LimitOffsetClause{._limit = 0, ._offset = 0}, result);
    EXPECT_NO_THROW(convertToVector(std::move(generator)));
  }

  {
    auto throwAfterYieldGenerator = []() -> cppcoro::generator<IdTable> {
      IdTable idTable1 = makeIdTableFromVector({{1}});
      co_yield idTable1;

      ADD_FAILURE() << "Generator was resumed" << std::endl;
      throw std::runtime_error("Generator was resumed");
    }();

    Result result{std::move(throwAfterYieldGenerator), {}, LocalVocab{}};
    auto generator = ExportQueryExecutionTrees::getRowIndices(
        LimitOffsetClause{._limit = 1, ._offset = 0}, result);
    IdTable referenceTable1 = makeIdTableFromVector({{1}});
    std::vector<IdTable> tables;
    EXPECT_NO_THROW({ tables = convertToVector(std::move(generator)); });
    EXPECT_THAT(tables, matchesIdTables(referenceTable1));
  }
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, verifyQleverJsonContainsValidMetadata) {
  std::string_view query =
      "SELECT * WHERE { ?x ?y ?z . FILTER(?y != <p2>) } OFFSET 1 LIMIT 4";
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();

  auto* qec = ad_utility::testing::getQec(
      "<s> <p1> 40,41,42,43,44,45,46,47,48,49"
      " ; <p2> 50,51,52,53,54,55,56,57,58,59");
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = SparqlParser::parseQuery(std::string{query});
  auto qet = qp.createExecutionTree(pq);

  ad_utility::Timer timer{ad_utility::Timer::Started};

  // Verify this is accounted for for time calculation.
  std::this_thread::sleep_for(1ms);

  auto jsonStream = ExportQueryExecutionTrees::computeResultAsQLeverJSON(
      pq, qet, timer, std::move(cancellationHandle));

  std::string aggregateString{};
  for (std::string& chunk : jsonStream) {
    aggregateString += chunk;
  }
  nlohmann::json json = nlohmann::json::parse(aggregateString);
  auto originalRuntimeInfo = qet.getRootOperation()->runtimeInfo();

  EXPECT_EQ(json["query"], query);
  EXPECT_EQ(json["status"], "OK");
  EXPECT_THAT(json["warnings"], ElementsAre());
  EXPECT_THAT(json["selected"], ElementsAre(Eq("?x"), Eq("?y"), Eq("?z")));
  EXPECT_EQ(json["res"].size(), 4);
  auto& runtimeInformationWrapper = json["runtimeInformation"];
  EXPECT_TRUE(runtimeInformationWrapper.contains("meta"));
  ASSERT_TRUE(runtimeInformationWrapper.contains("query_execution_tree"));
  auto& runtimeInformation = runtimeInformationWrapper["query_execution_tree"];
  EXPECT_EQ(runtimeInformation["result_cols"], 3);
  EXPECT_EQ(runtimeInformation["result_rows"], 4);
  EXPECT_EQ(json["resultsize"], 4);
  auto& timingInformation = json["time"];
  EXPECT_GE(toChrono(timingInformation["total"].get<std::string_view>()), 1ms);
  // Ensure result is not returned in microseconds and subsequently interpreted
  // in milliseconds
  EXPECT_LT(
      toChrono(timingInformation["computeResult"].get<std::string_view>()),
      100ms);
  EXPECT_GE(
      toChrono(timingInformation["total"].get<std::string_view>()),
      toChrono(timingInformation["computeResult"].get<std::string_view>()));
}

TEST(ExportQueryExecutionTrees, convertGeneratorForChunkedTransfer) {
  using S = ad_utility::streams::stream_generator;
  auto throwEarly = []() -> S {
    co_yield " Hallo... Ups\n";
    throw std::runtime_error{"failed"};
  };
  auto call = [](S stream) {
    [[maybe_unused]] auto res =
        ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
            std::move(stream));
  };
  AD_EXPECT_THROW_WITH_MESSAGE(call(throwEarly()), std::string_view("failed"));
  auto throwLate = [](bool throwProperException) -> S {
    size_t largerThanBufferSize = (1ul << 20) + 4;
    std::string largerThanBuffer;
    largerThanBuffer.resize(largerThanBufferSize);
    co_yield largerThanBuffer;
    if (throwProperException) {
      throw std::runtime_error{"proper exception"};
    } else {
      throw 424231;
    }
  };

  auto consume = [](auto generator) {
    std::string res;
    for (const auto& el : generator) {
      res.append(el);
    }
    return res;
  };

  cppcoro::generator<std::string> res;
  using namespace ::testing;
  EXPECT_NO_THROW((
      res = ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
          throwLate(true))));
  EXPECT_THAT(consume(std::move(res)),
              AllOf(HasSubstr("!!!!>># An error has occurred"),
                    HasSubstr("proper exception")));

  EXPECT_NO_THROW((
      res = ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
          throwLate(false))));
  EXPECT_THAT(consume(std::move(res)),
              AllOf(HasSubstr("!!!!>># An error has occurred"),
                    HasSubstr("A very strange")));
}
