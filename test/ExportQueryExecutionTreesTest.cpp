// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <robintf@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryPlanner.h"
#include "parser/LiteralOrIri.h"
#include "parser/NormalizedString.h"
#include "parser/SparqlParser.h"
#include "rdfTypes/Literal.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParseableDuration.h"
#include "util/RuntimeParametersTestHelpers.h"

using namespace std::string_literals;
using namespace std::chrono_literals;
using ::testing::ElementsAre;
using ::testing::EndsWith;
using ::testing::Eq;
using ::testing::HasSubstr;

namespace {
auto parseQuery(std::string query,
                const std::vector<DatasetClause>& datasets = {}) {
  static EncodedIriManager evM;
  return SparqlParser::parseQuery(&evM, std::move(query), datasets);
}

// Run the given SPARQL `query` on the given Turtle `kg` and export the result
// as the `mediaType`. `mediaType` must be TSV or CSV.
std::string runQueryStreamableResult(
    const std::string& kg, const std::string& query,
    ad_utility::MediaType mediaType, bool useTextIndex = false,
    std::optional<size_t> exportLimit = std::nullopt) {
  ad_utility::testing::TestIndexConfig config{kg};
  config.createTextIndex = useTextIndex;
  auto qec = ad_utility::testing::getQec(std::move(config));
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(query);
  pq._limitOffset.exportLimit_ = exportLimit;
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
                            bool useTextIndex = false,
                            std::optional<size_t> exportLimit = std::nullopt) {
  ad_utility::testing::TestIndexConfig config{kg};
  config.createTextIndex = useTextIndex;
  auto qec = ad_utility::testing::getQec(std::move(config));
  // TODO<joka921> There is a bug in the caching that we have yet to trace.
  // This cache clearing should not be necessary.
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto pq = parseQuery(query);
  pq._limitOffset.exportLimit_ = exportLimit;
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
  uint64_t resultSize;              // The expected number of results.
  std::string resultTsv;            // The expected result in TSV format.
  std::string resultCsv;            // The expected result in CSV format
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSOn format.
                                    // Note: this member only contains the inner
                                    // result array with the bindings and NOT
                                    // the metadata.
  nlohmann::json resultSparqlJSON;  // The expected result in SparqlJSON format.
  std::string resultXml;
};

// A test case that tests the correct execution and exporting of an ASK query
// in various formats.
struct TestCaseAskQuery {
  std::string kg;                   // The knowledge graph (TURTLE)
  std::string query;                // The query (SPARQL)
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSON format.
  // Note: this member only contains the inner
  // result array with the bindings and NOT
  // the metadata.
  nlohmann::json resultSparqlJSON;  // The expected result in SparqlJSON format.
  std::string resultXml;
};

// For a CONSTRUCT query, the `resultSize` of the QLever JSON is the number of
// results of the WHERE clause.
struct TestCaseConstructQuery {
  std::string kg;                   // The knowledge graph (TURTLE)
  std::string query;                // The query (SPARQL)
  uint64_t resultSizeTotal;         // The expected number of results,
                                    // including triples with UNDEF values.
  uint64_t resultSizeExported;      // The expected number of results exported.
  std::string resultTsv;            // The expected result in TSV format.
  std::string resultCsv;            // The expected result in CSV format
  std::string resultTurtle;         // The expected result in Turtle format
  nlohmann::json resultQLeverJSON;  // The expected result in QLeverJSOn format.
                                    // Note: this member only contains the inner
                                    // result array with the bindings and NOT
                                    // the metadata.
  // How many triples the construct query contains.
  size_t numTriples = 1;
};

// Run a single test case for a SELECT query.
void runSelectQueryTestCase(
    const TestCaseSelectQuery& testCase, bool useTextIndex = false,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
  auto trace = generateLocationTrace(l, "runSelectQueryTestCase");
  using enum ad_utility::MediaType;
  EXPECT_EQ(
      runQueryStreamableResult(testCase.kg, testCase.query, tsv, useTextIndex),
      testCase.resultTsv);
  EXPECT_EQ(
      runQueryStreamableResult(testCase.kg, testCase.query, csv, useTextIndex),
      testCase.resultCsv);

  auto resultJSON = nlohmann::json::parse(runQueryStreamableResult(
      testCase.kg, testCase.query, qleverJson, useTextIndex));
  // TODO<joka921> Test other members of the JSON result (e.g. the selected
  // variables).
  ASSERT_EQ(resultJSON["query"], testCase.query);
  ASSERT_EQ(resultJSON["resultSizeTotal"], testCase.resultSize);
  ASSERT_EQ(resultJSON["resultSizeExported"], testCase.resultSize);
  EXPECT_EQ(resultJSON["res"], testCase.resultQLeverJSON);

  EXPECT_EQ(nlohmann::json::parse(runQueryStreamableResult(
                testCase.kg, testCase.query, sparqlJson, useTextIndex)),
            testCase.resultSparqlJSON);

  // TODO<joka921> Use this for proper testing etc.
  auto xmlAsString = runQueryStreamableResult(testCase.kg, testCase.query,
                                              sparqlXml, useTextIndex);
  EXPECT_EQ(testCase.resultXml, xmlAsString);

  // Test the interaction of normal limit (the LIMIT of the query) and export
  // limit (the value of the `send` parameter).
  for (uint64_t exportLimit = 0ul; exportLimit < 4ul; ++exportLimit) {
    auto resultJson = nlohmann::json::parse(runQueryStreamableResult(
        testCase.kg, testCase.query, qleverJson, useTextIndex, exportLimit));
    ASSERT_EQ(resultJson["resultSizeTotal"], testCase.resultSize);
    ASSERT_EQ(resultJson["resultSizeExported"],
              std::min(exportLimit, testCase.resultSize));
  }
}

// Run a single test case for a CONSTRUCT query.
void runConstructQueryTestCase(
    const TestCaseConstructQuery& testCase,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
  auto trace = generateLocationTrace(l, "runConstructQueryTestCase");
  using enum ad_utility::MediaType;
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, tsv),
            testCase.resultTsv);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, csv),
            testCase.resultCsv);
  auto resultJson = nlohmann::json::parse(
      runQueryStreamableResult(testCase.kg, testCase.query, qleverJson));
  ASSERT_EQ(resultJson["query"], testCase.query);
  ASSERT_EQ(resultJson["resultSizeTotal"], testCase.resultSizeTotal);
  ASSERT_EQ(resultJson["resultSizeExported"], testCase.resultSizeExported);
  EXPECT_EQ(resultJson["res"], testCase.resultQLeverJSON);
  EXPECT_EQ(runQueryStreamableResult(testCase.kg, testCase.query, turtle),
            testCase.resultTurtle);

  // Test the interaction of normal limit (the LIMIT of the query) and export
  // limit (the value of the `send` parameter).
  for (uint64_t exportLimit = 0ul; exportLimit < 4ul; ++exportLimit) {
    auto resultJson = nlohmann::json::parse(runQueryStreamableResult(
        testCase.kg, testCase.query, qleverJson, false, exportLimit));
    ASSERT_EQ(resultJson["resultSizeTotal"], testCase.resultSizeTotal);
    ASSERT_EQ(resultJson["resultSizeExported"],
              std::min(exportLimit * testCase.numTriples,
                       testCase.resultSizeExported));
  }
}

// Run a single test case for an ASK query.
void runAskQueryTestCase(
    const TestCaseAskQuery& testCase,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l, "runAskQueryTestCase");
  using enum ad_utility::MediaType;
  // TODO<joka921> match the exception
  EXPECT_ANY_THROW(runQueryStreamableResult(testCase.kg, testCase.query, tsv));
  EXPECT_ANY_THROW(runQueryStreamableResult(testCase.kg, testCase.query, csv));
  EXPECT_ANY_THROW(
      runQueryStreamableResult(testCase.kg, testCase.query, octetStream));
  EXPECT_ANY_THROW(
      runQueryStreamableResult(testCase.kg, testCase.query, turtle));
  auto resultJson = nlohmann::json::parse(
      runQueryStreamableResult(testCase.kg, testCase.query, qleverJson));
  ASSERT_EQ(resultJson["query"], testCase.query);
  ASSERT_EQ(resultJson["resultSizeExported"], 1u);
  EXPECT_EQ(resultJson["res"], testCase.resultQLeverJSON);

  EXPECT_EQ(nlohmann::json::parse(runQueryStreamableResult(
                testCase.kg, testCase.query, sparqlJson)),
            testCase.resultSparqlJSON);

  auto xmlAsString =
      runQueryStreamableResult(testCase.kg, testCase.query, sparqlXml);
  EXPECT_EQ(testCase.resultXml, xmlAsString);
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
    ad_utility::InputRangeTypeErased<
        ExportQueryExecutionTrees::TableConstRefWithVocab>
        generator) {
  std::vector<IdTable> result;
  for (const ExportQueryExecutionTrees::TableConstRefWithVocab& pair :
       generator) {
    result.push_back(pair.idTable().clone());
  }
  return result;
}

// match the contents of a `vector<IdTable>` to the given `tables`.
template <typename... Tables>
auto matchesIdTables(const Tables&... tables) {
  return ElementsAre(matchesIdTable(tables)...);
}

std::vector<IdTable> convertToVector(
    ad_utility::InputRangeTypeErased<ExportQueryExecutionTrees::TableWithRange>
        generator) {
  std::vector<IdTable> result;
  for (const auto& [pair, range] : generator) {
    const auto& idTable = pair.idTable();
    result.emplace_back(idTable.numColumns(), idTable.getAllocator());
    result.back().insertAtEnd(idTable, *range.begin(), *(range.end() - 1) + 1);
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
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 3, 3,
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
  std::string kg =
      "<s> <p> true . <s> <p> false ."
      " <s2> <p2> \"1\"^^<http://www.w3.org/2001/XMLSchema#boolean> ."
      " <s2> <p2> \"0\"^^<http://www.w3.org/2001/XMLSchema#boolean> .";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">false</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">true</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#boolean">1</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{
      kg, query, 4,
      // TSV
      "?o\n"
      "false\n"
      "0\n"
      "true\n"
      "1\n",
      // CSV
      "o\n"
      "false\n"
      "0\n"
      "true\n"
      "1\n",
      makeExpectedQLeverJSON(
          {"\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s,
           "\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s,
           "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s,
           "\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "false"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "true"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#boolean",
                           "literal", "1")}),
      expectedXml};
  runSelectQueryTestCase(testCase);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 4, 4,
      // TSV
      "<s>\t<p>\tfalse\n"
      "<s2>\t<p2>\t\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>\n"
      "<s>\t<p>\ttrue\n"
      "<s2>\t<p2>\t\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>\n",
      // CSV
      "<s>,<p>,false\n"
      "<s2>,<p2>,\"\"\"0\"\"^^<http://www.w3.org/2001/XMLSchema#boolean>\"\n"
      "<s>,<p>,true\n"
      "<s2>,<p2>,\"\"\"1\"\"^^<http://www.w3.org/2001/XMLSchema#boolean>\"\n",
      // Turtle
      "<s> <p> false .\n"
      "<s2> <p2> \"0\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n"
      "<s> <p> true .\n"
      "<s2> <p2> \"1\"^^<http://www.w3.org/2001/XMLSchema#boolean> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"<s>"s, "<p>"s, "false"s});
        j.push_back(
            std::vector{"<s2>"s, "<p2>"s,
                        "\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "true"s});
        j.push_back(
            std::vector{"<s2>"s, "<p2>"s,
                        "\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>"s});
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
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("o");
        j["results"]["bindings"].push_back({});
        j["results"]["bindings"].push_back({});
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCase);

  // The `2` is the number of results including triples with UNDEF values. The
  // `0` is the number of results excluding such triples.
  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?x ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 2, 0,
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
      "<s> <p> 42.2 . <s> <p> -42019234865.781e12 ."
      " <s> <p> 100.0 . <s> <p> 960000.06 ."
      " <s> <p> 123456.00000001 . <s> <p> 1e-10 ."
      " <s> <p> 4.012934858173560e-12 ."
      " <s> <p> \"NaN\"^^<http://www.w3.org/2001/XMLSchema#double> ."
      " <s> <p> \"INF\"^^<http://www.w3.org/2001/XMLSchema#double> ."
      " <s> <p> \"-INF\"^^<http://www.w3.org/2001/XMLSchema#double> .";
  std::string query = "SELECT ?o WHERE {?s ?p ?o} ORDER BY ?o";

  std::string expectedXml = makeXMLHeader({"o"}) +
                            R"(
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#double">-INF</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">-42019234865780982022144.0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">4.012934858174e-12</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">1e-10</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">42.2</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">100.0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">123456.0</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#decimal">960000.06</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#double">INF</literal></binding>
  </result>
  <result>
    <binding name="o"><literal datatype="http://www.w3.org/2001/XMLSchema#double">NaN</literal></binding>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCaseFloat{
      kg, query, 10,
      // TSV
      "?o\n"
      "-INF\n"
      "-42019234865780982022144.0\n"
      "4.012934858174e-12\n"
      "1e-10\n"
      "42.2\n"
      "100.0\n"
      "123456.0\n"
      "960000.06\n"
      "INF\n"
      "NaN\n",
      // CSV
      "o\n"
      "-INF\n"
      "-42019234865780982022144.0\n"
      "4.012934858174e-12\n"
      "1e-10\n"
      "42.2\n"
      "100.0\n"
      "123456.0\n"
      "960000.06\n"
      "INF\n"
      "NaN\n",
      makeExpectedQLeverJSON(
          {"\"-INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s,
           "\"-42019234865780982022144.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"4.012934858174e-12\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"1e-10\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"42.2\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"100.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"123456.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"960000.06\"^^<http://www.w3.org/2001/XMLSchema#decimal>"s,
           "\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s,
           "\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>"s}),
      makeExpectedSparqlJSON(
          {makeJSONBinding("http://www.w3.org/2001/XMLSchema#double", "literal",
                           "-INF"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "-42019234865780982022144.0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "4.012934858174e-12"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "1e-10"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "42.2"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "100.0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "123456.0"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#decimal",
                           "literal", "960000.06"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#double", "literal",
                           "INF"),
           makeJSONBinding("http://www.w3.org/2001/XMLSchema#double", "literal",
                           "NaN")}),
      expectedXml};
  runSelectQueryTestCase(testCaseFloat);

  TestCaseConstructQuery testCaseConstruct{
      kg, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o} ORDER BY ?o", 10, 10,
      // TSV
      "<s>\t<p>\t\"-INF\"^^<http://www.w3.org/2001/XMLSchema#double>\n"
      "<s>\t<p>\t-42019234865780982022144.0\n"
      "<s>\t<p>\t4.012934858174e-12\n"
      "<s>\t<p>\t1e-10\n"
      "<s>\t<p>\t42.2\n"
      "<s>\t<p>\t100.0\n"
      "<s>\t<p>\t123456.0\n"
      "<s>\t<p>\t960000.06\n"
      "<s>\t<p>\t\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>\n"
      "<s>\t<p>\t\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>\n",
      // CSV
      "<s>,<p>,\"\"\"-INF\"\"^^<http://www.w3.org/2001/XMLSchema#double>\"\n"
      "<s>,<p>,-42019234865780982022144.0\n"
      "<s>,<p>,4.012934858174e-12\n"
      "<s>,<p>,1e-10\n"
      "<s>,<p>,42.2\n"
      "<s>,<p>,100.0\n"
      "<s>,<p>,123456.0\n"
      "<s>,<p>,960000.06\n"
      "<s>,<p>,\"\"\"INF\"\"^^<http://www.w3.org/2001/XMLSchema#double>\"\n"
      "<s>,<p>,\"\"\"NaN\"\"^^<http://www.w3.org/2001/XMLSchema#double>\"\n",
      // Turtle
      "<s> <p> \"-INF\"^^<http://www.w3.org/2001/XMLSchema#double> .\n"
      "<s> <p> -42019234865780982022144.0 .\n"
      "<s> <p> 4.012934858174e-12 .\n"
      "<s> <p> 1e-10 .\n"
      "<s> <p> 42.2 .\n"
      "<s> <p> 100.0 .\n"
      "<s> <p> 123456.0 .\n"
      "<s> <p> 960000.06 .\n"
      "<s> <p> \"INF\"^^<http://www.w3.org/2001/XMLSchema#double> .\n"
      "<s> <p> \"NaN\"^^<http://www.w3.org/2001/XMLSchema#double> .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{
            "<s>"s, "<p>"s,
            "\"-INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "-42019234865780982022144.0"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "4.012934858174e-12"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "1e-10"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "42.2"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "100.0"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "123456.0"s});
        j.push_back(std::vector{"<s>"s, "<p>"s, "960000.06"s});
        j.push_back(
            std::vector{"<s>"s, "<p>"s,
                        "\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>"s});
        j.push_back(
            std::vector{"<s>"s, "<p>"s,
                        "\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>"s});
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
  std::string kg = R"(<s> <p> "Some\"Where	Over,"@en-ca.)";
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

  // The `1` is the number of results including triples with UNDEF values. The
  // `0` is the number of results excluding such triples.
  TestCaseConstructQuery testCaseConstruct{
      kg,
      "CONSTRUCT {?s <pred> ?o} WHERE {?s <p> <o> OPTIONAL {?s <p2> ?o}} ORDER "
      "BY ?o",
      1,
      0,
      "",
      "",
      "",
      std::vector<std::string>{}};
  runConstructQueryTestCase(testCaseConstruct);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, EmptyLines) {
  std::string kg = "<s> <p> <o>";
  std::string query = "SELECT * WHERE { <s> <p> <o> }";
  std::string expectedXml = makeXMLHeader({}) +
                            R"(
  <result>
  </result>)" + xmlTrailer;
  TestCaseSelectQuery testCase{kg,
                               query,
                               1,
                               "\n\n",
                               "\n\n",
                               nlohmann::json{std::vector{std::vector<int>{}}},
                               []() {
                                 nlohmann::json j;
                                 j["head"]["vars"] = nlohmann::json::array();
                                 j["results"]["bindings"].push_back({});
                                 return j;
                               }(),
                               expectedXml};
  runSelectQueryTestCase(testCase);
}

// ____________________________________________________________________________
TEST(ExportQueryExecutionTrees, BlankNode) {
  std::string kg = "<s> <p> _:blank";
  std::string objectQuery = "SELECT ?o WHERE { ?s ?p ?o } ORDER BY ?o";
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
  kg = "<s> <p> <o>";
  objectQuery =
      "SELECT (BNODE(\"1\") AS ?a) (BNODE(?x) AS ?b) WHERE { VALUES (?x) { (1) "
      "(2) } }";
  expectedXml = makeXMLHeader({"a", "b"}) +
                R"(
  <result>
    <binding name="a"><bnode>un1_0</bnode></binding>
    <binding name="b"><bnode>un1_0</bnode></binding>
  </result>
  <result>
    <binding name="a"><bnode>un1_1</bnode></binding>
    <binding name="b"><bnode>un2_1</bnode></binding>
  </result>)" + xmlTrailer;
  testCaseBlankNode = TestCaseSelectQuery{
      kg, objectQuery, 2,
      // TSV
      "?a\t?b\n"
      "_:un1_0\t_:un1_0\n"
      "_:un1_1\t_:un2_1\n",
      // CSV
      "a,b\n"
      "_:un1_0,_:un1_0\n"
      "_:un1_1,_:un2_1\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"_:un1_0"s, "_:un1_0"s});
        j.push_back(std::vector{"_:un1_1"s, "_:un2_1"s});
        return j;
      }(),
      []() {
        nlohmann::json j;
        j["head"]["vars"].push_back("a");
        j["head"]["vars"].push_back("b");
        auto& bindings = j["results"]["bindings"];
        bindings.emplace_back();
        bindings.back()["a"] = makeJSONBinding(std::nullopt, "bnode", "un1_0");
        bindings.back()["b"] = makeJSONBinding(std::nullopt, "bnode", "un1_0");
        bindings.emplace_back();
        bindings.back()["a"] = makeJSONBinding(std::nullopt, "bnode", "un1_1");
        bindings.back()["b"] = makeJSONBinding(std::nullopt, "bnode", "un2_1");
        return j;
      }(),
      expectedXml};
  runSelectQueryTestCase(testCaseBlankNode);

  TestCaseConstructQuery testCaseConstruct{
      "<a> <b> <c> . <d> <e> <f> . <g> <h> <i> . <j> <k> <l>",
      "CONSTRUCT { [] <p> _:a . [] <p> _:a } WHERE { ?s ?p ?o }", 8, 8,
      // TSV
      "_:g0_0\t<p>\t_:u0_a\n"
      "_:g0_1\t<p>\t_:u0_a\n"
      "_:g1_0\t<p>\t_:u1_a\n"
      "_:g1_1\t<p>\t_:u1_a\n"
      "_:g2_0\t<p>\t_:u2_a\n"
      "_:g2_1\t<p>\t_:u2_a\n"
      "_:g3_0\t<p>\t_:u3_a\n"
      "_:g3_1\t<p>\t_:u3_a\n",
      // CSV
      "_:g0_0,<p>,_:u0_a\n"
      "_:g0_1,<p>,_:u0_a\n"
      "_:g1_0,<p>,_:u1_a\n"
      "_:g1_1,<p>,_:u1_a\n"
      "_:g2_0,<p>,_:u2_a\n"
      "_:g2_1,<p>,_:u2_a\n"
      "_:g3_0,<p>,_:u3_a\n"
      "_:g3_1,<p>,_:u3_a\n",
      // Turtle
      "_:g0_0 <p> _:u0_a .\n"
      "_:g0_1 <p> _:u0_a .\n"
      "_:g1_0 <p> _:u1_a .\n"
      "_:g1_1 <p> _:u1_a .\n"
      "_:g2_0 <p> _:u2_a .\n"
      "_:g2_1 <p> _:u2_a .\n"
      "_:g3_0 <p> _:u3_a .\n"
      "_:g3_1 <p> _:u3_a .\n",
      []() {
        nlohmann::json j;
        j.push_back(std::vector{"_:g0_0"s, "<p>"s, "_:u0_a"s});
        j.push_back(std::vector{"_:g0_1"s, "<p>"s, "_:u0_a"s});
        j.push_back(std::vector{"_:g1_0"s, "<p>"s, "_:u1_a"s});
        j.push_back(std::vector{"_:g1_1"s, "<p>"s, "_:u1_a"s});
        j.push_back(std::vector{"_:g2_0"s, "<p>"s, "_:u2_a"s});
        j.push_back(std::vector{"_:g2_1"s, "<p>"s, "_:u2_a"s});
        j.push_back(std::vector{"_:g3_0"s, "<p>"s, "_:u3_a"s});
        j.push_back(std::vector{"_:g3_1"s, "<p>"s, "_:u3_a"s});
        return j;
      }(),
      2};
  runConstructQueryTestCase(testCaseConstruct);
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
  std::string expectedXml = makeXMLHeader({"s"}) +
                            R"(
  <result>
    <binding name="s"><uri>d</uri></binding>
  </result>
  <result>
    <binding name="s"><uri>g</uri></binding>
  </result>)" + xmlTrailer;
  // The `OrderBy` operation doesn't support the limit natively.
  std::string_view objectQuery0 =
      "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 2 OFFSET 1";
  // The `IndexScan` operation does support the limit natively.
  std::string_view objectQuery1 =
      "SELECT ?s WHERE { ?s ?p ?o } INTERNAL SORT BY ?s LIMIT 2 OFFSET 1";
  for (auto objectQuery : {objectQuery0, objectQuery1}) {
    TestCaseSelectQuery testCaseLimitOffset{
        kg, std::string{objectQuery}, 2,
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

  // If none of the selected variables is defined in the query body, we have an
  // empty solution mapping per row, but there is no need to materialize any
  // IRIs or literals.
  std::string queryNoVariablesVisible = "SELECT ?not ?known WHERE {<s> ?p ?o}";
  auto resultNoColumns = runJSONQuery(kg, queryNoVariablesVisible,
                                      ad_utility::MediaType::sparqlJson);
  ASSERT_EQ(resultNoColumns["results"]["bindings"].size(), 1);
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

// Test the correct exporting of ASK queries.
TEST(ExportQueryExecutionTrees, AskQuery) {
  auto askResultTrue = [](bool lazy) {
    TestCaseAskQuery testCase;
    if (lazy) {
      testCase.kg = "<x> <y> <z>";
      testCase.query = "ASK { <x> ?p ?o}";
    } else {
      testCase.query = "ASK { BIND (3 as ?x) FILTER (?x > 0)}";
    }
    testCase.resultQLeverJSON = nlohmann::json{std::vector<std::string>{
        "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>"}};
    testCase.resultSparqlJSON =
        nlohmann::json::parse(R"({"head":{ }, "boolean" : true})");
    testCase.resultXml =
        "<?xml version=\"1.0\"?>\n<sparql "
        "xmlns=\"http://www.w3.org/2005/sparql-results#\">\n  <head/>\n  "
        "<boolean>true</boolean>\n</sparql>";

    return testCase;
  };

  auto askResultFalse = [](bool lazy) {
    TestCaseAskQuery testCase;
    if (lazy) {
      testCase.kg = "<x> <y> <z>";
      testCase.query = "ASK { <y> ?p ?o}";
    } else {
      testCase.query = "ASK { BIND (3 as ?x) FILTER (?x < 0)}";
    }
    testCase.resultQLeverJSON = nlohmann::json{std::vector<std::string>{
        "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>"}};
    testCase.resultSparqlJSON =
        nlohmann::json::parse(R"({"head":{ }, "boolean" : false})");
    testCase.resultXml =
        "<?xml version=\"1.0\"?>\n<sparql "
        "xmlns=\"http://www.w3.org/2005/sparql-results#\">\n  <head/>\n  "
        "<boolean>false</boolean>\n</sparql>";
    return testCase;
  };
  runAskQueryTestCase(askResultTrue(true));
  runAskQueryTestCase(askResultTrue(false));
  runAskQueryTestCase(askResultFalse(true));
  runAskQueryTestCase(askResultFalse(false));
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
  auto pq = parseQuery(GetParam() == turtle
                           ? "CONSTRUCT { ?x ?y ?z } WHERE { ?x ?y ?z }"
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
                           IdTable idTableB) -> Result::Generator {
    co_yield {std::move(idTableA), LocalVocab{}};

    co_yield {std::move(idTableB), LocalVocab{}};
  }(idTable1.clone(), idTable2.clone());

  Result result{std::move(tableGenerator), {}};
  auto generator = ExportQueryExecutionTrees::getIdTables(result);

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(idTable1, idTable2));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ensureCorrectSlicingOfSingleIdTable) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 1, ._offset = 1}, result, resultSizeTotal);

  auto expectedResult = makeIdTableFromVector({{2}});
  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult));
  EXPECT_EQ(resultSizeTotal, 1);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstIsSkipped) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = std::nullopt, ._offset = 3}, result,
      resultSizeTotal);

  auto expectedResult = makeIdTableFromVector({{4}, {5}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult));
  EXPECT_EQ(resultSizeTotal, 2);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenLastIsSkipped) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 3}, result, resultSizeTotal);

  auto expectedResult = makeIdTableFromVector({{1}, {2}, {3}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult));
  EXPECT_EQ(resultSizeTotal, 3);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstAndSecondArePartial) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 3, ._offset = 1}, result, resultSizeTotal);

  auto expectedResult1 = makeIdTableFromVector({{2}, {3}});
  auto expectedResult2 = makeIdTableFromVector({{4}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedResult1, expectedResult2));
  EXPECT_EQ(resultSizeTotal, 3);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees,
     ensureCorrectSlicingOfIdTablesWhenFirstAndLastArePartial) {
  auto tableGenerator = []() -> Result::Generator {
    Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}, {2}, {3}}),
                                   LocalVocab{}};
    co_yield pair1;

    Result::IdTableVocabPair pair2{makeIdTableFromVector({{4}, {5}}),
                                   LocalVocab{}};
    co_yield pair2;

    Result::IdTableVocabPair pair3{makeIdTableFromVector({{6}, {7}, {8}, {9}}),
                                   LocalVocab{}};
    co_yield pair3;
  }();

  Result result{std::move(tableGenerator), {}};
  uint64_t resultSizeTotal = 0;
  auto generator = ExportQueryExecutionTrees::getRowIndices(
      LimitOffsetClause{._limit = 5, ._offset = 2}, result, resultSizeTotal);

  auto expectedTable1 = makeIdTableFromVector({{3}});
  auto expectedTable2 = makeIdTableFromVector({{4}, {5}});
  auto expectedTable3 = makeIdTableFromVector({{6}, {7}});

  EXPECT_THAT(convertToVector(std::move(generator)),
              matchesIdTables(expectedTable1, expectedTable2, expectedTable3));
  EXPECT_EQ(resultSizeTotal, 5);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ensureGeneratorIsNotConsumedWhenNotRequired) {
  {
    auto throwingGenerator = []() -> Result::Generator {
      std::string message = "Generator was started, but should not have been";
      ADD_FAILURE() << message << std::endl;
      throw std::runtime_error(message);
      co_return;
    }();

    Result result{std::move(throwingGenerator), {}};
    uint64_t resultSizeTotal = 0;
    auto generator = ExportQueryExecutionTrees::getRowIndices(
        LimitOffsetClause{._limit = 0, ._offset = 0}, result, resultSizeTotal);
    EXPECT_NO_THROW(convertToVector(std::move(generator)));
  }

  {
    auto throwAfterYieldGenerator = []() -> Result::Generator {
      Result::IdTableVocabPair pair1{makeIdTableFromVector({{1}}),
                                     LocalVocab{}};
      co_yield pair1;

      std::string message =
          "Generator was called a second time, but should not "
          "have been";
      ADD_FAILURE() << message << std::endl;
      throw std::runtime_error(message);
    }();

    Result result{std::move(throwAfterYieldGenerator), {}};
    uint64_t resultSizeTotal = 0;
    auto generator = ExportQueryExecutionTrees::getRowIndices(
        LimitOffsetClause{._limit = 1, ._offset = 0}, result, resultSizeTotal);
    IdTable expectedTable = makeIdTableFromVector({{1}});
    std::vector<IdTable> tables;
    EXPECT_NO_THROW({ tables = convertToVector(std::move(generator)); });
    EXPECT_THAT(tables, matchesIdTables(expectedTable));
    EXPECT_EQ(resultSizeTotal, 1);
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
  auto pq = parseQuery(std::string{query});
  auto qet = qp.createExecutionTree(pq);

  ad_utility::Timer timer{ad_utility::Timer::Started};

  // Verify this is accounted for for time calculation.
  std::this_thread::sleep_for(1ms);

  auto jsonStream = ExportQueryExecutionTrees::computeResultAsQLeverJSON(
      pq, qet, pq._limitOffset, timer, std::move(cancellationHandle));

  std::string aggregateString{};
  for (std::string_view chunk : jsonStream) {
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

// _____________________________________________________________________________
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

  std::optional<ad_utility::InputRangeTypeErased<std::string>> res;
  using namespace ::testing;
  EXPECT_NO_THROW((
      res = ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
          throwLate(true))));
  EXPECT_THAT(consume(std::move(res.value())),
              AllOf(HasSubstr("!!!!>># An error has occurred"),
                    HasSubstr("proper exception")));

  EXPECT_NO_THROW((
      res = ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
          throwLate(false))));
  EXPECT_THAT(consume(std::move(res.value())),
              AllOf(HasSubstr("!!!!>># An error has occurred"),
                    HasSubstr("A very strange")));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, idToLiteralFunctionality) {
  std::string kg =
      "<s> <p> \"something\" . <s> <p> 1 . <s> <p> "
      "\"some\"^^<http://www.w3.org/2001/XMLSchema#string> . <s> <p> "
      "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype> .";
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  using enum Datatype;

  // Helper function that takes an ID and a vector of test cases and checks
  // if the ID is correctly converted. A more detailed explanation of the test
  // logic is below with the test cases.
  auto checkIdToLiteral =
      [&](Id id,
          const std::vector<std::tuple<bool, std::optional<std::string>>>&
              cases) {
        for (const auto& [onlyLiteralsWithXsdString, expected] : cases) {
          auto result = ExportQueryExecutionTrees::idToLiteral(
              qec->getIndex(), id, LocalVocab{}, onlyLiteralsWithXsdString);
          if (expected) {
            EXPECT_THAT(result,
                        ::testing::Optional(::testing::ResultOf(
                            [](const auto& literalOrIri) {
                              return literalOrIri.toStringRepresentation();
                            },
                            ::testing::StrEq(*expected))));
          } else {
            EXPECT_EQ(result, std::nullopt);
          }
        }
      };

  // Test cases: Each tuple describes one test case.
  // The first element is the ID of the element to test.
  // The second element is a list of 2 configurations:
  // 1. for literals all datatypes are removed, IRIs
  // are converted to literals
  // 2. only literals with no datatype or`xsd:string` are returned.
  // In the last case the datatype is removed.
  std::vector<
      std::tuple<Id, std::vector<std::tuple<bool, std::optional<std::string>>>>>
      testCases = {
          // Case: Literal without datatype
          {getId("\"something\""),
           {{false, "\"something\""}, {true, "\"something\""}}},

          // Case: Literal with datatype `xsd:string`
          {getId("\"some\"^^<http://www.w3.org/2001/XMLSchema#string>"),
           {{false, "\"some\""}, {true, "\"some\""}}},

          // Case: Literal with unknown datatype
          {getId("\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>"),
           {{false, "\"dadudeldu\""}, {true, std::nullopt}}},

          // Case: IRI
          {getId("<s>"), {{false, "\"s\""}, {true, std::nullopt}}},

          // Case: datatype `Int`
          {ad_utility::testing::IntId(1),
           {{false, "\"1\""}, {true, std::nullopt}}},

          // Case: Undefined ID
          {ad_utility::testing::UndefId(),
           {{false, std::nullopt}, {true, std::nullopt}}}};

  for (const auto& [id, cases] : testCases) {
    checkIdToLiteral(id, cases);
  }
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, idToLiteralOrIriFunctionality) {
  std::string kg =
      "<s> <p> \"something\" . <s> <p> 1 . <s> <p> "
      "\"some\"^^<http://www.w3.org/2001/XMLSchema#string> . <s> <p> "
      "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype> . <s> <p> "
      "<http://example.com/> .";
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;
  using Iri = ad_utility::triple_component::Iri;

  std::vector<std::pair<ValueId, std::optional<LiteralOrIri>>> expected{
      {getId("\"something\""),
       LiteralOrIri{Literal::fromStringRepresentation("\"something\"")}},
      {getId("\"some\"^^<http://www.w3.org/2001/XMLSchema#string>"),
       LiteralOrIri{Literal::fromStringRepresentation(
           "\"some\"^^<http://www.w3.org/2001/XMLSchema#string>")}},
      {getId("\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>"),
       LiteralOrIri{Literal::fromStringRepresentation(
           "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>")}},
      {getId("<http://example.com/>"),
       LiteralOrIri{Iri::fromIriref("<http://example.com/>")}},
      {ValueId::makeFromBool(true),
       LiteralOrIri{Literal::fromStringRepresentation(
           "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")}},
      {ValueId::makeUndefined(), std::nullopt}};
  for (const auto& [valueId, expRes] : expected) {
    ASSERT_EQ(ExportQueryExecutionTrees::idToLiteralOrIri(
                  qec->getIndex(), valueId, LocalVocab{}),
              expRes);
  }
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, getLiteralOrNullopt) {
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;
  using Iri = ad_utility::triple_component::Iri;

  auto litOrNulloptTestHelper = [](std::optional<LiteralOrIri> input,
                                   std::optional<std::string> expectedRes) {
    auto res = ExportQueryExecutionTrees::getLiteralOrNullopt(input);
    ASSERT_EQ(res.has_value(), expectedRes.has_value());
    if (res.has_value()) {
      ASSERT_EQ(expectedRes.value(), res.value().toStringRepresentation());
    }
  };

  auto lit = LiteralOrIri{Literal::fromStringRepresentation("\"test\"")};
  litOrNulloptTestHelper(lit, "\"test\"");

  auto litWithType = LiteralOrIri{Literal::fromStringRepresentation(
      "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>")};
  litOrNulloptTestHelper(
      litWithType, "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>");

  litOrNulloptTestHelper(std::nullopt, std::nullopt);

  auto iri = LiteralOrIri{Iri::fromIriref("<https://example.com/>")};
  litOrNulloptTestHelper(iri, std::nullopt);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, IsPlainLiteralOrLiteralWithXsdString) {
  using Iri = ad_utility::triple_component::Iri;
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;

  auto toLiteralOrIri = [](std::string_view content, auto descriptor) {
    return LiteralOrIri{Literal::literalWithNormalizedContent(
        asNormalizedStringViewUnsafe(content), descriptor)};
  };

  auto verify = [](const LiteralOrIri& input, bool expected) {
    EXPECT_EQ(
        ExportQueryExecutionTrees::isPlainLiteralOrLiteralWithXsdString(input),
        expected);
  };

  verify(toLiteralOrIri("Hallo", std::nullopt), true);
  verify(toLiteralOrIri(
             "Hallo",
             Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>")),
         true);
  verify(
      toLiteralOrIri(
          "Hallo", Iri::fromIriref("<http://www.unknown.com/NoSuchDatatype>")),
      false);

  EXPECT_THROW(
      verify(LiteralOrIri{Iri::fromIriref("<http://www.example.com/someIri>")},
             false),
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, ReplaceAnglesByQuotes) {
  std::string input = "<s>";
  std::string expected = "\"s\"";
  EXPECT_EQ(ExportQueryExecutionTrees::replaceAnglesByQuotes(input), expected);
  input = "s>";
  EXPECT_THROW(ExportQueryExecutionTrees::replaceAnglesByQuotes(input),
               ad_utility::Exception);
  input = "<s";
  EXPECT_THROW(ExportQueryExecutionTrees::replaceAnglesByQuotes(input),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, blankNodeIrisAreProperlyFormatted) {
  using ad_utility::triple_component::Iri;
  std::string_view input = "_:test";
  EXPECT_THAT(ExportQueryExecutionTrees::blankNodeIriToString(
                  Iri::fromStringRepresentation(absl::StrCat(
                      QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, input, ">"))),
              ::testing::Optional(::testing::Eq(input)));
  EXPECT_EQ(ExportQueryExecutionTrees::blankNodeIriToString(
                Iri::fromStringRepresentation("<some_iri>")),
            std::nullopt);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, compensateForLimitOffsetClause) {
  auto* qec = ad_utility::testing::getQec();

  auto qet1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}),
      std::vector<std::optional<Variable>>{std::nullopt}, false);
  auto qet2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}),
      std::vector<std::optional<Variable>>{std::nullopt}, true);

  LimitOffsetClause limit{10, 5};
  ExportQueryExecutionTrees::compensateForLimitOffsetClause(limit, *qet1);
  EXPECT_EQ(limit._offset, 5);

  ExportQueryExecutionTrees::compensateForLimitOffsetClause(limit, *qet2);
  EXPECT_EQ(limit._offset, 0);
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, EncodedIriManagerUsage) {
  // Test that encoded IRIs are properly handled during export

  // Create a knowledge graph with IRIs that should be encodable
  std::string kg =
      "<http://example.org/123> <http://example.org/predicate456> "
      "<http://example.org/789> ."
      "<http://test.com/id/111> <http://example.org/predicate456> \"literal "
      "value\" .";

  // Test XML export with encoded IRIs
  std::string query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s ?p ?o";

  // Create test configuration with EncodedIriManager
  auto encodedIriManager = std::make_shared<EncodedIriManager>(
      std::vector<std::string>{"http://example.org/", "http://test.com/id/"});

  ad_utility::testing::TestIndexConfig config{kg};
  config.encodedIriManager = EncodedIriManager{
      std::vector<std::string>{"http://example.org/", "http://test.com/id/"}};
  auto qec = ad_utility::testing::getQec(std::move(config));

  // Parse query with the same EncodedIriManager
  auto parsedQuery =
      SparqlParser::parseQuery(encodedIriManager.get(), query, {});

  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qec, cancellationHandle};
  auto qet = qp.createExecutionTree(parsedQuery);

  // Export as XML and verify encoded IRIs are properly converted back
  ad_utility::Timer timer{ad_utility::Timer::Started};
  auto cancellationHandle2 =
      std::make_shared<ad_utility::CancellationHandle<>>();
  std::string result;
  for (const auto& chunk : ExportQueryExecutionTrees::computeResult(
           parsedQuery, qet, ad_utility::MediaType::sparqlXml, timer,
           std::move(cancellationHandle2))) {
    result += chunk;
  }

  // Verify that the original IRI strings appear in the output
  EXPECT_THAT(result, HasSubstr("http://example.org/123"));
  EXPECT_THAT(result, HasSubstr("http://example.org/predicate456"));
  EXPECT_THAT(result, HasSubstr("http://example.org/789"));
  EXPECT_THAT(result, HasSubstr("http://test.com/id/111"));
  EXPECT_THAT(result, HasSubstr("literal value"));

  // Test TSV export as well
  ad_utility::Timer tsvTimer{ad_utility::Timer::Started};
  auto cancellationHandle3 =
      std::make_shared<ad_utility::CancellationHandle<>>();
  std::string tsvResult;
  for (const auto& chunk : ExportQueryExecutionTrees::computeResult(
           parsedQuery, qet, ad_utility::MediaType::tsv, tsvTimer,
           std::move(cancellationHandle3))) {
    tsvResult += chunk;
  }
  EXPECT_THAT(tsvResult, HasSubstr("http://example.org/123"));
  EXPECT_THAT(tsvResult, HasSubstr("http://example.org/predicate456"));
  EXPECT_THAT(tsvResult, HasSubstr("http://example.org/789"));
  EXPECT_THAT(tsvResult, HasSubstr("http://test.com/id/111"));
}

// _____________________________________________________________________________
TEST(ExportQueryExecutionTrees, GetLiteralOrIriFromVocabIndexWithEncodedIris) {
  // Test the getLiteralOrIriFromVocabIndex function specifically with encoded
  // IRIs

  // Create an EncodedIriManager with test prefixes
  std::vector<std::string> prefixes = {"http://example.org/",
                                       "http://test.com/"};
  EncodedIriManager encodedIriManager{prefixes};

  // Create a test index config with the encoded IRI manager
  using namespace ad_utility::testing;
  TestIndexConfig config;
  config.encodedIriManager = encodedIriManager;
  auto qec = getQec(std::move(config));

  // Test driver lambda to reduce code duplication
  LocalVocab emptyLocalVocab;
  auto testEncodedIri = [&](const std::string& iri) {
    // Encode the IRI
    auto encodedIdOpt = encodedIriManager.encode(iri);
    ASSERT_TRUE(encodedIdOpt.has_value()) << "Failed to encode IRI: " << iri;

    Id encodedId = *encodedIdOpt;
    EXPECT_EQ(encodedId.getDatatype(), Datatype::EncodedVal);

    // Test getLiteralOrIriFromVocabIndex with the encoded ID
    auto result = ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
        qec->getIndex(), encodedId, emptyLocalVocab);

    // The result should be the original IRI
    EXPECT_TRUE(result.isIri());
    EXPECT_EQ(result.toStringRepresentation(), iri);
  };

  // Test multiple encoded IRIs
  testEncodedIri("<http://example.org/123>");
  testEncodedIri("<http://test.com/456>");

  // Test that non-encodable IRIs fall back to VocabIndex handling
  // (This test assumes the test index has some vocabulary entries)
  if (!qec->getIndex().getVocab().size()) {
    VocabIndex vocabIndex = VocabIndex::make(0);  // First vocab entry
    Id vocabId = Id::makeFromVocabIndex(vocabIndex);

    auto vocabResult = ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
        qec->getIndex(), vocabId, emptyLocalVocab);

    // Should successfully return some IRI or literal from vocabulary
    EXPECT_FALSE(vocabResult.toStringRepresentation().empty());
  }
}

// Test that a `sparql-results+json` export includes a `meta` field if and
// only if the respective runtime parameter is enabled.
TEST(ExportQueryExecutionTrees, SparqlJsonWithMetaField) {
  std::string kg = "<x> <y> <z>";
  std::string query = "SELECT ?s ?p ?o WHERE {?s ?p ?o}";

  // Case 1: Runtime parameter enabled (default).
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(true);
    auto result = runJSONQuery(kg, query, ad_utility::MediaType::sparqlJson);
    ASSERT_TRUE(result.contains("head"));
    ASSERT_TRUE(result.contains("results"));
    ASSERT_TRUE(result["head"].contains("vars"));
    ASSERT_TRUE(result.contains("meta"));
    ASSERT_TRUE(result["meta"].contains("query-time-ms"));
    ASSERT_TRUE(result["meta"].contains("result-size-total"));
    ASSERT_TRUE(result["meta"]["query-time-ms"].is_number());
    ASSERT_TRUE(result["meta"]["result-size-total"].is_number());
    EXPECT_GE(result["meta"]["query-time-ms"].get<int64_t>(), 0);
    EXPECT_EQ(result["meta"]["result-size-total"].get<int64_t>(), 1);
  }

  // Case 2: Runtime parameter disabled.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
    auto result = runJSONQuery(kg, query, ad_utility::MediaType::sparqlJson);
    ASSERT_TRUE(result.contains("head"));
    ASSERT_TRUE(result.contains("results"));
    ASSERT_TRUE(result["head"].contains("vars"));
    ASSERT_FALSE(result.contains("meta"));
  }
}
