// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/Proxy.h"
#include "engine/ValuesForTesting.h"
#include "global/RuntimeParameters.h"
#include "index/ExportIds.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlTriple.h"
#include "util/AllocatorWithLimit.h"
#include "util/GTestHelpers.h"
#include "util/HttpClientTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"
#include "util/TripleComponentTestHelpers.h"
#include "util/http/HttpUtils.h"

// Fixture that sets up a test index and a factory for producing mocks for the
// `sendRequestFunction` needed by the `Proxy`.
class ProxyTest : public ::testing::Test {
 protected:
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ad_utility::AllocatorWithLimit<Id> testAllocator =
      ad_utility::testing::makeAllocator();

  // Factory for generating mocks of the `sendHttpOrHttpsRequest` function.
  static auto constexpr getResultFunctionFactory =
      [](std::string_view expectedUrl,
         testing::Matcher<std::string_view> payloadMatcher,
         std::string predefinedResult,
         boost::beast::http::status status = boost::beast::http::status::ok,
         std::string contentType = "application/sparql-results+json",
         std::exception_ptr mockException = nullptr,
         ad_utility::source_location loc =
             AD_CURRENT_SOURCE_LOC()) -> SendRequestType {
    httpClientTestHelpers::RequestMatchers matchers{
        .url_ = testing::Eq(expectedUrl),
        .method_ = testing::Eq(boost::beast::http::verb::post),
        .postData_ = std::move(payloadMatcher),
        .contentType_ = testing::Eq("application/sparql-results+json"),
        .accept_ = testing::Eq("application/sparql-results+json")};
    return httpClientTestHelpers::getResultFunctionFactory(
        predefinedResult, contentType, status, matchers, mockException, loc);
  };

  // Generate a JSON result for testing.
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

// Helper to create a ProxyConfiguration for tests.
auto makeConfig(
    std::string endpoint,
    std::vector<std::pair<std::string, Variable>> inputVariables,
    std::vector<std::pair<std::string, Variable>> outputVariables,
    std::pair<std::string, Variable> rowVariable,
    std::vector<std::pair<std::string, std::string>> parameters = {}) {
  return parsedQuery::ProxyConfiguration{
      std::move(endpoint), std::move(inputVariables),
      std::move(outputVariables), std::move(rowVariable),
      std::move(parameters)};
}

// Test basic methods of class `Proxy`.
TEST_F(ProxyTest, basicMethods) {
  // Construct a ProxyConfiguration with no input variables.
  auto config =
      makeConfig("http://example.org/api", {},
                 {{"result", Variable{"?result"}}}, {"row", Variable{"?row"}});

  // Create an operation from this (no child operation).
  Proxy proxyOp{testQec, config, std::nullopt};

  // Test the basic methods.
  ASSERT_EQ(proxyOp.getDescriptor(), "Proxy to http://example.org/api");
  ASSERT_TRUE(ql::starts_with(proxyOp.getCacheKey(), "PROXY "))
      << proxyOp.getCacheKey();
  // No input variables and no child: result width is output variables + row.
  ASSERT_EQ(proxyOp.getResultWidth(), 2);
  ASSERT_EQ(proxyOp.getMultiplicity(0), 1.0f);
  ASSERT_EQ(proxyOp.getSizeEstimateBeforeLimit(), 100'000);
  ASSERT_EQ(proxyOp.getCostEstimate(), 1'000'000);
  using V = Variable;
  ASSERT_THAT(proxyOp.computeVariableToColumnMap(),
              ::testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {V{"?result"}, makePossiblyUndefinedColumn(0)},
                  {V{"?row"}, makePossiblyUndefinedColumn(1)}}));
  ASSERT_FALSE(proxyOp.knownEmptyResult());

  // No child operation means empty children.
  ASSERT_TRUE(proxyOp.getChildren().empty());
}

// Test that multiple output variables are mapped correctly.
TEST_F(ProxyTest, multipleOutputVariables) {
  auto config = makeConfig(
      "http://example.org/api", {},
      {{"a", Variable{"?x"}}, {"b", Variable{"?y"}}, {"c", Variable{"?z"}}},
      {"row", Variable{"?row"}});

  Proxy proxyOp{testQec, config, std::nullopt};

  // 3 output variables + 1 row variable.
  ASSERT_EQ(proxyOp.getResultWidth(), 4);
  using V = Variable;
  ASSERT_THAT(proxyOp.computeVariableToColumnMap(),
              ::testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {V{"?x"}, makePossiblyUndefinedColumn(0)},
                  {V{"?y"}, makePossiblyUndefinedColumn(1)},
                  {V{"?z"}, makePossiblyUndefinedColumn(2)},
                  {V{"?row"}, makePossiblyUndefinedColumn(3)}}));
}

// Helper to generate JSON result with row variable (1-based indices).
static std::string genJsonResultWithRow(
    std::string_view rowVar, std::vector<std::string_view> vars,
    std::vector<std::pair<size_t, std::vector<std::string_view>>> rows) {
  nlohmann::json res;
  std::vector<std::string> allVars;
  allVars.push_back(std::string(rowVar));
  for (const auto& v : vars) {
    allVars.push_back(std::string(v));
  }
  res["head"]["vars"] = allVars;
  res["results"]["bindings"] = nlohmann::json::array();

  for (const auto& [rowIdx1Based, values] : rows) {
    nlohmann::json binding;
    binding[rowVar] = {
        {"type", "literal"},
        {"value", std::to_string(rowIdx1Based)},
        {"datatype", "http://www.w3.org/2001/XMLSchema#integer"}};
    for (size_t j = 0; j < std::min(values.size(), vars.size()); ++j) {
      binding[vars[j]] = {{"type", "uri"}, {"value", values[j]}};
    }
    res["results"]["bindings"].push_back(binding);
  }
  return res.dump();
}

// Test `computeResult` with a simple response (no child).
TEST_F(ProxyTest, computeResultNoChild) {
  auto config = makeConfig("http://example.org/api", {},
                           {{"x", Variable{"?x"}}, {"y", Variable{"?y"}}},
                           {"row", Variable{"?row"}});

  std::string_view expectedUrl = "http://example.org:80/api";
  std::string jsonResult = genJsonResultWithRow(
      "row", {"x", "y"},
      {{1, {"http://example.org/1", "http://example.org/a"}},
       {2, {"http://example.org/2", "http://example.org/b"}}});

  Proxy proxyOp{testQec, config, std::nullopt,
                getResultFunctionFactory(expectedUrl, testing::_, jsonResult)};

  auto result = proxyOp.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTableView().size(), 2);
  // Output variables + row variable.
  ASSERT_EQ(result.idTableView().numColumns(), 3);
}

// Test `computeResult` with URL parameters.
TEST_F(ProxyTest, computeResultWithParams) {
  auto config = makeConfig(
      "http://example.org/api", {}, {{"result", Variable{"?result"}}},
      {"row", Variable{"?row"}}, {{"op", "add"}, {"version", "1"}});

  // Expected URL should include the parameters.
  std::string_view expectedUrl = "http://example.org:80/api?op=add&version=1";
  std::string jsonResult =
      genJsonResultWithRow("row", {"result"}, {{1, {"http://example.org/42"}}});

  Proxy proxyOp{testQec, config, std::nullopt,
                getResultFunctionFactory(expectedUrl, testing::_, jsonResult)};

  auto result = proxyOp.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTableView().size(), 1);
  // 1 output variable + 1 row variable.
  ASSERT_EQ(result.idTableView().numColumns(), 2);
}

// Test error handling when HTTP request fails.
TEST_F(ProxyTest, httpErrorStatus) {
  auto config =
      makeConfig("http://example.org/api", {},
                 {{"result", Variable{"?result"}}}, {"row", Variable{"?row"}});

  std::string_view expectedUrl = "http://example.org:80/api";

  Proxy proxyOp{testQec, config, std::nullopt,
                getResultFunctionFactory(
                    expectedUrl, testing::_, "Error",
                    boost::beast::http::status::internal_server_error)};

  ASSERT_THROW(proxyOp.computeResultOnlyForTesting(), std::runtime_error);
}

// Test error handling when content type is wrong.
TEST_F(ProxyTest, wrongContentType) {
  auto config =
      makeConfig("http://example.org/api", {},
                 {{"result", Variable{"?result"}}}, {"row", Variable{"?row"}});

  std::string_view expectedUrl = "http://example.org:80/api";

  Proxy proxyOp{
      testQec, config, std::nullopt,
      getResultFunctionFactory(expectedUrl, testing::_, "<html>Error</html>",
                               boost::beast::http::status::ok, "text/html")};

  ASSERT_THROW(proxyOp.computeResultOnlyForTesting(), std::runtime_error);
}

// Test error when response is missing the row variable.
TEST_F(ProxyTest, missingRowVariable) {
  auto config =
      makeConfig("http://example.org/api", {},
                 {{"result", Variable{"?result"}}}, {"row", Variable{"?row"}});

  std::string_view expectedUrl = "http://example.org:80/api";
  // Response without the row variable.
  std::string jsonResult =
      genJsonResult({"result"}, {{"http://example.org/1"}});

  Proxy proxyOp{testQec, config, std::nullopt,
                getResultFunctionFactory(expectedUrl, testing::_, jsonResult)};

  ASSERT_THROW(proxyOp.computeResultOnlyForTesting(), std::runtime_error);
}

// The strings of the `result` as exported, row by row, `std::nullopt` for an
// undefined value.
static std::vector<std::vector<std::optional<std::string>>> toStrings(
    const Index& index, const Result& result) {
  std::vector<std::vector<std::optional<std::string>>> rows;
  const auto& table = result.idTableView();
  for (size_t i = 0; i < table.size(); ++i) {
    auto& row = rows.emplace_back();
    for (size_t j = 0; j < table.numColumns(); ++j) {
      auto opt = ql::exportIds::idToStringAndType(index, table(i, j),
                                                  result.localVocab());
      row.push_back(opt.has_value() ? std::optional{opt.value().first}
                                    : std::nullopt);
    }
  }
  return rows;
}

// Test `computeResult` with a child: the input bindings of the child are sent
// as SPARQL JSON (an IRI from the index, literals from the local vocabulary, an
// encoded integer, a blank node, no binding for an undefined value) and the
// response rows are joined with the child rows via the row variable.
TEST_F(ProxyTest, computeResultWithChild) {
  using V = Variable;
  auto getId = ad_utility::testing::makeGetId(testQec->getIndex());
  LocalVocab localVocab;
  auto localId = [this, &localVocab](std::string s) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(
            std::move(s), testQec->getLocalVocabContext())));
  };
  std::vector<std::vector<IntOrId>> childRows{
      {getId("<x>"), localId("\"hallo\"@de")},
      {ad_utility::testing::IntId(7), Id::makeUndefined()},
      {Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0)), localId("\"x\"")}};
  IdTable childTable = makeIdTableFromVector(childRows);
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      testQec, std::move(childTable),
      std::vector<std::optional<Variable>>{V{"?a"}, V{"?b"}}, false,
      std::vector<ColumnIndex>{}, std::move(localVocab));

  // The expected payload, with the 1-based row index in each binding.
  auto intLit = [](std::string value) {
    return nlohmann::json{{"type", "literal"},
                          {"value", std::move(value)},
                          {"datatype", XSD_INTEGER_TYPE}};
  };
  nlohmann::json expectedPayload = {
      {"head", {{"vars", {"row", "a", "b"}}}},
      {"results",
       {{"bindings",
         {{{"row", intLit("1")},
           {"a", {{"type", "uri"}, {"value", "x"}}},
           {"b",
            {{"type", "literal"}, {"value", "hallo"}, {"xml:lang", "de"}}}},
          {{"row", intLit("2")},
           {"a",
            {{"type", "literal"}, {"value", "7"}, {"datatype", XSD_INT_TYPE}}}},
          {{"row", intLit("3")},
           {"a", {{"type", "bnode"}, {"value", "bn0"}}},
           {"b", {{"type", "literal"}, {"value", "x"}}}}}}}}};
  auto payloadMatcher = testing::ResultOf(
      [](std::string_view s) { return nlohmann::json::parse(s); },
      testing::Eq(expectedPayload));

  // The response has two rows for child row 1 and one for child row 3.
  std::string jsonResult =
      genJsonResultWithRow("row", {"o"},
                           {{3, {"http://example.org/3"}},
                            {1, {"http://example.org/1"}},
                            {1, {"http://example.org/1b"}}});
  auto config =
      makeConfig("http://example.org/api", {{"a", V{"?a"}}, {"b", V{"?b"}}},
                 {{"o", V{"?o"}}}, {"row", V{"?row"}});
  Proxy proxyOp{testQec, config, child,
                getResultFunctionFactory("http://example.org:80/api",
                                         payloadMatcher, jsonResult)};
  ASSERT_TRUE(proxyOp.isConstructed());
  ASSERT_EQ(proxyOp.getChildren().size(), 1);
  ASSERT_EQ(proxyOp.getResultWidth(), 4);

  // The columns are those of the child, then the output variable, then the row
  // variable.
  auto varToCol = proxyOp.computeVariableToColumnMap();
  EXPECT_EQ(varToCol.at(V{"?a"}).columnIndex_, 0);
  EXPECT_EQ(varToCol.at(V{"?b"}).columnIndex_, 1);
  EXPECT_EQ(varToCol.at(V{"?o"}).columnIndex_, 2);
  EXPECT_EQ(varToCol.at(V{"?row"}).columnIndex_, 3);

  auto result = proxyOp.computeResultOnlyForTesting();
  using Row = std::vector<std::optional<std::string>>;
  EXPECT_THAT(toStrings(testQec->getIndex(), result),
              testing::ElementsAre(
                  Row{"_:bn0", "\"x\"", "<http://example.org/3>", "3"},
                  Row{"<x>", "\"hallo\"@de", "<http://example.org/1>", "1"},
                  Row{"<x>", "\"hallo\"@de", "<http://example.org/1b>", "1"}));
}

// Test that a response row with a row index outside of the child rows is an
// error.
TEST_F(ProxyTest, invalidRowIndex) {
  using V = Variable;
  std::vector<std::vector<IntOrId>> childRows{{ad_utility::testing::IntId(1)}};
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      testQec, makeIdTableFromVector(childRows),
      std::vector<std::optional<Variable>>{V{"?a"}});
  auto config = makeConfig("http://example.org/api", {{"a", V{"?a"}}},
                           {{"o", V{"?o"}}}, {"row", V{"?row"}});
  for (size_t rowIndex : {size_t{0}, size_t{2}}) {
    Proxy proxyOp{
        testQec, config, child,
        getResultFunctionFactory(
            "http://example.org:80/api", testing::_,
            genJsonResultWithRow("row", {"o"}, {{rowIndex, {"http://e/1"}}}))};
    AD_EXPECT_THROW_WITH_MESSAGE(proxyOp.computeResultOnlyForTesting(),
                                 testing::HasSubstr("invalid row index"));
  }
}

// Test that a `Proxy` with input variables but without a child (the enclosing
// graph pattern does not bind the input variables) throws when computed, and
// that `addChild` checks the variables of the child.
TEST_F(ProxyTest, inputVariablesAndChild) {
  using V = Variable;
  auto config =
      makeConfig("http://example.org/api", {{"a", V{"?a"}}, {"b", V{"?b"}}},
                 {{"o", V{"?o"}}}, {"row", V{"?row"}});
  Proxy proxyOp{
      testQec, config, std::nullopt,
      getResultFunctionFactory("http://example.org:80/api", testing::_, "")};
  ASSERT_FALSE(proxyOp.isConstructed());
  AD_EXPECT_THROW_WITH_MESSAGE(
      proxyOp.computeResultOnlyForTesting(),
      testing::HasSubstr("?a, ?b of the qlproxy service are not all bound"));

  // Only a child that binds all input variables can be added.
  auto child = [this](std::vector<std::optional<Variable>> variables) {
    IdTable table{variables.size(), testQec->getAllocator()};
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        testQec, std::move(table), std::move(variables));
  };
  EXPECT_FALSE(
      proxyOp.bindsAllInputVariables(child({V{"?a"}})->getVariableColumns()));
  EXPECT_TRUE(proxyOp.bindsAllInputVariables(
      child({V{"?a"}, V{"?b"}})->getVariableColumns()));
  EXPECT_THROW(proxyOp.addChild(child({V{"?a"}})), ad_utility::Exception);
  EXPECT_TRUE(proxyOp.addChild(child({V{"?a"}, V{"?b"}}))->isConstructed());

  // A child that binds an output variable or the row variable is an error.
  AD_EXPECT_THROW_WITH_MESSAGE(
      proxyOp.addChild(child({V{"?a"}, V{"?b"}, V{"?o"}})),
      testing::HasSubstr("?o of the parameter `qlproxy:output-o` is already "
                         "bound by the enclosing graph pattern"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      proxyOp.addChild(child({V{"?a"}, V{"?b"}, V{"?row"}})),
      testing::HasSubstr("?row of the parameter `qlproxy:output-row`"));
}

// Test the parsing of the configuration triples of a `SERVICE qlproxy:` into a
// `ProxyConfiguration`.
TEST(ProxyQuery, configuration) {
  using namespace parsedQuery;
  using V = Variable;
  auto triple = [](std::string_view parameter, TripleComponent object) {
    return SparqlTriple{
        TripleComponent{V{"?config"}},
        ad_utility::testing::iri(absl::StrCat(
            "<https://qlever.cs.uni-freiburg.de/qlproxy/", parameter, ">")),
        std::move(object)};
  };
  auto lit = [](std::string_view s) {
    return TripleComponent{TripleComponent::Literal::fromStringRepresentation(
        absl::StrCat("\"", s, "\""))};
  };
  auto iri = [](std::string_view s) {
    return TripleComponent{ad_utility::testing::iri(s)};
  };
  auto parse =
      [&triple](const std::vector<std::pair<std::string_view, TripleComponent>>&
                    parameters) {
        ProxyQuery query;
        for (const auto& [parameter, object] : parameters) {
          query.addParameter(triple(parameter, object));
        }
        return query;
      };

  // A complete configuration.
  auto config = parse({{"endpoint", iri("<http://e/api>")},
                       {"input-first", TripleComponent{V{"?x1"}}},
                       {"input-second", TripleComponent{V{"?x2"}}},
                       {"output-result", TripleComponent{V{"?r"}}},
                       {"output-row", TripleComponent{V{"?row"}}},
                       {"param-op", lit("add")}})
                    .toConfiguration();
  EXPECT_EQ(config.endpoint_, "http://e/api");
  EXPECT_THAT(config.inputVariables_,
              testing::ElementsAre(testing::Pair("first", V{"?x1"}),
                                   testing::Pair("second", V{"?x2"})));
  EXPECT_THAT(config.outputVariables_,
              testing::ElementsAre(testing::Pair("result", V{"?r"})));
  EXPECT_EQ(config.rowVariable_, std::pair(std::string{"row"}, V{"?row"}));
  EXPECT_THAT(config.parameters_,
              testing::ElementsAre(testing::Pair("op", "add")));

  // Missing or invalid parameters.
  auto expectError =
      [&parse](const std::vector<std::pair<std::string_view, TripleComponent>>&
                   params,
               std::string_view message) {
        AD_EXPECT_THROW_WITH_MESSAGE(parse(params).toConfiguration(),
                                     testing::HasSubstr(message));
      };
  expectError({{"output-result", TripleComponent{V{"?r"}}},
               {"output-row", TripleComponent{V{"?row"}}}},
              "Missing required parameter `<endpoint>`");
  expectError({{"endpoint", iri("<http://e/api>")},
               {"output-row", TripleComponent{V{"?row"}}}},
              "At least one output variable is required");
  expectError({{"endpoint", iri("<http://e/api>")},
               {"output-result", TripleComponent{V{"?r"}}}},
              "The row variable is required");
  expectError({{"endpoint", lit("http://e/api")}},
              "The parameter `<endpoint>` expects an IRI");
  expectError({{"input-x", lit("x")}}, "has to be a variable");
  expectError({{"input-", TripleComponent{V{"?x"}}}},
              "The input parameter name cannot be empty");
  expectError({{"output-", TripleComponent{V{"?x"}}}},
              "The output parameter name cannot be empty");
  expectError({{"param-", lit("x")}}, "The URL parameter name cannot be empty");
  expectError({{"param-op", iri("<http://e/add>")}},
              "The parameter `<param-op>` expects a literal value");
  expectError({{"bogus", lit("x")}}, "Unsupported parameter `bogus`");
  expectError({{"endpoint", iri("<http://e/api>")},
               {"output-a", TripleComponent{V{"?x"}}},
               {"output-row", TripleComponent{V{"?x"}}}},
              "The variable ?x is used for more than one of the parameters");
  expectError({{"endpoint", iri("<http://e/api>")},
               {"output-a", TripleComponent{V{"?x"}}},
               {"output-b", TripleComponent{V{"?x"}}},
               {"output-row", TripleComponent{V{"?row"}}}},
              "The variable ?x is used for more than one of the parameters");

  // No graph patterns inside the SERVICE.
  AD_EXPECT_THROW_WITH_MESSAGE(
      ProxyQuery{}.addGraph(GraphPatternOperation{BasicGraphPattern{}}),
      testing::HasSubstr("does not support nested graph patterns"));
}
