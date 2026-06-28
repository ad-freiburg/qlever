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
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
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
         [[maybe_unused]] std::string_view expectedPayload,
         std::string predefinedResult,
         boost::beast::http::status status = boost::beast::http::status::ok,
         std::string contentType = "application/sparql-results+json",
         std::exception_ptr mockException = nullptr,
         ad_utility::source_location loc =
             AD_CURRENT_SOURCE_LOC()) -> SendRequestType {
    httpClientTestHelpers::RequestMatchers matchers{
        .url_ = testing::Eq(expectedUrl),
        .method_ = testing::Eq(boost::beast::http::verb::post),
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
                getResultFunctionFactory(expectedUrl, "", jsonResult)};

  auto result = proxyOp.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().size(), 2);
  // Output variables + row variable.
  ASSERT_EQ(result.idTable().numColumns(), 3);
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
                getResultFunctionFactory(expectedUrl, "", jsonResult)};

  auto result = proxyOp.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().size(), 1);
  // 1 output variable + 1 row variable.
  ASSERT_EQ(result.idTable().numColumns(), 2);
}

// Test error handling when HTTP request fails.
TEST_F(ProxyTest, httpErrorStatus) {
  auto config =
      makeConfig("http://example.org/api", {},
                 {{"result", Variable{"?result"}}}, {"row", Variable{"?row"}});

  std::string_view expectedUrl = "http://example.org:80/api";

  Proxy proxyOp{testQec, config, std::nullopt,
                getResultFunctionFactory(
                    expectedUrl, "", "Error",
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
      getResultFunctionFactory(expectedUrl, "", "<html>Error</html>",
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
                getResultFunctionFactory(expectedUrl, "", jsonResult)};

  ASSERT_THROW(proxyOp.computeResultOnlyForTesting(), std::runtime_error);
}
