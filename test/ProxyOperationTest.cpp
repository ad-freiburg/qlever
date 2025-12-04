// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/ProxyOperation.h"
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
// `sendRequestFunction` needed by the `ProxyOperation`.
class ProxyOperationTest : public ::testing::Test {
 protected:
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ad_utility::AllocatorWithLimit<Id> testAllocator =
      ad_utility::testing::makeAllocator();

  // Factory for generating mocks of the `sendHttpOrHttpsRequest` function.
  static auto constexpr getResultFunctionFactory =
      [](std::string_view expectedUrl, std::string_view expectedPayload,
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

// Test basic methods of class `ProxyOperation`.
TEST_F(ProxyOperationTest, basicMethods) {
  // Construct a ProxyConfiguration by hand.
  parsedQuery::ProxyConfiguration config;
  config.endpoint_ = "http://example.org/api";
  config.resultVariables_ = {{"result", Variable{"?result"}}};
  config.payloadVariables_ = {};
  config.parameters_ = {};

  // Create an operation from this (no child operation).
  ProxyOperation proxyOp{testQec, config, std::nullopt};

  // Test the basic methods.
  ASSERT_EQ(proxyOp.getDescriptor(), "Proxy to http://example.org/api");
  ASSERT_TRUE(ql::starts_with(proxyOp.getCacheKey(), "PROXY "))
      << proxyOp.getCacheKey();
  ASSERT_EQ(proxyOp.getResultWidth(), 1);
  ASSERT_EQ(proxyOp.getMultiplicity(0), 1.0f);
  ASSERT_EQ(proxyOp.getSizeEstimateBeforeLimit(), 100'000);
  ASSERT_EQ(proxyOp.getCostEstimate(), 1'000'000);
  using V = Variable;
  ASSERT_THAT(proxyOp.computeVariableToColumnMap(),
              ::testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {V{"?result"}, makePossiblyUndefinedColumn(0)}}));
  ASSERT_FALSE(proxyOp.knownEmptyResult());

  // No child operation means empty children.
  ASSERT_TRUE(proxyOp.getChildren().empty());
}

// Test that multiple result variables are mapped correctly.
TEST_F(ProxyOperationTest, multipleResultVariables) {
  parsedQuery::ProxyConfiguration config;
  config.endpoint_ = "http://example.org/api";
  config.resultVariables_ = {
      {"a", Variable{"?x"}}, {"b", Variable{"?y"}}, {"c", Variable{"?z"}}};
  config.payloadVariables_ = {};
  config.parameters_ = {};

  ProxyOperation proxyOp{testQec, config, std::nullopt};

  ASSERT_EQ(proxyOp.getResultWidth(), 3);
  using V = Variable;
  ASSERT_THAT(proxyOp.computeVariableToColumnMap(),
              ::testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {V{"?x"}, makePossiblyUndefinedColumn(0)},
                  {V{"?y"}, makePossiblyUndefinedColumn(1)},
                  {V{"?z"}, makePossiblyUndefinedColumn(2)}}));
}

// Test `computeResult` with a simple response.
TEST_F(ProxyOperationTest, computeResult) {
  parsedQuery::ProxyConfiguration config;
  config.endpoint_ = "http://example.org/api";
  config.resultVariables_ = {{"x", Variable{"?x"}}, {"y", Variable{"?y"}}};
  config.payloadVariables_ = {};
  config.parameters_ = {};

  std::string_view expectedUrl = "http://example.org:80/api";
  std::string jsonResult = genJsonResult(
      {"x", "y"}, {{"http://example.org/1", "http://example.org/a"},
                   {"http://example.org/2", "http://example.org/b"}});

  ProxyOperation proxyOp{testQec, config, std::nullopt,
                         getResultFunctionFactory(expectedUrl, "", jsonResult)};

  auto result = proxyOp.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().size(), 2);
  ASSERT_EQ(result.idTable().numColumns(), 2);
}

// Test `computeResult` with URL parameters.
TEST_F(ProxyOperationTest, computeResultWithParams) {
  parsedQuery::ProxyConfiguration config;
  config.endpoint_ = "http://example.org/api";
  config.resultVariables_ = {{"result", Variable{"?result"}}};
  config.payloadVariables_ = {};
  config.parameters_ = {{"op", "add"}, {"version", "1"}};

  // Expected URL should include the parameters.
  std::string_view expectedUrl = "http://example.org:80/api?op=add&version=1";
  std::string jsonResult =
      genJsonResult({"result"}, {{"http://example.org/42"}});

  ProxyOperation proxyOp{testQec, config, std::nullopt,
                         getResultFunctionFactory(expectedUrl, "", jsonResult)};

  auto result = proxyOp.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().size(), 1);
  ASSERT_EQ(result.idTable().numColumns(), 1);
}

// Test error handling when HTTP request fails.
TEST_F(ProxyOperationTest, httpErrorStatus) {
  parsedQuery::ProxyConfiguration config;
  config.endpoint_ = "http://example.org/api";
  config.resultVariables_ = {{"result", Variable{"?result"}}};
  config.payloadVariables_ = {};
  config.parameters_ = {};

  std::string_view expectedUrl = "http://example.org:80/api";

  ProxyOperation proxyOp{
      testQec, config, std::nullopt,
      getResultFunctionFactory(
          expectedUrl, "", "Error",
          boost::beast::http::status::internal_server_error)};

  ASSERT_THROW(proxyOp.computeResultOnlyForTesting(), std::runtime_error);
}

// Test error handling when content type is wrong.
TEST_F(ProxyOperationTest, wrongContentType) {
  parsedQuery::ProxyConfiguration config;
  config.endpoint_ = "http://example.org/api";
  config.resultVariables_ = {{"result", Variable{"?result"}}};
  config.payloadVariables_ = {};
  config.parameters_ = {};

  std::string_view expectedUrl = "http://example.org:80/api";

  ProxyOperation proxyOp{
      testQec, config, std::nullopt,
      getResultFunctionFactory(expectedUrl, "", "<html>Error</html>",
                               boost::beast::http::status::ok, "text/html")};

  ASSERT_THROW(proxyOp.computeResultOnlyForTesting(), std::runtime_error);
}
