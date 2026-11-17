// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>

#include "../parser/SparqlAntlrParserTestHelpers.h"
#include "TensorTestHelpers.h"
#include "engine/sparqlExpressions/NaryExpression.h"
namespace {
using namespace ad_utility;
using namespace sparqlExpression;
using namespace TensorTestHelpers;
using namespace sparqlParserHelpers;
using namespace sparqlParserTestHelpers;
namespace m = matchers;
using Parser = SparqlAutomaticParser;
using namespace std::literals;
// _____________________________________________________________________________
TEST(TensorAggregate, AggregateCall) {
  using namespace m::builtInCall;
  // test if the makeCosineSimilarityExpression is correctly called and returns
  // a result without errors.

  auto expectFunctionCall = ExpectCompleteParse<&Parser::functionCall>{};
  auto expectFunctionCallFails = ExpectParseFails<&Parser::functionCall>{};
  auto tensora = absl::StrCat("<", TENSOR_AGGREGATE_PREFIX.second);
  // Tensor aggregate functions
  expectFunctionCall(absl::StrCat(tensora, "sum>(?x)"),
                     matchUnary(&makeTensorSumExpr));
  expectFunctionCall(absl::StrCat(tensora, "avg>(?x)"),
                     matchUnary(&makeTensorAvgExpr));
  expectFunctionCall(absl::StrCat(tensora, "var>(?x)"),
                     matchUnary(&makeTensorVarExpr));
}
// _____________________________________________________________________________

struct TensorAggTypeArg {
  std::string aggType;
  // Expected result for each of the groups `<s1>`, `<s2>`, `<s3>`.
  std::vector<std::vector<float>> expected;
};
typedef TensorQueryTest<::testing::TestWithParam<TensorAggTypeArg>>
    TensorAggregateCall;
TEST_P(TensorAggregateCall, TensorEnd2EndAggregateCall) {
  auto param = GetParam();
  auto qlv =
      qlvFresh(VocabularyType(VocabularyType::Enum::OnDiskCompressed), R"(
  <s1> <p2> <o1> .
  <s3> <p1> "{\"data\":[1.0,2.0,-2.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s3> <p1> "{\"data\":[1.0,1.0,1.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s3> <p1> "{\"data\":[1.0,-1.0,2.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p2> <o2> .
  <s1> <p1> "{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s1> <p1> "{\"data\":[3.0,2.0,1.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p1> "{\"data\":[1.0,1.0,3.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p2> <o2> .
)",
               "_tensorAggregateCall" + param.aggType);
  std::string full_query = absl::StrCat(R"(
PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX dta: <https://w3id.org/rdf-tensor/aggregates#>
      SELECT ?s (dta:)",
                                        param.aggType, R"((?v) AS ?agg) WHERE {
      ?s <p1> ?v.
      }
      GROUP BY ?s ORDER BY ?s
      )");

  auto results_query =
      qlv->query(full_query, ad_utility::MediaType::sparqlJson);
  auto parsed_results = nlohmann::json::parse(results_query);

  ASSERT_TRUE(parsed_results.contains("results"));
  const auto& bindings = parsed_results["results"]["bindings"];
  ASSERT_EQ(bindings.size(), param.expected.size()) << results_query;

  for (size_t i = 0; i < bindings.size(); ++i) {
    auto tensor = TensorData::parseFromString(
        bindings[i]["agg"]["value"].get<std::string>());
    const auto& expected = param.expected[i];
    ASSERT_EQ(tensor.shape(), std::vector<size_t>{expected.size()});
    auto data = tensor.tensorData();
    for (size_t j = 0; j < expected.size(); ++j) {
      EXPECT_NEAR(data[j], expected[j], 1e-5)
          << "group " << i << ", element " << j;
    }
  }
}
INSTANTIATE_TEST_SUITE_P(
    TensorAggregateCallVariation, TensorAggregateCall,
    ::testing::Values(
        TensorAggTypeArg{"sum", {{4, 4, 4}, {1, 1, 3}, {3, 2, 1}}},
        TensorAggTypeArg{"avg", {{2, 2, 2}, {1, 1, 3}, {1, 2.f / 3, 1.f / 3}}},
        // Sample variance (divided by `n - 1`, zero for a single value), see
        // `TensorVarFinalOperation`.
        TensorAggTypeArg{"var", {{4, 4, 4}, {0, 0, 0}, {1.5, 1, 0.5}}}),
    [](const auto& info) { return info.param.aggType; });
}  // namespace
