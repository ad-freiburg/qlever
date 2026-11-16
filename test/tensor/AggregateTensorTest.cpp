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
TEST(TensorAggregate, AggreagateCall) {
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
  std::vector<float> expected;
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
               "_tensorAggregateCall");
  std::string full_query;
  full_query += R"(
PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX dta: <https://w3id.org/rdf-tensor/aggregates#>
      SELECT ?s (dta:)";
  full_query += param.aggType;
  full_query += R"((?v) AS ?agg) ?v WHERE  {
      ?s <p1> ?v.
      }
      GROUP BY
      )";
  auto query_plan = qlv->parseAndPlanQuery(full_query);
  auto [qet, qec, parsed] = std::move(query_plan);
  auto res = qet->getResult();

  auto results_query =
      qlv->query(full_query, ad_utility::MediaType::sparqlJson);
  std::cout << results_query << std::endl;
  auto parsed_results = nlohmann::json::parse(results_query);

  EXPECT_TRUE(parsed_results.contains("results"));
  auto val = parsed_results["results"]["bindings"][0]["sim"]["shape"][0]
                 .get<std::string>();
  EXPECT_EQ(std::stoi(val), 3);
}
INSTANTIATE_TEST_SUITE_P(TensorAggregateCallVariation, TensorAggregateCall,
                         ::testing::Values(TensorAggTypeArg{"sum" },
                                           TensorAggTypeArg{"val"},
                                           TensorAggTypeArg{"avg"})

);
}  // namespace
