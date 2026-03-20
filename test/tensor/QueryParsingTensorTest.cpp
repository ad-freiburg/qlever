// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "../parser/SparqlAntlrParserTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "TensorTestHelpers.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "rdfTypes/TensorData.h"
namespace {
using namespace ad_utility;
using namespace sparqlExpression;
using namespace TensorTestHelpers;
using namespace sparqlParserHelpers;
using namespace sparqlParserTestHelpers;
namespace m = matchers;
using Parser = SparqlAutomaticParser;
using namespace std::literals;
using Var = Variable;
auto iri = ad_utility::testing::iri;

auto lit = ad_utility::testing::tripleComponentLiteral;
// _____________________________________________________________________________
TEST(TensorQueryCall, TensorCall) {
  using namespace m::builtInCall;
  // test if the makeCosineSimilarityExpression is correctly called and returns
  // a result without errors.

  auto expectFunctionCall = ExpectCompleteParse<&Parser::functionCall>{};
  auto expectFunctionCallFails = ExpectParseFails<&Parser::functionCall>{};
  auto tensorf = absl::StrCat("<", TENSOR_FUNCTION_PREFIX.second);
  // Tensor functions
  expectFunctionCall(absl::StrCat(tensorf, "cosineSimilarity>(?x,?y)"),
                     matchNary(&makeTensorCosineSimilarityExpression,
                               Variable{"?x"}, Variable{"?y"}));
  expectFunctionCall(absl::StrCat(tensorf, "dotProduct>(?x,?y)"),
                     matchNary(&makeTensorDotProductExpression, Variable{"?x"},
                               Variable{"?y"}));
  // TODO: also for the other tensor functions!
}
// _____________________________________________________________________________
TEST_F(TensorQueryTest, TensorEnd2EndConstruction) {
  std::string full_query = R"(
PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
PREFIX dta: <https://w3id.org/rdf-tensor/aggregates#>
      SELECT ?s (dtf:cosineSimilarity("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":\"float64\"}"^^dt:DataTensor, ?v) AS ?sim) ?v WHERE  { 
      ?s <p1> ?v.
      }
      ORDER BY DESC(?sim)      
      )";
  auto query_plan = qlv().parseAndPlanQuery(full_query);
  auto [qet, qec, parsed] = std::move(query_plan);
  auto res = qet->getResult();

  auto results_query =
      qlv().query(full_query, ad_utility::MediaType::sparqlJson);
  std::cout << results_query << std::endl;
  auto parsed_results = nlohmann::json::parse(results_query);

  EXPECT_TRUE(parsed_results.contains("results"));
  auto val = parsed_results["results"]["bindings"][0]["sim"]["value"]
                 .get<std::string>();
  EXPECT_FLOAT_EQ(std::stof(val), 1.0f);
}
}  // namespace
