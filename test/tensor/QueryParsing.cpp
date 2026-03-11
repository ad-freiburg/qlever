
#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "../parser/SparqlAntlrParserTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "TensorTestHelpers.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "util/TensorData.h"
namespace {
using namespace ad_utility;
using namespace sparqlExpression;
using namespace tensorTestHelpers;
using namespace sparqlParserHelpers;
using namespace sparqlParserTestHelpers;
namespace m = matchers;
using Parser = SparqlAutomaticParser;
using namespace std::literals;
using Var = Variable;
auto iri = ad_utility::testing::iri;

auto lit = ad_utility::testing::tripleComponentLiteral;
// _____________________________________________________________________________
TEST(TensorParse, TensorDump) {
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  auto [tensorString, datatypeIri] = tensor.toString();
  EXPECT_THAT(tensorString, R"({"data":[1.0,2.0,3.0],"shape":[3],"type":"float64"})");
  EXPECT_THAT(datatypeIri, "https://w3id.org/rdf-tensor/datatypes#DataTensor");
}
// _____________________________________________________________________________
TEST(TensorParse, TensorConstruction) {
  auto tensor_from_string = TensorData::parseFromString(
      R"({"data":[1.0,2.0,3.0],"shape":[3],"type":"float64"})");
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  EXPECT_EQ(tensor_from_string[0], tensor[0]);
  EXPECT_EQ(tensor_from_string.size(), tensor.size());
  EXPECT_EQ(tensor_from_string.shape(), tensor.shape());
  EXPECT_EQ(tensor_from_string.dtype(), tensor.dtype());

  // also test that parsing an invalid string throws an error
  EXPECT_THROW(TensorData::parseFromString("invalid string"), std::runtime_error);
  EXPECT_THROW(TensorData::parseFromString(R"({"data":[1.0,2.0,3.0],"shape":[3],"type":0})"),
               std::runtime_error);  // invalid type
  EXPECT_THROW(TensorData::parseFromString(R"({"data":[1.0,2.0,3.0],"shape":[3]})"),
               std::runtime_error);  // missing type
  EXPECT_THROW(TensorData::parseFromString(R"({"data":[1.0,2.0,3.0],"type":"float64"})"),
               std::runtime_error);  // missing shape
}
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
TEST_F(TensorQueryTest, TensorQueryConstruction) {
  std::string full_query=R"(
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
}
}  // namespace
