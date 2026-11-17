// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>

#include "../parser/SparqlAntlrParserTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "TensorTestHelpers.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/tensor/aggregate/TensorAggregate.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"
#include "parser/TensorIndexQuery.h"

namespace {
using namespace sparqlExpression;
using namespace sparqlParserHelpers;
using namespace sparqlParserTestHelpers;
namespace m = matchers;
using Parser = SparqlAutomaticParser;
using namespace std::literals;
using ad_utility::testing::iri;
using VocabularyType = ad_utility::VocabularyType;

const std::string tensorf = absl::StrCat("<", TENSOR_FUNCTION_PREFIX.second);
const std::string tensora = absl::StrCat("<", TENSOR_AGGREGATE_PREFIX.second);

// _____________________________________________________________________________
TEST(TensorQueryCall, TensorFunctions) {
  using namespace m::builtInCall;
  auto expectFunctionCall = ExpectCompleteParse<&Parser::functionCall>{};
  auto expectFunctionCallFails = ExpectParseFails<&Parser::functionCall>{};

  // Unary functions.
  expectFunctionCall(absl::StrCat(tensorf, "tensor>(?x)"),
                     matchUnary(&makeTensorFromStringExpression));
  expectFunctionCall(absl::StrCat(tensorf, "norm2>(?x)"),
                     matchUnary(&makeTensorNormExpression));

  // Binary functions.
  auto expectBinary = [&](std::string_view name, auto makeFunction,
                          ad_utility::source_location l =
                              AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    expectFunctionCall(absl::StrCat(tensorf, name, ">(?x,?y)"),
                       matchNary(makeFunction, Variable{"?x"}, Variable{"?y"}));
    // Wrong number of arguments.
    expectFunctionCallFails(absl::StrCat(tensorf, name, ">(?x)"));
    expectFunctionCallFails(absl::StrCat(tensorf, name, ">(?x,?y,?z)"));
    // DISTINCT is only allowed for aggregates.
    expectFunctionCallFails(absl::StrCat(tensorf, name, ">(DISTINCT ?x,?y)"));
  };
  expectBinary("add", &makeTensorAddExpression);
  expectBinary("subtract", &makeTensorSubtractExpression);
  expectBinary("multiply", &makeTensorMultiplyExpression);
  expectBinary("divide", &makeTensorDivideExpression);
  expectBinary("dotProduct", &makeTensorDotProductExpression);
  expectBinary("cosineSimilarity", &makeTensorCosineSimilarityExpression);
  expectBinary("euclideanDistance", &makeTensorEuclideanDistanceExpression);

  expectFunctionCallFails(absl::StrCat(tensorf, "norm2>(?x,?y)"));
  expectFunctionCallFails(absl::StrCat(tensorf, "tensor>()"));
  // Unknown functions.
  expectFunctionCallFails(absl::StrCat(tensorf, "unknownFunction>(?x)"));
  expectFunctionCallFails(absl::StrCat(tensora, "unknownAggregate>(?x)"));
  // The aggregates are not tensor functions and vice versa.
  expectFunctionCallFails(absl::StrCat(tensorf, "sum>(?x)"));
  expectFunctionCallFails(absl::StrCat(tensora, "add>(?x,?y)"));
}

// _____________________________________________________________________________
TEST(TensorQueryCall, TensorAggregates) {
  using namespace m::builtInCall;
  using namespace ::testing;
  auto expectFunctionCall = ExpectCompleteParse<&Parser::functionCall>{};
  auto expectFunctionCallFails = ExpectParseFails<&Parser::functionCall>{};

  using Status = SparqlExpression::AggregateStatus;
  auto aggregateStatus = [](Status status) {
    return Pointee(AD_PROPERTY(SparqlExpression, isAggregate, Eq(status)));
  };

  auto expectAggregate = [&](std::string_view name, auto makeFunction,
                             ad_utility::source_location l =
                                 AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto make = [makeFunction](SparqlExpression::Ptr child) {
      return makeFunction(std::move(child), false);
    };
    expectFunctionCall(
        absl::StrCat(tensora, name, ">(?x)"),
        AllOf(matchUnary(make), aggregateStatus(Status::NonDistinctAggregate)));
    expectFunctionCall(
        absl::StrCat(tensora, name, ">(DISTINCT ?x)"),
        AllOf(matchUnary(make), aggregateStatus(Status::DistinctAggregate)));
    expectFunctionCallFails(absl::StrCat(tensora, name, ">(?x,?y)"));
  };
  expectAggregate("sum", &makeTensorSumExpr);
  expectAggregate("avg", &makeTensorAvgExpr);
  expectAggregate("var", &makeTensorVarExpr);
  expectAggregate("std", &makeTensorStdExpr);
}

// _____________________________________________________________________________
TEST(TensorIndexQuery, MagicPredicate) {
  using parsedQuery::TensorIndexQuery;
  TensorIndexQuery query{SparqlTriple{Variable{"?a"},
                                      iri("<tensor-nearest-neighbors:5>"),
                                      Variable{"?b"}}};
  EXPECT_EQ(query.maxResults_, 5u);
  EXPECT_EQ(query.left_, Variable{"?a"});
  EXPECT_EQ(query.right_, Variable{"?b"});
  EXPECT_NO_THROW(query.validate());
  auto config = query.toTensorIndexConfiguration();
  EXPECT_EQ(config.maxResults_, 5u);
  EXPECT_EQ(config.left_, Variable{"?a"});
  EXPECT_EQ(config.right_, Variable{"?b"});
  EXPECT_EQ(query.name(), "tensor search");

  // The number of results is mandatory.
  EXPECT_ANY_THROW(
      TensorIndexQuery(SparqlTriple{Variable{"?a"},
                                    iri("<tensor-nearest-neighbors:>"),
                                    Variable{"?b"}}));
  EXPECT_ANY_THROW(
      TensorIndexQuery(SparqlTriple{Variable{"?a"},
                                    iri("<tensor-nearest-neighbors:x>"),
                                    Variable{"?b"}}));
  // Both subject and object have to be variables.
  EXPECT_ANY_THROW(TensorIndexQuery(SparqlTriple{
      iri("<x>"), iri("<tensor-nearest-neighbors:5>"), Variable{"?b"}}));
}

// _____________________________________________________________________________
TEST(TensorIndexQuery, Parameters) {
  using parsedQuery::TensorIndexException;
  using parsedQuery::TensorIndexQuery;
  using TC = TripleComponent;
  auto config = Variable{"?config"};
  auto add = [&](TensorIndexQuery& query, std::string_view parameter,
                 TC object) {
    query.addParameter(SparqlTriple{
        config, iri(absl::StrCat("<", parameter, ">")), std::move(object)});
  };

  TensorIndexQuery query;
  add(query, "left", Variable{"?a"});
  add(query, "right", Variable{"?b"});
  add(query, "bindDistance", Variable{"?d"});
  add(query, "numNN", TC{int64_t{7}});
  add(query, "searchK", TC{int64_t{20}});
  add(query, "kIVF", TC{int64_t{4}});
  add(query, "nNeighbors", TC{int64_t{16}});
  add(query, "algorithm", iri("<hnsw>"));
  add(query, "distance", iri("<cosine>"));
  add(query, "payload", iri("<all>"));
  // The full IRI is also accepted.
  add(query, absl::StrCat(TENSOR_INDEX_IRI.substr(1, TENSOR_INDEX_IRI.size() - 2),
                          "left"),
      Variable{"?a2"});
  EXPECT_EQ(query.left_, Variable{"?a2"});
  EXPECT_EQ(query.right_, Variable{"?b"});
  EXPECT_EQ(query.distanceVariable_, Variable{"?d"});
  EXPECT_EQ(query.maxResults_, 7u);
  EXPECT_EQ(query.searchK_, 20u);
  EXPECT_EQ(query.kIVF_, 4u);
  EXPECT_EQ(query.nNeighbors_, 16u);
  EXPECT_EQ(query.algo_, TensorIndexAlgorithm::FAISS_HNSW);
  EXPECT_EQ(query.dist_, TensorDistanceAlgorithm::COSINE_SIMILARITY);
  EXPECT_TRUE(query.payloadVariables_.isAll());

  // The other algorithms and distances.
  add(query, "algorithm", iri("<naive>"));
  EXPECT_EQ(query.algo_, TensorIndexAlgorithm::NAIVE);
  add(query, "algorithm", iri("<ivf>"));
  EXPECT_EQ(query.algo_, TensorIndexAlgorithm::FAISS_IVF);
  for (auto [name, dist] :
       std::vector<std::pair<std::string, TensorDistanceAlgorithm>>{
           {"angular", TensorDistanceAlgorithm::ANGULAR_DISTANCE},
           {"dot", TensorDistanceAlgorithm::DOT_PRODUCT},
           {"euclidean", TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE},
           {"manhattan", TensorDistanceAlgorithm::MANHATTAN_DISTANCE},
           {"hamming", TensorDistanceAlgorithm::HAMMING_DISTANCE}}) {
    add(query, "distance", iri(absl::StrCat("<", name, ">")));
    EXPECT_EQ(query.dist_, dist);
  }

  // Invalid parameters.
  auto lit = ad_utility::testing::tripleComponentLiteral;
  EXPECT_THROW(add(query, "numNN", TC{int64_t{-1}}), TensorIndexException);
  EXPECT_THROW(add(query, "numNN", lit("\"5\"")), TensorIndexException);
  EXPECT_THROW(add(query, "searchK", TC{int64_t{-3}}), TensorIndexException);
  EXPECT_THROW(add(query, "kIVF", iri("<x>")), TensorIndexException);
  EXPECT_THROW(add(query, "nNeighbors", TC{1.5}), TensorIndexException);
  EXPECT_THROW(add(query, "algorithm", iri("<faiss>")), TensorIndexException);
  EXPECT_THROW(add(query, "algorithm", TC{int64_t{1}}), TensorIndexException);
  EXPECT_THROW(add(query, "distance", iri("<l3>")), TensorIndexException);
  EXPECT_THROW(add(query, "payload", iri("<some>")), TensorIndexException);
  EXPECT_THROW(add(query, "experimentalRightCacheName", iri("<x>")),
               TensorIndexException);
  EXPECT_THROW(add(query, "unknownParameter", Variable{"?x"}),
               TensorIndexException);
  EXPECT_ANY_THROW(add(query, "left", iri("<notAVariable>")));

  // The cache name is only supported by the FAISS algorithms.
  add(query, "experimentalRightCacheName", lit("\"cache\""));
  EXPECT_EQ(query.rightCacheName_, "cache");
  add(query, "algorithm", iri("<naive>"));
  EXPECT_THROW(query.toTensorIndexConfiguration(), TensorIndexException);
}

// _____________________________________________________________________________
TEST(TensorIndexQuery, Validate) {
  using parsedQuery::TensorIndexException;
  using parsedQuery::TensorIndexQuery;
  auto config = Variable{"?config"};
  auto add = [&](TensorIndexQuery& query, std::string_view parameter,
                 TripleComponent object) {
    query.addParameter(SparqlTriple{
        config, iri(absl::StrCat("<", parameter, ">")), std::move(object)});
  };
  {
    // Missing `<left>` and `<right>`.
    TensorIndexQuery query;
    EXPECT_THROW(query.validate(), TensorIndexException);
    add(query, "left", Variable{"?a"});
    EXPECT_THROW(query.validate(), TensorIndexException);
    add(query, "right", Variable{"?b"});
    EXPECT_NO_THROW(query.validate());
    // Without a child graph pattern, all payload variables are selected.
    auto conf = query.toTensorIndexConfiguration();
    EXPECT_TRUE(conf.payloadVariables_.isAll());
    EXPECT_EQ(conf.algo_, TENSOR_INDEX_DEFAULT_ALGORITHM);
    EXPECT_EQ(conf.dist_, TENSOR_INDEX_DEFAULT_DISTANCE);
    // With a maximum number of results, the right side has to be declared
    // inside the `SERVICE`.
    add(query, "numNN", TripleComponent{int64_t{3}});
    EXPECT_THROW(query.validate(), TensorIndexException);
  }
  {
    // `<payload>` requires the right side to be declared inside the `SERVICE`.
    TensorIndexQuery query;
    add(query, "left", Variable{"?a"});
    add(query, "right", Variable{"?b"});
    add(query, "payload", Variable{"?p"});
    EXPECT_THROW(query.validate(), TensorIndexException);
  }
}

// _____________________________________________________________________________
struct IndexParameter {
  VocabularyType vocabType;
};
using TensorQueryVocab =
    TensorTestHelpers::TensorQueryTest<::testing::TestWithParam<IndexParameter>>;
TEST_P(TensorQueryVocab, TensorEnd2EndFunctions) {
  auto param = GetParam();
  std::string query = R"(
PREFIX dt: <https://w3id.org/rdf-tensor/datatypes#>
PREFIX dtf: <https://w3id.org/rdf-tensor/functions#>
      SELECT ?s ?v
        (dtf:cosineSimilarity("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":\"float32\"}"^^dt:DataTensor, ?v) AS ?sim)
        (dtf:dotProduct(?v, ?v) AS ?dot)
        (dtf:norm2(dtf:add(?v, ?v)) AS ?norm)
        (dtf:euclideanDistance(?v, dtf:tensor("{\"data\":[1.0,2.0],\"shape\":[2],\"type\":\"float32\"}")) AS ?mismatch)
      WHERE {
      ?s <p1> ?v.
      }
      ORDER BY DESC(?sim)
      )";
  auto resultString =
      qlv(param.vocabType).query(query, ad_utility::MediaType::sparqlJson);
  auto result = nlohmann::json::parse(resultString);

  ASSERT_TRUE(result.contains("results"));
  const auto& bindings = result["results"]["bindings"];
  // See `dummyTurtle` in `TensorTestHelpers.h`.
  ASSERT_EQ(bindings.size(), 3u) << resultString;
  const auto& first = bindings[0];
  EXPECT_FLOAT_EQ(std::stof(first["sim"]["value"].get<std::string>()), 1.0f);
  EXPECT_FLOAT_EQ(std::stof(first["dot"]["value"].get<std::string>()), 14.0f);
  EXPECT_FLOAT_EQ(std::stof(first["norm"]["value"].get<std::string>()),
                  2 * std::sqrt(14.0f));
  // Shape mismatch -> unbound.
  for (const auto& binding : bindings) {
    EXPECT_FALSE(binding.contains("mismatch")) << resultString;
  }
}
INSTANTIATE_TEST_SUITE_P(
    TensorQueryVocabVariant, TensorQueryVocab,
    ::testing::Values(
        IndexParameter{VocabularyType(VocabularyType::Enum::OnDiskCompressed)},
        IndexParameter{
            VocabularyType(VocabularyType::Enum::OnDiskCompressedTensorSplit)},
        IndexParameter{
            VocabularyType(VocabularyType::Enum::OnDiskCompressedGeoSplit)}));
}  // namespace
