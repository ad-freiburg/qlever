
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2polyline.h>

#include <cstdlib>
#include <fstream>
#include <regex>
#include <thread>
#include <variant>

#include "../tensor/TensorTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "TensorTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TensorSearch.h"
#include "engine/TensorSearchCachedIndex.h"
#include "engine/TensorSearchConfig.h"
#include "index/vocabulary/VocabularyType.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/Variable.h"

namespace {
using namespace tensorTestHelpers;
using qlever::Qlever;

// Helper to build a small deterministic dataset of vectors. The returned
// Turtle contains subjects <s0> .. <sN-1> with predicate <p1> and a
// tensor literal as required by the tensor datatype.
static std::string makeVectorKg(size_t n, size_t dim = 3) {
  std::string out;
  for (size_t i = 0; i < n; ++i) {
    std::vector<double> vec(dim, 0.0);
    vec[i % dim] = 1.0;
    std::string data = R"({\"data\":[)";
    for (size_t d = 0; d < dim; ++d) {
      data += absl::StrCat(vec[d]);
      if (d + 1 < dim) data += ",";
    }
    data +=
        R"(],\"shape\":[)" + std::to_string(dim) + R"(],\"type\":\"float64\"})";
    out += absl::StrCat(
        "<s", i, "> <p1> \"", data,
        "\"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .\n");
  }
  return out;
}
}  // namespace

struct AlgDistParam {
  TensorSearchAlgorithm algo;
  TensorDistanceAlgorithm dist;
  std::string name;
};

class TensorSearchFunctionalTest
    : public ::testing::TestWithParam<AlgDistParam> {};

TEST_P(TensorSearchFunctionalTest, NearestNeighborSelfIsReturned) {
  auto param = GetParam();

  // Build a small index with 6 vectors in 3 dimensions.
  const std::string basename = "_tensorFunctionalTestIndex";
  const auto ttl = makeVectorKg(6, 7);
  tensorTestHelpers::makeTestIndex(basename, ttl);

  // Create a Qlever instance using the generated index.
  qlever::EngineConfig cfg;
  cfg.baseName_ = basename;
  Qlever ql(cfg);

  // Compose SPARQL SERVICE config triples depending on algorithm/distance.
  std::string algoTriple;
  if (param.algo == TensorSearchAlgorithm::ANNOY) {
    algoTriple = "tensorSearch:algorithm tensorSearch:annoy ;\n";
  } else {
    algoTriple = "tensorSearch:algorithm tensorSearch:naive ;\n";
  }
  std::string distTriple;
  switch (param.dist) {
    case TensorDistanceAlgorithm::COSINE_SIMILARITY:
      distTriple = "tensorSearch:distance tensorSearch:cosine ;\n";
      break;
    case TensorDistanceAlgorithm::DOT_PRODUCT:
      distTriple = "tensorSearch:distance tensorSearch:dot ;\n";
      break;
    case TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE:
      distTriple = "tensorSearch:distance tensorSearch:euclidean ;\n";
      break;
    default:
      distTriple = "";
  }

  const std::string query = absl::StrCat(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>\n"
      "SELECT ?s ?so ?v ?dist WHERE {"
      "  ?s <p1> ?v ."
      "  SERVICE tensorSearch: {",
      " _:config tensorSearch:numNN 1 ;", algoTriple, distTriple,
      "tensorSearch:left ?v ;"
      "tensorSearch:bindDistance ?dist ;"
      "tensorSearch:right ?t .",
      " { ?so <p1> ?t . }"
      "}"
      "}");

  // Plan and execute the query.
  auto plan = ql.parseAndPlanQuery(query);
  auto& qet = *std::get<0>(plan);
  auto& parsed = std::get<2>(plan);
  auto result = qet.getResult(false);
  const auto& idTable = result->idTable();

  // For each result row, assert that the returned candidate equals the
  // original subject (self is nearest neighbor because identical vector
  // exists in the dataset).
  for (size_t i = 0; i < idTable.numRows(); ++i) {
    auto sid = idTable.at(i, 0);
    auto cid = idTable.at(i, 1);
    auto vid = idTable.at(i, 2);
    auto dist = idTable.at(i, 3);
    auto sStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), sid, {});
    auto vStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), vid, {});
    auto distOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), dist, {});
    auto cidOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), cid, {});
    std::cout << "Result row " << i << ": subject id " << sStrOpt->first
              << ", candidate id " << cidOpt->first << ", value id " << vStrOpt->first
              << ", distance " << distOpt->first << "\n";
    ASSERT_TRUE(sStrOpt.has_value());
    ASSERT_TRUE(cidOpt.has_value());
    EXPECT_EQ(sStrOpt->first, cidOpt->first);
  }

  // Clean up generated files
  tensorTestHelpers::removeTestIndex(basename);
}

INSTANTIATE_TEST_SUITE_P(
    DefaultAndAnnoy, TensorSearchFunctionalTest,
    ::testing::Values(AlgDistParam{TensorSearchAlgorithm::NAIVE,
                                   TensorDistanceAlgorithm::COSINE_SIMILARITY,
                                   "default_cosine"},
                      AlgDistParam{TensorSearchAlgorithm::ANNOY,
                                   TensorDistanceAlgorithm::COSINE_SIMILARITY,
                                   "annoy_cosine"},
                      AlgDistParam{TensorSearchAlgorithm::ANNOY,
                                   TensorDistanceAlgorithm::DOT_PRODUCT,
                                   "annoy_dot"},
                      AlgDistParam{TensorSearchAlgorithm::ANNOY,
                                   TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                                   "annoy_euclid"}),
    [](auto const& info) { return info.param.name; });
