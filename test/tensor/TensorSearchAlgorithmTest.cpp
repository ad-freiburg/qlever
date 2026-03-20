
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
#include "TensorSearchTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TensorSearch.h"
#include "engine/TensorSearchCachedIndex.h"
#include "engine/TensorSearchConfig.h"
#include "index/vocabulary/VocabularyType.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/Variable.h"

using namespace ad_utility::testing;
using namespace TensorTestHelpers;
using namespace TensorSearchTestHelpers;
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
  TensorTestHelpers::makeTestIndex(basename, ttl);

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
      "SELECT ?s ?so ?v ?t ?dist WHERE {"
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
    auto tid = idTable.at(i, 3);
    auto dist = idTable.at(i, 4);
    auto sStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), sid, {});
    auto cidOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), cid, {});
    auto tStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), vid, {});
    auto vStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), vid, {});
    auto distOpt =
        ExportQueryExecutionTrees::idToStringAndType(ql.index(), dist, {});
    std::cout << "Result row " << i << ": subject id " << sStrOpt->first
              << ", candidate id " << cidOpt->first << ", value id "
              << vStrOpt->first << ", distance " << distOpt->first << "\n";
    ASSERT_TRUE(sStrOpt.has_value());
    ASSERT_TRUE(cidOpt.has_value());
    EXPECT_EQ(sStrOpt->first, cidOpt->first);
  }

  // Clean up generated files
  TensorTestHelpers::removeTestIndex(basename);
}
