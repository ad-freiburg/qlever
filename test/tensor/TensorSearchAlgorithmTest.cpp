
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
#include "TensorSearchTestHelpers.h"
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

namespace {  // anonymous namespace to avoid linker problems
using namespace TensorSearchTestHelpers;
using namespace ad_utility::testing;
using namespace TensorTestHelpers;

TEST_P(TensorSearchFunctionalTest, NearestNeighborSelfIsReturned) {
  auto param = GetParam();

  // // Build a small index with 6 vectors in 3 dimensions.

  const size_t N = 20;
  auto qec = buildQec(makeVectorKg(
      N, N));  // identity vectors, so nearest neighbor should be self
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples,
            N * 3 - 1);  // N vector triples + N name triples + N-1 rel triples

  auto tensorSearchOp =
      makeTensorSearch(qec, true, PayloadVariables::all(), param.algo,
                       param.dist, std::nullopt, 1);
  auto tensorSearch = static_cast<TensorSearch*>(tensorSearchOp.get());

  auto varColMap = tensorSearch->computeVariableToColumnMap();
  auto resultTable = tensorSearch->computeResult(false);

  const auto* idTable = &resultTable.idTable();

  // For each result row, assert that the returned candidate equals the
  // original subject (self is nearest neighbor because identical vector
  // exists in the dataset).
  expectSelfResult(idTable, varColMap, qec, param.reverse);
}

INSTANTIATE_TEST_SUITE_P(
    TensorSearchAlgorithmTests, TensorSearchFunctionalTest,
    ::testing::Values(
        AlgDistParam{TensorSearchAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::COSINE_SIMILARITY, "NaiveCosine"},
        AlgDistParam{TensorSearchAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "NaiveEuclidean", true},
        AlgDistParam{TensorSearchAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "NaiveInnerProduct"},
        AlgDistParam{TensorSearchAlgorithm::FAISS,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissDot"},
        AlgDistParam{TensorSearchAlgorithm::FAISS,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissEuclidean", true},
        AlgDistParam{TensorSearchAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::COSINE_SIMILARITY, "NaiveCosine",
                     false, true},
        AlgDistParam{TensorSearchAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "NaiveEuclidean", true, true},
        AlgDistParam{TensorSearchAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "NaiveInnerProduct",
                     false, true},
        AlgDistParam{TensorSearchAlgorithm::FAISS,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissDot", false,
                     true},
        AlgDistParam{TensorSearchAlgorithm::FAISS,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissEuclidean", true, true}
        ));
}  // namespace
