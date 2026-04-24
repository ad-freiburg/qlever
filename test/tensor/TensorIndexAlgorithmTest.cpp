
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
#include "TensorIndexTestHelpers.h"
#include "TensorTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TensorIndex.h"
#include "engine/TensorIndexCachedIndex.h"
#include "engine/TensorIndexConfig.h"
#include "index/vocabulary/VocabularyType.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/Variable.h"

namespace {  // anonymous namespace to avoid linker problems
using namespace TensorIndexTestHelpers;
using namespace ad_utility::testing;
using namespace TensorTestHelpers;

TEST_P(TensorIndexFunctionalTest, NearestNeighborSelfIsReturned) {
  auto param = GetParam();

  // // Build a small index with 6 vectors in 3 dimensions.

  const size_t N = 20;
  auto qec = buildQec(makeVectorKg(
      N, N));  // identity vectors, so nearest neighbor should be self
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples,
            N * 3 - 1);  // N vector triples + N name triples + N-1 rel triples

  auto tensorIndexOp =
      makeTensorIndex(qec, true, PayloadVariables::all(), param.algo,
                       param.dist, std::nullopt, 1, false, N);

  auto tensorIndex = static_cast<TensorIndex*>(tensorIndexOp.get());

  auto varColMap = tensorIndex->computeVariableToColumnMap();
  auto resultTable = tensorIndex->computeResult(false);

  const auto* idTable = &resultTable.idTable();

  // For each result row, assert that the returned candidate equals the
  // original subject (self is nearest neighbor because identical vector
  // exists in the dataset).
  expectSelfResult(idTable, varColMap, qec, param.reverse);
}

INSTANTIATE_TEST_SUITE_P(
    TensorIndexAlgorithmTests, TensorIndexFunctionalTest,
    ::testing::Values(
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::COSINE_SIMILARITY, "NaiveCosine"},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "NaiveEuclidean", true},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "NaiveInnerProduct"},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissDot"},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissEuclidean", true},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::COSINE_SIMILARITY, "NaiveCosine",
                     false, true},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "NaiveEuclidean", true, true},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "NaiveInnerProduct",
                     false, true},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissDot", false,
                     true},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissEuclidean", true, true},
        AlgDistParam{TensorIndexAlgorithm::FAISS_HSNW,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissHSNWDot",
                     false, true},
        AlgDistParam{TensorIndexAlgorithm::FAISS_HSNW,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissHSNWEuclidean", true, true}));
}  // namespace
