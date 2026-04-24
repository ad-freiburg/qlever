
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
#include "engine/TensorIndexConfig.h"
#include "engine/TensorIndexCachedIndex.h"
#include "index/vocabulary/VocabularyType.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/Variable.h"

namespace {  // anonymous namespace to avoid linker problems
using namespace TensorIndexTestHelpers;
using namespace ad_utility::testing;
using namespace TensorTestHelpers;
struct AlgCacheParam {
  TensorIndexAlgorithm algo;
  TensorDistanceAlgorithm dist;
  std::string name;
  bool testName;
  bool reverse = false;
};
class TensorIndexCacheTest : public ::testing::TestWithParam<AlgCacheParam> {};

TEST_P(TensorIndexCacheTest, CacheCreatedTest) {
  auto param = GetParam();

  // // Build a small index with 6 vectors in 3 dimensions.
  // const std::string basename = "_tensorFunctionalTestIndex";
  const size_t N = 20;
  auto qec = buildQec(makeVectorKg(
      N, N));  // identity vectors, so nearest neighbor should be self
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples,
            N * 3 - 1);  // N vector triples + N name triples + N-1 rel triples
  std::optional<std::string> cacheName = std::nullopt;
  if (param.testName) {
    cacheName = "TestName" + std::to_string(N) + "_" + param.name;
  }
  AD_LOG_INFO << "Cache name: " << cacheName.value_or("--") << std::endl;
  auto tensorIndexOp = makeTensorIndex(qec, true, PayloadVariables::all(),
                                         param.algo, param.dist, cacheName, 1);
  auto tensorIndex = static_cast<TensorIndex*>(tensorIndexOp.get());

  // For each result row, assert that the returned candidate equals the
  // original subject (self is nearest neighbor because identical vector
  // exists in the dataset).
  constexpr size_t cacheTests = 4;
  std::string expectedName = cacheName.value_or(tensorIndex->getCacheKey());
  AD_LOG_INFO << "Expected Cache name: " << expectedName << std::endl;
  AD_EXPECT_THROW_WITH_MESSAGE(TensorIndexCachedIndex::fromKey(expectedName),
                               ::testing::HasSubstr(expectedName));
  for (auto i = 0; i < cacheTests; i++) {
    auto resultTable = tensorIndex->computeResult(false);
    auto varColMap = tensorIndex->computeVariableToColumnMap();
    const auto* idTable = &resultTable.idTable();
    expectSelfResult(idTable, varColMap, qec, param.reverse);

    auto cache = TensorIndexCachedIndex::fromKey(expectedName);
    EXPECT_EQ(cache.get()->getConfig().dist_, param.dist);
    EXPECT_EQ(cache.get()->getConfig().algo_, param.algo);
  }

  // Clean up generated files
  // TensorTestHelpers::removeTestIndex(basename);
}

INSTANTIATE_TEST_SUITE_P(
    TensorIndexCacheTests, TensorIndexCacheTest,
    ::testing::Values(AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::DOT_PRODUCT,
                                    "FaissDotNamed", true},
                      AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                                    "FaissEuclideanNamed", true, true},
                      AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::DOT_PRODUCT,
                                    "FaissDot", false},
                      AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                                    "FaissEuclidean", false, true}

                      )

);
}  // namespace
