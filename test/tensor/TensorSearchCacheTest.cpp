
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
struct AlgCacheParam {
  TensorSearchAlgorithm algo;
  TensorDistanceAlgorithm dist;
  std::string name;
  bool testName;
  bool reverse = false;
};
class TensorSearchCacheTest : public ::testing::TestWithParam<AlgCacheParam> {};

TEST_P(TensorSearchCacheTest, CacheCreatedTest) {
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
  auto tensorSearchOp = makeTensorSearch(qec, true, PayloadVariables::all(),
                                         param.algo, param.dist, cacheName, 1);
  auto tensorSearch = static_cast<TensorSearch*>(tensorSearchOp.get());

  // For each result row, assert that the returned candidate equals the
  // original subject (self is nearest neighbor because identical vector
  // exists in the dataset).
  constexpr size_t cacheTests = 4;
  std::string expectedName = cacheName.value_or(tensorSearch->getCacheKey());
  AD_LOG_INFO << "Expected Cache name: " << expectedName << std::endl;
  AD_EXPECT_THROW_WITH_MESSAGE(TensorSearchCachedIndex::fromKey(expectedName),
                               ::testing::HasSubstr(expectedName));
  for (auto i = 0; i < cacheTests; i++) {
    auto resultTable = tensorSearch->computeResult(false);
    auto varColMap = tensorSearch->computeVariableToColumnMap();
    const auto* idTable = &resultTable.idTable();
    expectSelfResult(idTable, varColMap, qec, param.reverse);

    auto cache = TensorSearchCachedIndex::fromKey(expectedName);
    EXPECT_EQ(cache.get()->getConfig().dist_, param.dist);
    EXPECT_EQ(cache.get()->getConfig().algo_, param.algo);
  }

  // Clean up generated files
  // TensorTestHelpers::removeTestIndex(basename);
}

INSTANTIATE_TEST_SUITE_P(
    TensorSearchCacheTests, TensorSearchCacheTest,
    ::testing::Values(AlgCacheParam{TensorSearchAlgorithm::FAISS,
                                    TensorDistanceAlgorithm::DOT_PRODUCT,
                                    "FaissDotNamed", true},
                      AlgCacheParam{TensorSearchAlgorithm::FAISS,
                                    TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                                    "FaissEuclideanNamed", true, true},
                      AlgCacheParam{TensorSearchAlgorithm::FAISS,
                                    TensorDistanceAlgorithm::DOT_PRODUCT,
                                    "FaissDot", false},
                      AlgCacheParam{TensorSearchAlgorithm::FAISS,
                                    TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                                    "FaissEuclidean", false, true}

                      )

);
}  // namespace
