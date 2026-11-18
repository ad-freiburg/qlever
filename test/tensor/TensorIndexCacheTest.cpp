
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <regex>
#include <thread>
#include <variant>

#include "engine/tensor/TensorIndex.h"
#include "../tensor/TensorTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "TensorIndexTestHelpers.h"
#include "TensorTestHelpers.h"
#include "index/ExportIds.h"
#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"
#include "engine/tensor/TensorIndexCachedIndex.h"
#include "engine/tensor/TensorIndexConfig.h"
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
  TensorIndexCachedIndex::clearCache();

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
  auto tensorIndexOp =
      makeTensorIndex(qec, true, PayloadVariables::all(), param.algo,
                      param.dist, cacheName, 1, false, N);
  auto tensorIndex = static_cast<TensorIndex*>(tensorIndexOp.get());

  // The key is the name if given, otherwise derived from the right child.
  std::string expectedName = tensorIndex->getTensorIndexCacheKey();
  ASSERT_FALSE(expectedName.empty());
  if (cacheName.has_value()) {
    EXPECT_EQ(expectedName, cacheName.value());
  } else {
    EXPECT_THAT(expectedName,
                ::testing::HasSubstr(tensorIndex->onlyForTestingGetRightChild()
                                         ->getCacheKey()));
  }
  AD_EXPECT_THROW_WITH_MESSAGE(TensorIndexCachedIndex::fromKey(expectedName),
                               ::testing::HasSubstr(expectedName));

  // For each result row, assert that the returned candidate equals the
  // original subject (self is nearest neighbor because identical vector
  // exists in the dataset). The index must only be built once and then be
  // reused.
  constexpr size_t cacheTests = 4;
  std::shared_ptr<const TensorIndexCachedIndex> firstIndex;
  for (size_t i = 0; i < cacheTests; i++) {
    auto resultTable = tensorIndex->computeResult(false);
    auto varColMap = tensorIndex->computeVariableToColumnMap();
    const auto* idTable = &resultTable.idTableView();
    EXPECT_EQ(idTable->numRows(), N);
    expectSelfResult(idTable, varColMap, qec, param.reverse);

    auto cache = TensorIndexCachedIndex::fromKey(expectedName);
    EXPECT_EQ(cache->getConfig().dist_, param.dist);
    EXPECT_EQ(cache->getConfig().algo_, param.algo);
    EXPECT_EQ(cache->numVectors(), N);
    EXPECT_EQ(cache->dimension(), N);
    if (i == 0) {
      firstIndex = cache;
    } else {
      EXPECT_EQ(cache.get(), firstIndex.get());
    }
  }

  // A different query (with the same left child) reuses the cached index.
  {
    auto otherOp =
        makeTensorIndex(qec, false, PayloadVariables::all(), param.algo,
                        param.dist, cacheName, 3, false, N);
    auto other = static_cast<TensorIndex*>(otherOp.get());
    EXPECT_EQ(other->getTensorIndexCacheKey(), expectedName);
    auto res = other->computeResult(false);
    EXPECT_EQ(res.idTableView().numRows(), 3 * N);
    EXPECT_EQ(TensorIndexCachedIndex::fromKey(expectedName).get(),
              firstIndex.get());
  }

  // If the name is reused for a different right input, the stale index is
  // replaced instead of being used for the wrong data.
  if (cacheName.has_value()) {
    auto otherOp = makeTensorIndex(
        qec, true, PayloadVariables::all(), param.algo, param.dist, cacheName,
        1, false, N, std::nullopt, "<p1>", "<name>");
    auto other = static_cast<TensorIndex*>(otherOp.get());
    auto res = other->computeResult(false);
    // `<name>` doesn't hold any tensors.
    EXPECT_EQ(res.idTableView().numRows(), 0);
    auto cache = TensorIndexCachedIndex::fromKey(expectedName);
    EXPECT_NE(cache.get(), firstIndex.get());
    EXPECT_EQ(cache->numVectors(), 0);
  }
  TensorIndexCachedIndex::clearCache();
}

INSTANTIATE_TEST_SUITE_P(
    TensorIndexCacheTests, TensorIndexCacheTest,
    ::testing::Values(AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::DOT_PRODUCT,
                                    "FaissDotNamed", true},
                      AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                                    "FaissEuclideanNamed", true},
                      AlgCacheParam{TensorIndexAlgorithm::FAISS_HNSW,
                                    TensorDistanceAlgorithm::COSINE_SIMILARITY,
                                    "FaissHNSWCosineNamed", true},
                      AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::DOT_PRODUCT,
                                    "FaissDot", false},
                      AlgCacheParam{TensorIndexAlgorithm::FAISS_IVF,
                                    TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                                    "FaissEuclidean", false}));
}  // namespace
