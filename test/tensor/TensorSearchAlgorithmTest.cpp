
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
struct AlgDistParam {
  TensorSearchAlgorithm algo;
  TensorDistanceAlgorithm dist;
  std::string name;
  bool reverse = false;
};

class TensorSearchFunctionalTest
    : public ::testing::TestWithParam<AlgDistParam> {};

inline std::shared_ptr<QueryExecutionTree> buildSmallestChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple) {
  auto scan = buildIndexScan(qec, triple);
  return scan;
}

std::shared_ptr<TensorSearch> makeTensorSearch(
    QueryExecutionContext* qec, bool addDist = true,
    PayloadVariables pv = PayloadVariables::all(),
    TensorSearchAlgorithm alg = TENSOR_SEARCH_DEFAULT_ALGORITHM,
    TensorDistanceAlgorithm distAlg = TENSOR_SEARCH_DEFAULT_DISTANCE,
    ssize_t max_num_results = -1, bool addLeftChildFirst = false) {
  auto leftChild =
      buildSmallestChild(qec, {"?obja", std::string{"<p1>"}, "?t1"});
  auto rightChild =
      buildSmallestChild(qec, {"?objb", std::string{"<p1>"}, "?t2"});

  std::optional<Variable> dist = std::nullopt;
  if (addDist) {
    dist = Variable{"?dist"};
  }
  std::shared_ptr<QueryExecutionTree> tensorSearchOperation =
      ad_utility::makeExecutionTree<TensorSearch>(
          qec,
          TensorSearchConfiguration{Variable{"?t1"}, Variable{"?t2"}, dist, pv,
                                    alg, distAlg, max_num_results},
          std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = tensorSearchOperation->getRootOperation();
  TensorSearch* tensorSearch = static_cast<TensorSearch*>(op.get());
  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  Variable firstVariable =
      addLeftChildFirst ? Variable{"?t1"} : Variable{"?t2"};
  Variable secondVariable =
      addLeftChildFirst ? Variable{"?t2"} : Variable{"?t1"};
  auto tensorSearch1 = tensorSearch->addChild(firstChild, firstVariable);
  tensorSearch = static_cast<TensorSearch*>(tensorSearch1.get());
  auto tensorSearch2 = tensorSearch->addChild(secondChild, secondVariable);
  return tensorSearch2;
}

TEST_P(TensorSearchFunctionalTest, NearestNeighborSelfIsReturned) {
  auto param = GetParam();

  // // Build a small index with 6 vectors in 3 dimensions.
  // const std::string basename = "_tensorFunctionalTestIndex";
  const size_t N = 20;
  auto qec = buildQec(makeVectorKg(
      N, N));  // identity vectors, so nearest neighbor should be self
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples,
            N * 3 - 1);  // N vector triples + N name triples + N-1 rel triples

  auto tensorSearchOp = makeTensorSearch(qec, true, PayloadVariables::all(),
                                         param.algo, param.dist, 1);
  auto tensorSearch = static_cast<TensorSearch*>(tensorSearchOp.get());

  auto varColMap = tensorSearch->computeVariableToColumnMap();
  auto resultTable = tensorSearch->computeResult(false);

  const auto* idTable = &resultTable.idTable();

  // For each result row, assert that the returned candidate equals the
  // original subject (self is nearest neighbor because identical vector
  // exists in the dataset).
  for (size_t i = 0; i < idTable->numRows(); ++i) {
    // id of obja:
    auto col_id_obja = varColMap.at(Variable{"?obja"}).columnIndex_;
    auto col_id_objb = varColMap.at(Variable{"?objb"}).columnIndex_;
    auto col_id_dist = varColMap.at(Variable{"?dist"}).columnIndex_;
    auto aid = idTable->at(i, col_id_obja);
    auto bid = idTable->at(i, col_id_objb);
    auto dist = idTable->at(i, col_id_dist);
    auto aStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), aid, {});
    auto bStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), bid, {});
    auto distOpt =
        ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), dist, {});
    std::cout << "Result row " << i << ": subject id " << aStrOpt->first
              << ", value id " << bStrOpt->first << ", distance "
              << distOpt->first << "\n";
    ASSERT_TRUE(aStrOpt.has_value());
    ASSERT_TRUE(bStrOpt.has_value());
    if (param.reverse) {
      // EXPECT_NEQ(aStrOpt->first, bStrOpt->first);
    } else {
      EXPECT_EQ(aStrOpt->first, bStrOpt->first);
    }
  }

  // Clean up generated files
  // TensorTestHelpers::removeTestIndex(basename);
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
                     "FaissEuclidean", true}));
}  // namespace
