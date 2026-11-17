
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cmath>
#include <cstdlib>
#include <map>
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

TEST_P(TensorIndexFunctionalTest, NearestNeighborSelfIsReturned) {
  auto param = GetParam();

  // // Build a small index with 6 vectors in 3 dimensions.

  const size_t N = 20;
  // identity vectors, so nearest neighbor should be self
  auto qec = buildQec(makeVectorKg(N, N), param.tensorVocab);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples,
            N * 3 - 1);  // N vector triples + N name triples + N-1 rel triples

  auto tensorIndexOp =
      makeTensorIndex(qec, true, PayloadVariables::all(), param.algo,
                       param.dist, std::nullopt, 1, false, N);

  auto tensorIndex = static_cast<TensorIndex*>(tensorIndexOp.get());

  auto varColMap = tensorIndex->computeVariableToColumnMap();
  auto resultTable = tensorIndex->computeResult(false);

  const auto* idTable = &resultTable.idTableView();
  // Each of the N left rows has exactly one nearest neighbor.
  EXPECT_EQ(idTable->numRows(), N);

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
                     "NaiveEuclidean"},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "NaiveInnerProduct"},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissDot"},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissEuclidean"},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::COSINE_SIMILARITY, "NaiveCosine",
                     false, true},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "NaiveEuclidean", false, true},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "NaiveInnerProduct",
                     false, true},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissDot", false,
                     true},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissEuclidean", false, true},
        AlgDistParam{TensorIndexAlgorithm::FAISS_HNSW,
                     TensorDistanceAlgorithm::DOT_PRODUCT, "FaissHNSWDot",
                     false, true},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::ANGULAR_DISTANCE, "NaiveAngular"},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::MANHATTAN_DISTANCE,
                     "NaiveManhattan"},
        AlgDistParam{TensorIndexAlgorithm::NAIVE,
                     TensorDistanceAlgorithm::HAMMING_DISTANCE, "NaiveHamming"},
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::COSINE_SIMILARITY,
                     "FaissCosine"},
        AlgDistParam{TensorIndexAlgorithm::FAISS_HNSW,
                     TensorDistanceAlgorithm::ANGULAR_DISTANCE,
                     "FaissHNSWAngular"},
        // Not supported by FAISS, falls back to the naive algorithm.
        AlgDistParam{TensorIndexAlgorithm::FAISS_IVF,
                     TensorDistanceAlgorithm::MANHATTAN_DISTANCE,
                     "FaissManhattanFallback"},
        AlgDistParam{TensorIndexAlgorithm::FAISS_HNSW,
                     TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE,
                     "FaissHNSWEuclidean", false, true}));

// _____________________________________________________________________________
// Helpers for the tests below.
using Dist = TensorDistanceAlgorithm;
using Algo = TensorIndexAlgorithm;

// The expected distance value (as bound to the distance variable) between two
// vectors, computed in double precision directly from the definition.
double expectedDistance(Dist dist, const std::vector<float>& a,
                        const std::vector<float>& b) {
  double dot = 0, normA = 0, normB = 0, l2 = 0, l1 = 0, hamming = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    dot += double{a[i]} * b[i];
    normA += double{a[i]} * a[i];
    normB += double{b[i]} * b[i];
    l2 += (double{a[i]} - b[i]) * (double{a[i]} - b[i]);
    l1 += std::abs(double{a[i]} - b[i]);
    hamming += a[i] != b[i] ? 1 : 0;
  }
  double cosine = (normA == 0 || normB == 0)
                      ? 0.0
                      : dot / (std::sqrt(normA) * std::sqrt(normB));
  switch (dist) {
    case Dist::DOT_PRODUCT:
      return dot;
    case Dist::COSINE_SIMILARITY:
      return cosine;
    case Dist::ANGULAR_DISTANCE:
      return std::sqrt(std::max(0.0, 2.0 - 2.0 * cosine));
    case Dist::EUCLIDEAN_DISTANCE:
      return std::sqrt(l2);
    case Dist::MANHATTAN_DISTANCE:
      return l1;
    case Dist::HAMMING_DISTANCE:
      return hamming;
  }
  AD_FAIL();
}

bool higherIsBetter(Dist dist) {
  return dist == Dist::DOT_PRODUCT || dist == Dist::COSINE_SIMILARITY;
}

// Parse the index `i` from the subject `<si>`.
size_t subjectIndex(const std::string& subject) {
  return std::stoul(subject.substr(2, subject.size() - 3));
}

// The result of a tensor search: for each left subject the (right subject,
// distance) pairs in the order of the result.
using Matches = std::map<size_t, std::vector<std::pair<size_t, double>>>;

struct SearchResult {
  Matches matches_;
  size_t numRows_;
  size_t numColumns_;
  std::vector<std::string> warnings_;
};

SearchResult runSearch(QueryExecutionContext* qec, Algo algo, Dist dist,
                       size_t k, std::optional<size_t> searchK = std::nullopt,
                       std::optional<size_t> kIVF = std::nullopt,
                       PayloadVariables pv = PayloadVariables::all(),
                       std::string leftPredicate = "<p1>",
                       std::string rightPredicate = "<p1>") {
  auto op = makeTensorIndex(qec, true, pv, algo, dist, std::nullopt, k, false,
                            searchK, kIVF, leftPredicate, rightPredicate);
  auto result = op->computeResult(false);
  auto varColMap = op->computeVariableToColumnMap();
  const auto& table = result.idTableView();
  SearchResult out;
  out.numRows_ = table.numRows();
  out.numColumns_ = table.numColumns();
  EXPECT_EQ(table.numColumns(), op->getResultWidth());
  auto colA = varColMap.at(Variable{"?obja"}).columnIndex_;
  auto colDist = varColMap.at(Variable{"?dist"}).columnIndex_;
  std::optional<ColumnIndex> colB;
  if (varColMap.contains(Variable{"?objb"})) {
    colB = varColMap.at(Variable{"?objb"}).columnIndex_;
  }
  for (size_t i = 0; i < table.numRows(); ++i) {
    auto a = subjectIndex(idToString(qec, table.at(i, colA)));
    size_t b = colB.has_value()
                   ? subjectIndex(idToString(qec, table.at(i, colB.value())))
                   : 0;
    auto distId = table.at(i, colDist);
    EXPECT_EQ(distId.getDatatype(), Datatype::Double);
    out.matches_[a].emplace_back(b, distId.getDouble());
  }
  out.warnings_ = op->collectWarnings();
  return out;
}

// Check that the `result` contains for each of the `vectors` (used as both
// the left and the right side) exactly the `k` nearest neighbors with the
// correct distance values.
void expectExactResult(const SearchResult& result,
                       const std::vector<std::vector<float>>& vectors,
                       Dist dist, size_t k) {
  size_t expectedK = std::min(k, vectors.size());
  ASSERT_EQ(result.numRows_, vectors.size() * expectedK);
  ASSERT_EQ(result.matches_.size(), vectors.size());
  for (const auto& [a, matches] : result.matches_) {
    ASSERT_EQ(matches.size(), expectedK);
    // All distances from `a`, sorted from best to worst.
    std::vector<double> all;
    for (const auto& vec : vectors) {
      all.push_back(expectedDistance(dist, vectors.at(a), vec));
    }
    if (higherIsBetter(dist)) {
      ql::ranges::sort(all, std::greater<>{});
    } else {
      ql::ranges::sort(all);
    }
    for (size_t j = 0; j < matches.size(); ++j) {
      auto [b, d] = matches[j];
      // The bound distance is the distance of the matched pair.
      EXPECT_NEAR(d, expectedDistance(dist, vectors.at(a), vectors.at(b)),
                  1e-3);
      // The j-th match is the j-th best one.
      EXPECT_NEAR(d, all.at(j), 1e-3);
    }
  }
}

class TensorIndexMetricTest : public ::testing::TestWithParam<Dist> {};

// _____________________________________________________________________________
TEST_P(TensorIndexMetricTest, NaiveIsExact) {
  auto dist = GetParam();
  auto vectors = makeRandomVectors(40, 8);
  auto qec = buildQec(makeKgFromVectors(vectors));
  for (size_t k : {1, 5, 40, 100}) {
    expectExactResult(runSearch(qec, Algo::NAIVE, dist, k), vectors, dist, k);
  }
}

// _____________________________________________________________________________
TEST_P(TensorIndexMetricTest, FaissAgreesWithNaive) {
  auto dist = GetParam();
  auto vectors = makeRandomVectors(60, 8, 7);
  auto qec = buildQec(makeKgFromVectors(vectors));
  constexpr size_t k = 5;
  auto naive = runSearch(qec, Algo::NAIVE, dist, k);
  expectExactResult(naive, vectors, dist, k);

  // An IVF index with a single list is an exhaustive (exact) search. This
  // also checks that the distance values of FAISS (e.g. squared L2 distances,
  // inner products of normalized vectors) are converted consistently.
  auto ivf = runSearch(qec, Algo::FAISS_IVF, dist, k, std::nullopt, 1);
  expectExactResult(ivf, vectors, dist, k);
  // IVF with a single list must return the same neighbors in the same order.
  for (const auto& [a, matches] : naive.matches_) {
    const auto& ivfMatches = ivf.matches_.at(a);
    ASSERT_EQ(ivfMatches.size(), matches.size());
    for (size_t j = 0; j < matches.size(); ++j) {
      EXPECT_EQ(ivfMatches[j].first, matches[j].first);
      EXPECT_NEAR(ivfMatches[j].second, matches[j].second, 1e-4);
    }
  }

  // An IVF index where all lists are probed is also exact.
  auto ivfAllProbes = runSearch(qec, Algo::FAISS_IVF, dist, k, 1000, 4);
  expectExactResult(ivfAllProbes, vectors, dist, k);

  // HNSW is approximate, but with a large `efSearch` on this small dataset the
  // recall must be high, and all distances must be correct.
  auto hnsw = runSearch(qec, Algo::FAISS_HNSW, dist, k, 200);
  ASSERT_EQ(hnsw.numRows_, naive.numRows_);
  size_t numCorrect = 0;
  for (const auto& [a, matches] : hnsw.matches_) {
    const auto& expected = naive.matches_.at(a);
    for (const auto& [b, d] : matches) {
      EXPECT_NEAR(d, expectedDistance(dist, vectors.at(a), vectors.at(b)),
                  1e-3);
      numCorrect += ql::ranges::any_of(
          expected, [b = b](const auto& m) { return m.first == b; });
    }
  }
  EXPECT_GE(numCorrect, naive.numRows_ * 9 / 10);
}

INSTANTIATE_TEST_SUITE_P(FaissMetrics, TensorIndexMetricTest,
                         ::testing::Values(Dist::DOT_PRODUCT,
                                           Dist::COSINE_SIMILARITY,
                                           Dist::ANGULAR_DISTANCE,
                                           Dist::EUCLIDEAN_DISTANCE));

// _____________________________________________________________________________
TEST(TensorIndexAlgorithm, NaiveOnlyMetrics) {
  auto vectors = makeRandomVectors(30, 6, 3);
  auto qec = buildQec(makeKgFromVectors(vectors));
  for (auto dist : {Dist::MANHATTAN_DISTANCE, Dist::HAMMING_DISTANCE}) {
    expectExactResult(runSearch(qec, Algo::NAIVE, dist, 4), vectors, dist, 4);
    // The FAISS algorithms fall back to the exact algorithm with a warning.
    for (auto algo : {Algo::FAISS_IVF, Algo::FAISS_HNSW}) {
      auto result = runSearch(qec, algo, dist, 4);
      expectExactResult(result, vectors, dist, 4);
      EXPECT_THAT(result.warnings_,
                  ::testing::Contains(::testing::HasSubstr("not supported")));
    }
  }
}

// _____________________________________________________________________________
TEST(TensorIndexAlgorithm, NumNNLimits) {
  const size_t N = 10;
  auto vectors = makeRandomVectors(N, 4, 11);
  auto qec = buildQec(makeKgFromVectors(vectors));
  for (auto algo : {Algo::NAIVE, Algo::FAISS_IVF, Algo::FAISS_HNSW}) {
    for (size_t k : {1, 3, 10, 25}) {
      auto result = runSearch(qec, algo, Dist::EUCLIDEAN_DISTANCE, k, N, 1);
      EXPECT_EQ(result.numRows_, N * std::min(k, N));
      for (const auto& [a, matches] : result.matches_) {
        EXPECT_EQ(matches.size(), std::min(k, N));
        // The nearest neighbor of a vector is the vector itself.
        EXPECT_EQ(matches.at(0).first, a);
        EXPECT_NEAR(matches.at(0).second, 0.0, 1e-6);
      }
    }
    // `numNN` 0 yields an empty result.
    auto op = makeTensorIndex(qec, true, PayloadVariables::all(), algo,
                              Dist::EUCLIDEAN_DISTANCE, std::nullopt, 0);
    EXPECT_TRUE(op->knownEmptyResult());
    EXPECT_EQ(op->computeResult(false).idTableView().numRows(), 0);
  }
}

// _____________________________________________________________________________
TEST(TensorIndexAlgorithm, PayloadColumns) {
  auto vectors = makeRandomVectors(12, 4, 5);
  auto qec = buildQec(makeKgFromVectors(vectors));
  for (auto algo : {Algo::NAIVE, Algo::FAISS_IVF}) {
    // Only `?objb` as payload (and the right join variable `?t2`).
    auto result =
        runSearch(qec, algo, Dist::COSINE_SIMILARITY, 3, 100, 1,
                  PayloadVariables{std::vector<Variable>{Variable{"?objb"}}});
    // ?obja ?t1 | ?objb ?t2 | ?dist
    EXPECT_EQ(result.numColumns_, 5);
    // The payload column `?objb` belongs to the matched right row: the bound
    // distance is the distance to the vector of `?objb`.
    expectExactResult(result, vectors, Dist::COSINE_SIMILARITY, 3);

    // No payload: only the right join column.
    auto onlyJoinColumn =
        runSearch(qec, algo, Dist::COSINE_SIMILARITY, 3, 100, 1,
                  PayloadVariables{std::vector<Variable>{}});
    EXPECT_EQ(onlyJoinColumn.numColumns_, 4);
    EXPECT_EQ(onlyJoinColumn.numRows_, 12 * 3);
  }
}

// _____________________________________________________________________________
TEST(TensorIndexAlgorithm, EmptyInputs) {
  auto vectors = makeRandomVectors(8, 4, 13);
  auto kg = makeKgFromVectors(vectors);
  kg += "<s0> <name> \"noTensor\" .\n";
  auto qec = buildQec(kg);
  for (auto algo : {Algo::NAIVE, Algo::FAISS_IVF, Algo::FAISS_HNSW}) {
    for (auto dist : {Dist::DOT_PRODUCT, Dist::EUCLIDEAN_DISTANCE}) {
      // Empty left side.
      EXPECT_EQ(runSearch(qec, algo, dist, 3, std::nullopt, std::nullopt,
                          PayloadVariables::all(), "<doesNotExist>", "<p1>")
                    .numRows_,
                0);
      // Empty right side.
      EXPECT_EQ(runSearch(qec, algo, dist, 3, std::nullopt, std::nullopt,
                          PayloadVariables::all(), "<p1>", "<doesNotExist>")
                    .numRows_,
                0);
      // Right side without any tensors.
      auto noTensors = runSearch(qec, algo, dist, 3, std::nullopt,
                                 std::nullopt, PayloadVariables::all(), "<p1>",
                                 "<name>");
      EXPECT_EQ(noTensors.numRows_, 0);
      // Left side without any tensors.
      auto noTensorsLeft = runSearch(qec, algo, dist, 3, std::nullopt,
                                     std::nullopt, PayloadVariables::all(),
                                     "<name>", "<p1>");
      EXPECT_EQ(noTensorsLeft.numRows_, 0);
      EXPECT_THAT(noTensorsLeft.warnings_,
                  ::testing::Contains(::testing::HasSubstr("valid tensor")));
    }
  }
}

// _____________________________________________________________________________
TEST(TensorIndexAlgorithm, DimensionMismatchAndNonTensors) {
  // <s0> ... <s4> have 3-dimensional tensors, <s5> and <s6> 4-dimensional
  // ones, <s7> and <s8> no tensors at all.
  auto vectors = makeRandomVectors(5, 3, 17);
  auto kg = makeKgFromVectors(vectors);
  kg += absl::StrCat("<s5> <p1> ", tensorLiteral({1, 2, 3, 4}), " .\n");
  kg += absl::StrCat("<s6> <p1> ", tensorLiteral({4, 3, 2, 1}), " .\n");
  kg += "<s7> <p1> \"notATensor\" .\n";
  kg += "<s8> <p1> 42 .\n";
  auto qec = buildQec(kg);
  for (auto algo : {Algo::NAIVE, Algo::FAISS_IVF, Algo::FAISS_HNSW}) {
    // The dimension is determined by the first tensor of the right side,
    // tensors with a different dimension and non-tensors are ignored (with a
    // warning) on both sides.
    auto result =
        runSearch(qec, algo, Dist::EUCLIDEAN_DISTANCE, 2, 100, 1);
    EXPECT_EQ(result.numRows_, 5 * 2);
    for (const auto& [a, matches] : result.matches_) {
      EXPECT_LT(a, 5);
      for (const auto& [b, d] : matches) {
        EXPECT_LT(b, 5);
        EXPECT_NEAR(d,
                    expectedDistance(Dist::EUCLIDEAN_DISTANCE, vectors.at(a),
                                     vectors.at(b)),
                    1e-3);
      }
    }
    EXPECT_THAT(result.warnings_,
                ::testing::Contains(::testing::HasSubstr("dimension")));
    EXPECT_THAT(result.warnings_,
                ::testing::Contains(::testing::HasSubstr("valid tensor")));
  }
}

// _____________________________________________________________________________
TEST(TensorIndexAlgorithm, DuplicateTensors) {
  // Several subjects share the same tensor. All of them must be returned as
  // separate matches (also by FAISS, which indexes each tensor only once).
  std::vector<float> v0{1, 0, 0};
  std::vector<float> v1{0, 1, 0};
  std::string kg;
  for (size_t i = 0; i < 6; ++i) {
    kg += absl::StrCat("<s", i, "> <p1> ", tensorLiteral(i < 4 ? v0 : v1),
                       " .\n");
  }
  auto qec = buildQec(kg);
  for (auto algo : {Algo::NAIVE, Algo::FAISS_IVF, Algo::FAISS_HNSW}) {
    auto result = runSearch(qec, algo, Dist::DOT_PRODUCT, 5, 100, 1);
    EXPECT_EQ(result.numRows_, 6 * 5);
    for (const auto& [a, matches] : result.matches_) {
      ASSERT_EQ(matches.size(), 5);
      if (a < 4) {
        // The 4 subjects with the same tensor (dot product 1) come first.
        for (size_t j = 0; j < 4; ++j) {
          EXPECT_LT(matches[j].first, 4);
          EXPECT_NEAR(matches[j].second, 1.0, 1e-6);
        }
        EXPECT_NEAR(matches[4].second, 0.0, 1e-6);
      } else {
        EXPECT_GE(matches[0].first, 4);
        EXPECT_GE(matches[1].first, 4);
        EXPECT_NEAR(matches[2].second, 0.0, 1e-6);
      }
    }
  }
}
}  // namespace
