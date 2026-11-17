// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <optional>
#include <thread>
#include <vector>

#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "engine/tensor/TensorIndex.h"
#include "engine/tensor/TensorIndexCachedIndex.h"
#include "engine/tensor/TensorIndexConfig.h"
#include "global/Id.h"
#include "global/RuntimeParameters.h"
#include "util/Exception.h"
#include "util/Log.h"

#ifdef QLEVER_USE_TENSOR_BLAS
extern "C" {
#include <cblas.h>
#include <openblas_config.h>
}
#endif
#ifdef QLEVER_TENSOR_PARALLEL
#include <omp.h>
#endif

namespace {
// The maximal number of (query, neighbor) pairs that are materialized at once
// during the search. This bounds the memory of the intermediate results and
// determines how often the cancellation is checked.
constexpr size_t MAX_CANDIDATES_PER_BATCH = size_t{1} << 20;

// Compute the number of query vectors that are processed in one batch.
size_t batchSize(size_t k) {
  return std::max<size_t>(1, MAX_CANDIDATES_PER_BATCH / std::max<size_t>(k, 1));
}
}  // namespace

// ____________________________________________________________________________
size_t TensorIndexImpl::getNumThreads() {
  size_t maxHwConcurrency =
      std::max<size_t>(1, std::thread::hardware_concurrency());
  size_t userPreference =
      getRuntimeParameter<&RuntimeParameters::tensorIndexMaxNumThreads_>();
  if (userPreference == 0 || maxHwConcurrency < userPreference) {
    return maxHwConcurrency;
  }
  return userPreference;
}

// ____________________________________________________________________________
void TensorIndexImpl::initializeGlobalRuntimeParameters() {
  // This function is called at the beginning of the server execution to set the
  // global runtime parameters related to tensor search. This is necessary to
  // ensure that the number of threads used for tensor search is set correctly
  // before any tensor search operations are executed.
#ifdef QLEVER_USE_TENSOR_BLAS
  AD_LOG_INFO << "Using OpenBLAS for tensor operations." << std::endl;
  size_t maxNumThreads = getNumThreads();
  AD_LOG_INFO << "Setting the number of threads for BLAS to " << maxNumThreads
              << " (as specified by the "
              << "tensorIndexMaxNumThreads runtime parameter)" << std::endl;
  openblas_set_num_threads(static_cast<int>(maxNumThreads));
#endif
#ifdef QLEVER_TENSOR_PARALLEL
  // OpenMP is also used by FAISS and by the naive tensor search.
  AD_LOG_INFO << "Using OpenMP for tensor operations." << std::endl;
  size_t numThreads = getNumThreads();
  AD_LOG_INFO << "Setting the number of threads for OpenMP to " << numThreads
              << " (as specified by the "
              << "tensorIndexMaxNumThreads runtime parameter)" << std::endl;
  omp_set_num_threads(static_cast<int>(numThreads));
#endif
#if !defined(QLEVER_USE_TENSOR_BLAS) && !defined(QLEVER_TENSOR_PARALLEL)
  AD_LOG_INFO << "Using the internal (single-threaded) implementation for "
                 "tensor operations."
              << std::endl;
#endif
}

// ____________________________________________________________________________
void TensorIndexImpl::checkCancellation() const {
  if (op_ != nullptr) {
    op_->checkCancellationWrapperForTensorIndexAlgorithms();
  }
}

// ____________________________________________________________________________
void TensorIndexImpl::addWarning(std::string warning) const {
  if (op_ != nullptr) {
    op_->addWarning(std::move(warning));
  } else {
    AD_LOG_WARN << warning << std::endl;
  }
}

// ____________________________________________________________________________
void TensorIndexImpl::appendMatches(IdTable& result,
                                    const std::vector<Match>& matches) const {
  if (matches.empty()) {
    return;
  }
  size_t oldSize = result.numRows();
  result.resize(oldSize + matches.size());
  size_t resultCol = 0;
  auto copyColumns = [&](const IdTableView<0>& input,
                         const qlm::vector<ColumnIndex>& selectedCols,
                         auto getRow) {
    for (ColumnIndex inputCol : selectedCols) {
      auto source = input.getColumn(inputCol);
      auto target = result.getColumn(resultCol++);
      for (size_t i = 0; i < matches.size(); ++i) {
        target[oldSize + i] = source[getRow(matches[i])];
      }
    }
  };
  copyColumns(*params_.idTableLeft_, params_.leftSelectedCols_,
              [](const Match& m) { return m.rowLeft_; });
  copyColumns(*params_.idTableRight_, params_.rightSelectedCols_,
              [](const Match& m) { return m.rowRight_; });
  if (params_.config_.distanceVariable_.has_value()) {
    auto target = result.getColumn(resultCol++);
    for (size_t i = 0; i < matches.size(); ++i) {
      target[oldSize + i] = Id::makeFromDouble(matches[i].dist_);
    }
  }
  AD_CORRECTNESS_CHECK(resultCol == result.numColumns());
}

// ____________________________________________________________________________
Result TensorIndexImpl::makeResult(IdTable result) const {
  return Result{std::move(result),
                qlm::vector<ColumnIndex>{qec_->getAllocator()},
                Result::getMergedLocalVocab(*params_.resultLeft_,
                                            *params_.resultRight_)};
}

namespace {
// Add warnings about rows that were ignored.
template <typename AddWarning>
void warnAboutSkippedRows(const tensorIndex::ExtractedTensors& tensors,
                          std::string_view side, AddWarning addWarning) {
  if (tensors.numNonTensor_ > 0) {
    addWarning(absl::StrCat(tensors.numNonTensor_, " row(s) of the ", side,
                            " input of the tensor search don't contain a "
                            "valid tensor and were ignored."));
  }
  if (tensors.numDimensionMismatch_ > 0) {
    addWarning(absl::StrCat(
        tensors.numDimensionMismatch_, " row(s) of the ", side,
        " input of the tensor search contain a tensor with a dimension "
        "different from ",
        tensors.dim_, " (the dimension of the indexed tensors) and were "
        "ignored."));
  }
}

// For one (deduplicated) left vector, emit the matches for all of its rows.
// `candidates` are pairs of (right vector index, raw score) sorted from best
// to worst. At most `maxResults` matches are emitted per left row.
template <typename Candidates, typename GetRightRows, typename Match>
void emitMatches(ql::span<const size_t> leftRows, const Candidates& candidates,
                 GetRightRows getRightRows, size_t maxResults,
                 TensorDistanceAlgorithm dist, std::vector<Match>& out) {
  for (size_t rowLeft : leftRows) {
    size_t numEmitted = 0;
    for (const auto& [vectorIdx, raw] : candidates) {
      if (numEmitted >= maxResults) {
        break;
      }
      double distance = tensorIndex::rawScoreToDistance(dist, raw);
      for (size_t rowRight : getRightRows(vectorIdx)) {
        if (numEmitted >= maxResults) {
          break;
        }
        out.push_back(Match{rowLeft, rowRight, distance});
        ++numEmitted;
      }
    }
  }
}
}  // namespace

// ____________________________________________________________________________
Result TensorIndexImpl::computeTensorIndexResultFaiss() {
  const auto& config = params_.config_;
  const auto& index = qec_->getIndex();
  auto metric = tensorIndex::getMetricInfo(config.dist_);
  IdTable result{params_.numColumns_, qec_->getAllocator()};
  auto cancellation = [this]() { checkCancellation(); };

  auto faissIndex = TensorIndexCachedIndex::fromKeyOrBuild(
      params_.indexCacheKey_, params_.indexFingerprint_, params_.pinIndex_,
      params_.idTableRight_->getColumn(params_.rightJoinCol_), index,
      params_.resultRight_->localVocab(), config, cancellation);
  if (faissIndex->numVectors() == 0) {
    return makeResult(std::move(result));
  }

  auto left = tensorIndex::extractTensors(
      params_.idTableLeft_->getColumn(params_.leftJoinCol_), index,
      params_.resultLeft_->localVocab(), faissIndex->dimension(),
      metric.normalize_, cancellation);
  warnAboutSkippedRows(left, "left",
                       [this](std::string w) { addWarning(std::move(w)); });
  if (left.empty()) {
    return makeResult(std::move(result));
  }

  size_t maxResults = config.maxResults_;
  // Each vector corresponds to at least one row, so the `maxResults` best rows
  // are always contained in the `maxResults` best vectors.
  size_t k = std::min(maxResults, faissIndex->numVectors());
  // Each left row gets (at most) this many matches.
  result.reserve(left.numRows() *
                 std::min(maxResults, faissIndex->numIndexedRows()));

  std::vector<Match> matches;
  std::vector<std::pair<size_t, float>> candidates;
  candidates.reserve(k);
  size_t numQueriesPerBatch = batchSize(k);
  auto getRightRows = [&faissIndex](size_t idx) {
    return faissIndex->getRows(idx);
  };
  for (size_t start = 0; start < left.numVectors();
       start += numQueriesPerBatch) {
    checkCancellation();
    size_t numQueries = std::min(numQueriesPerBatch, left.numVectors() - start);
    AD_LOG_TIME_START(tensorIndexLookup);
    // The vectors are stored contiguously, so we can search a batch of them
    // with a single call.
    auto batch = faissIndex->search(left.vector(start), numQueries, k);
    AD_LOG_TIME_END(tensorIndexLookup);
    for (size_t q = 0; q < numQueries; ++q) {
      candidates.clear();
      for (size_t j = 0; j < batch.k_; ++j) {
        auto label = batch.labels_[q * batch.k_ + j];
        if (label < 0) {
          // FAISS returns -1 for empty slots.
          continue;
        }
        candidates.emplace_back(static_cast<size_t>(label),
                                batch.rawScores_[q * batch.k_ + j]);
      }
      emitMatches(left.rowsOfVector(start + q), candidates, getRightRows,
                  maxResults, config.dist_, matches);
    }
    appendMatches(result, matches);
    matches.clear();
  }
  return makeResult(std::move(result));
}

// ____________________________________________________________________________
Result TensorIndexImpl::computeTensorIndexResultNaive() {
  const auto& config = params_.config_;
  const auto& index = qec_->getIndex();
  auto metric = tensorIndex::getMetricInfo(config.dist_);
  IdTable result{params_.numColumns_, qec_->getAllocator()};
  auto cancellation = [this]() { checkCancellation(); };

  // Extract all tensors once (instead of once per pair of rows).
  auto right = tensorIndex::extractTensors(
      params_.idTableRight_->getColumn(params_.rightJoinCol_), index,
      params_.resultRight_->localVocab(), std::nullopt, metric.normalize_,
      cancellation);
  warnAboutSkippedRows(right, "right",
                       [this](std::string w) { addWarning(std::move(w)); });
  if (right.empty()) {
    return makeResult(std::move(result));
  }
  auto left = tensorIndex::extractTensors(
      params_.idTableLeft_->getColumn(params_.leftJoinCol_), index,
      params_.resultLeft_->localVocab(), right.dim_, metric.normalize_,
      cancellation);
  warnAboutSkippedRows(left, "left",
                       [this](std::string w) { addWarning(std::move(w)); });
  if (left.empty()) {
    return makeResult(std::move(result));
  }

  size_t maxResults = config.maxResults_;
  size_t numRight = right.numVectors();
  size_t dim = right.dim_;
  size_t k = std::min(maxResults, numRight);
  result.reserve(left.numRows() * std::min(maxResults, right.numRows()));

  // The `k` best right vectors for each left vector of the current batch.
  // Bound both the size of the intermediate results and the amount of work
  // between two cancellation checks.
  constexpr size_t maxWorkPerBatch = size_t{1} << 26;
  size_t numQueriesPerBatch =
      std::clamp<size_t>(maxWorkPerBatch / std::max<size_t>(numRight * dim, 1),
                         1, batchSize(k));
  std::vector<std::pair<size_t, float>> topK(numQueriesPerBatch * k);
  std::vector<Match> matches;
  auto getRightRows = [&right](size_t idx) { return right.rowsOfVector(idx); };

  for (size_t start = 0; start < left.numVectors();
       start += numQueriesPerBatch) {
    checkCancellation();
    auto numQueries = static_cast<int64_t>(
        std::min(numQueriesPerBatch, left.numVectors() - start));
    // Note: No exceptions may be thrown inside of the parallel region.
#ifdef QLEVER_TENSOR_PARALLEL
#pragma omp parallel for schedule(dynamic, 1)
#endif
    for (int64_t q = 0; q < numQueries; ++q) {
      const float* query = left.vector(start + static_cast<size_t>(q));
      std::vector<std::pair<size_t, float>> scores(numRight);
      for (size_t j = 0; j < numRight; ++j) {
        scores[j] = {j, tensorIndex::rawScore(config.dist_, query,
                                              right.vector(j), dim)};
      }
      // Sort by score, break ties by the index of the vector to get a
      // deterministic result.
      auto better = [&metric](const auto& a, const auto& b) {
        if (a.second != b.second) {
          return tensorIndex::isBetter(metric, a.second, b.second);
        }
        return a.first < b.first;
      };
      std::partial_sort(scores.begin(), scores.begin() + k, scores.end(),
                        better);
      std::copy(scores.begin(), scores.begin() + k,
                topK.begin() + static_cast<size_t>(q) * k);
    }
    for (size_t q = 0; q < static_cast<size_t>(numQueries); ++q) {
      ql::span<const std::pair<size_t, float>> candidates{
          topK.data() + q * k, k};
      emitMatches(left.rowsOfVector(start + q), candidates, getRightRows,
                  maxResults, config.dist_, matches);
    }
    appendMatches(result, matches);
    matches.clear();
  }
  return makeResult(std::move(result));
}

// ____________________________________________________________________________
Result TensorIndexImpl::computeTensorIndexResult() {
  const auto& config = params_.config_;
  if (config.maxResults_ == 0 || params_.idTableLeft_->numRows() == 0 ||
      params_.idTableRight_->numRows() == 0) {
    return makeResult(IdTable{params_.numColumns_, qec_->getAllocator()});
  }
  switch (config.algo_) {
    case TensorIndexAlgorithm::FAISS_IVF:
    case TensorIndexAlgorithm::FAISS_HNSW:
      if (!tensorIndex::getMetricInfo(config.dist_).supportedByFaiss_) {
        addWarning(
            "The selected distance metric is not supported by the FAISS-based "
            "tensor search algorithms. The exact (naive) algorithm is used "
            "instead.");
        return computeTensorIndexResultNaive();
      }
      return computeTensorIndexResultFaiss();
    case TensorIndexAlgorithm::NAIVE:
      return computeTensorIndexResultNaive();
  }
  AD_FAIL();
}
