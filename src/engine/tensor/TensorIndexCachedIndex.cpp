// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include "engine/tensor/TensorIndexCachedIndex.h"

#include <absl/strings/str_cat.h>
#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/utils/distances.h>

#include <algorithm>
#include <cmath>
#include <limits>

#include "global/Constants.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "util/Cache.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/Synchronized.h"

namespace tensorIndex {

// ____________________________________________________________________________
std::optional<ad_utility::TensorData> idToTensor(const Index& index, Id id,
                                                 const LocalVocab& localVocab) {
  // Uses the precomputed tensors of the `TensorDataVocabulary` for
  // `VocabIndex` ids if available, and parses the literal otherwise.
  return ql::exportIds::idToTensorData(index.getImpl(), id, localVocab);
}

// ____________________________________________________________________________
MetricInfo getMetricInfo(TensorDistanceAlgorithm dist) {
  using enum TensorDistanceAlgorithm;
  switch (dist) {
    case DOT_PRODUCT:
      return {true, false, true};
    case COSINE_SIMILARITY:
      return {true, true, true};
    case ANGULAR_DISTANCE:
      // Ranked by the cosine similarity (higher is better), the bound value
      // is the (monotonically decreasing) angular distance.
      return {true, true, true};
    case EUCLIDEAN_DISTANCE:
      return {false, false, true};
    case MANHATTAN_DISTANCE:
      return {false, false, false};
    case HAMMING_DISTANCE:
      return {false, false, false};
  }
  AD_FAIL();
}

// ____________________________________________________________________________
float rawScore(TensorDistanceAlgorithm dist, const float* a, const float* b,
               size_t dim) {
  using enum TensorDistanceAlgorithm;
  switch (dist) {
    case DOT_PRODUCT:
    case COSINE_SIMILARITY:
    case ANGULAR_DISTANCE:
      // For cosine and angular, the vectors are already normalized.
      return faiss::fvec_inner_product(a, b, dim);
    case EUCLIDEAN_DISTANCE:
      return faiss::fvec_L2sqr(a, b, dim);
    case MANHATTAN_DISTANCE:
      return faiss::fvec_L1(a, b, dim);
    case HAMMING_DISTANCE: {
      size_t count = 0;
      for (size_t i = 0; i < dim; ++i) {
        count += static_cast<size_t>(a[i] != b[i]);
      }
      return static_cast<float>(count);
    }
  }
  AD_FAIL();
}

// ____________________________________________________________________________
double rawScoreToDistance(TensorDistanceAlgorithm dist, float raw) {
  using enum TensorDistanceAlgorithm;
  switch (dist) {
    case ANGULAR_DISTANCE:
      // Euclidean distance of the normalized vectors: sqrt(2 - 2 cos).
      return std::sqrt(std::max(0.0, 2.0 - 2.0 * static_cast<double>(raw)));
    case EUCLIDEAN_DISTANCE:
      // FAISS (and our raw score) uses squared L2 distances.
      return std::sqrt(std::max(0.0, static_cast<double>(raw)));
    default:
      return static_cast<double>(raw);
  }
}

// ____________________________________________________________________________
ExtractedTensors extractTensors(ql::span<const Id> column, const Index& index,
                                const LocalVocab& localVocab,
                                std::optional<size_t> requiredDimension,
                                bool normalize,
                                const std::function<void()>& checkCancellation) {
  ExtractedTensors result;
  // For each row, the index of its vector in `result.data_`, or `invalid`.
  constexpr size_t invalid = std::numeric_limits<size_t>::max();
  constexpr size_t mismatch = invalid - 1;
  std::vector<size_t> vectorOfRow(column.size(), invalid);
  // Each distinct `Id` is only resolved (and stored) once.
  ad_utility::HashMap<Id, size_t> idToVector;
  std::optional<size_t> dim = requiredDimension;
  size_t numVectors = 0;
  // The number of rows per vector, used for the CSR layout below.
  std::vector<size_t> rowCounts;

  for (size_t row = 0; row < column.size(); ++row) {
    if (checkCancellation && (row % 4096 == 0)) {
      checkCancellation();
    }
    Id id = column[row];
    if (id.isUndefined()) {
      ++result.numUndefined_;
      continue;
    }
    if (auto it = idToVector.find(id); it != idToVector.end()) {
      if (it->second == invalid) {
        ++result.numNonTensor_;
      } else if (it->second == mismatch) {
        ++result.numDimensionMismatch_;
      } else {
        vectorOfRow[row] = it->second;
        ++rowCounts[it->second];
      }
      continue;
    }
    auto tensor = idToTensor(index, id, localVocab);
    if (!tensor.has_value() || tensor->size() == 0) {
      idToVector[id] = invalid;
      ++result.numNonTensor_;
      continue;
    }
    const auto& values = tensor->tensorData();
    if (!dim.has_value()) {
      dim = values.size();
    }
    if (values.size() != dim.value()) {
      idToVector[id] = mismatch;
      ++result.numDimensionMismatch_;
      continue;
    }
    size_t vectorIdx = numVectors++;
    idToVector[id] = vectorIdx;
    vectorOfRow[row] = vectorIdx;
    rowCounts.push_back(1);
    result.data_.insert(result.data_.end(), values.begin(), values.end());
  }
  result.dim_ = dim.value_or(0);

  if (normalize && numVectors > 0) {
    // Leaves zero vectors unchanged.
    faiss::fvec_renorm_L2(result.dim_, numVectors, result.data_.data());
  }

  // Build the CSR layout of the rows per vector.
  result.rowOffsets_.assign(numVectors + 1, 0);
  for (size_t i = 0; i < numVectors; ++i) {
    result.rowOffsets_[i + 1] = result.rowOffsets_[i] + rowCounts[i];
  }
  result.rows_.resize(result.rowOffsets_.back());
  std::vector<size_t> fill(result.rowOffsets_.begin(),
                           result.rowOffsets_.end() - 1);
  for (size_t row = 0; row < vectorOfRow.size(); ++row) {
    if (vectorOfRow[row] < numVectors) {
      result.rows_[fill[vectorOfRow[row]]++] = row;
    }
  }
  return result;
}
}  // namespace tensorIndex

namespace {
// The global cache of built indices.
struct ValueSizeGetter {
  ad_utility::MemorySize operator()(const TensorIndexCachedIndex& idx) const {
    return ad_utility::MemorySize::bytes(idx.getSizeInBytes());
  }
};
using Cache =
    ad_utility::LRUCache<std::string, TensorIndexCachedIndex, ValueSizeGetter>;

// The maximal number of non-pinned (unnamed) indices that are kept.
constexpr size_t MAX_NUM_UNNAMED_CACHED_INDICES = 8;

// Function-local static to avoid the static initialization order fiasco.
ad_utility::Synchronized<Cache>& getCache() {
  static ad_utility::Synchronized<Cache> cache{MAX_NUM_UNNAMED_CACHED_INDICES};
  return cache;
}

// Defaults for the index parameters.
constexpr size_t DEFAULT_HNSW_M = 32;
constexpr size_t DEFAULT_HNSW_EF_SEARCH = 64;
constexpr size_t DEFAULT_IVF_NPROBE = 16;
}  // namespace

// ____________________________________________________________________________
TensorIndexCachedIndex::~TensorIndexCachedIndex() = default;
TensorIndexCachedIndex::TensorIndexCachedIndex(
    TensorIndexCachedIndex&&) noexcept = default;
TensorIndexCachedIndex& TensorIndexCachedIndex::operator=(
    TensorIndexCachedIndex&&) noexcept = default;

// ____________________________________________________________________________
TensorIndexCachedIndex::TensorIndexCachedIndex(
    ql::span<const Id> column, const Index& index, const LocalVocab& localVocab,
    TensorIndexConfiguration config, std::string fingerprint,
    const std::function<void()>& checkCancellation)
    : config_{std::move(config)},
      metric_{tensorIndex::getMetricInfo(config_.dist_)},
      numInputRows_{column.size()},
      fingerprint_{std::move(fingerprint)} {
  if (!metric_.supportedByFaiss_) {
    throw std::runtime_error(
        "The selected distance metric is not supported by the FAISS-based "
        "tensor index. Supported metrics are dot, cosine, angular, and "
        "euclidean.");
  }
  auto tensors =
      tensorIndex::extractTensors(column, index, localVocab, std::nullopt,
                                  metric_.normalize_, checkCancellation);
  if (tensors.numDimensionMismatch_ > 0) {
    AD_LOG_WARN << "Ignored " << tensors.numDimensionMismatch_
                << " tensors with a dimension different from "
                << tensors.dim_ << " while building the tensor index."
                << std::endl;
  }
  // Note: `n` must be computed before `tensors.rowOffsets_` is moved from.
  size_t n = tensors.numVectors();
  rowOffsets_ = std::move(tensors.rowOffsets_);
  rows_ = std::move(tensors.rows_);
  dimSize_ = tensors.dim_;
  if (n == 0) {
    AD_LOG_DEBUG << "No valid tensors found to build the tensor index."
                 << std::endl;
    return;
  }

  auto d = static_cast<faiss::idx_t>(dimSize_);
  auto faissMetric = metric_.higherIsBetter_ ? faiss::METRIC_INNER_PRODUCT
                                             : faiss::METRIC_L2;
  switch (config_.algo_) {
    case TensorIndexAlgorithm::FAISS_HNSW: {
      size_t m = std::max<size_t>(config_.nNeighbours_.value_or(DEFAULT_HNSW_M),
                                  2);
      index_ = std::make_unique<faiss::IndexHNSWFlat>(
          static_cast<int>(d), static_cast<int>(m), faissMetric);
      break;
    }
    case TensorIndexAlgorithm::FAISS_IVF: {
      // The number of lists must not exceed the number of training vectors.
      size_t defaultNlist = std::max<size_t>(
          1, static_cast<size_t>(std::sqrt(static_cast<double>(n))));
      nlist_ = std::clamp<size_t>(config_.kIVF_.value_or(defaultNlist), 1, n);
      std::unique_ptr<faiss::Index> quantizer;
      if (faissMetric == faiss::METRIC_INNER_PRODUCT) {
        quantizer = std::make_unique<faiss::IndexFlatIP>(d);
      } else {
        quantizer = std::make_unique<faiss::IndexFlatL2>(d);
      }
      auto ivf = std::make_unique<faiss::IndexIVFFlat>(quantizer.get(), d,
                                                       nlist_, faissMetric);
      // The IVF index takes ownership of the quantizer.
      ivf->own_fields = true;
      quantizer.release();
      index_ = std::move(ivf);
      break;
    }
    default:
      AD_THROW(
          "Unknown algorithm during FAISS index build, please report this to "
          "the developers!");
  }
  AD_LOG_INFO << "Building tensor index with " << n << " vectors of dimension "
              << dimSize_ << " for " << rows_.size() << " rows." << std::endl;
  if (checkCancellation) {
    checkCancellation();
  }
  index_->train(static_cast<faiss::idx_t>(n), tensors.data_.data());
  index_->add(static_cast<faiss::idx_t>(n), tensors.data_.data());
}

// ____________________________________________________________________________
size_t TensorIndexCachedIndex::getSizeInBytes() const {
  size_t size = sizeof(*this) + (rows_.size() + rowOffsets_.size()) *
                                    sizeof(size_t);
  if (index_) {
    // The flat storage of the vectors dominates for all supported index types.
    size += static_cast<size_t>(index_->ntotal) * dimSize_ * sizeof(float);
  }
  return size;
}

// ____________________________________________________________________________
std::shared_ptr<const TensorIndexCachedIndex>
TensorIndexCachedIndex::fromKeyOrBuild(
    const std::string& key, const std::string& fingerprint, bool pinned,
    ql::span<const Id> column, const Index& index,
    const LocalVocab& localVocab, TensorIndexConfiguration config,
    const std::function<void()>& checkCancellation) {
  auto isValid = [&](const std::shared_ptr<const TensorIndexCachedIndex>& idx) {
    return idx != nullptr && idx->getFingerprint() == fingerprint &&
           idx->numInputRows() == column.size();
  };
  if (key.empty()) {
    return std::make_shared<const TensorIndexCachedIndex>(
        column, index, localVocab, std::move(config), fingerprint,
        checkCancellation);
  }
  {
    auto lock = getCache().wlock();
    if (auto existing = (*lock)[key]; isValid(existing)) {
      return existing;
    }
  }
  // Build the index without holding the lock, such that other queries are not
  // blocked. If two queries concurrently build the same index, the first one
  // that finishes wins.
  auto newIndex = std::make_shared<TensorIndexCachedIndex>(
      column, index, localVocab, std::move(config), fingerprint,
      checkCancellation);
  auto lock = getCache().wlock();
  if (auto existing = (*lock)[key]; existing != nullptr) {
    if (isValid(existing)) {
      return existing;
    }
    AD_LOG_INFO << "Replacing the stale tensor index cached under the name \""
                << key << "\"" << std::endl;
    lock->erase(key);
  }
  if (pinned) {
    lock->insertPinned(key, newIndex);
  } else {
    lock->insert(key, newIndex);
  }
  return newIndex;
}

// ____________________________________________________________________________
std::shared_ptr<const TensorIndexCachedIndex> TensorIndexCachedIndex::fromKey(
    const std::string& key) {
  auto lock = getCache().wlock();
  auto result = (*lock)[key];
  if (result == nullptr) {
    throw std::runtime_error(
        absl::StrCat("Cannot find a tensor index with key \"", key, "\""));
  }
  return result;
}

// ____________________________________________________________________________
void TensorIndexCachedIndex::clearCache() { getCache().wlock()->clearAll(); }

// ____________________________________________________________________________
TensorIndexCachedIndex::BatchResult TensorIndexCachedIndex::search(
    const float* queries, size_t numQueries, size_t k) const {
  BatchResult result;
  k = std::min(k, numVectors());
  result.k_ = k;
  if (!index_ || k == 0 || numQueries == 0) {
    result.k_ = 0;
    return result;
  }
  result.rawScores_.resize(numQueries * k);
  result.labels_.resize(numQueries * k);

  // The query-time parameters are passed per search (and not set on the
  // shared index), which makes concurrent searches thread-safe.
  faiss::SearchParametersIVF ivfParams;
  faiss::SearchParametersHNSW hnswParams;
  const faiss::SearchParameters* params = nullptr;
  if (config_.algo_ == TensorIndexAlgorithm::FAISS_IVF) {
    ivfParams.nprobe = std::clamp<size_t>(
        config_.searchK_.value_or(DEFAULT_IVF_NPROBE), 1, nlist_);
    params = &ivfParams;
  } else if (config_.algo_ == TensorIndexAlgorithm::FAISS_HNSW) {
    hnswParams.efSearch = static_cast<int>(std::max(
        config_.searchK_.value_or(DEFAULT_HNSW_EF_SEARCH), k));
    params = &hnswParams;
  }
  index_->search(static_cast<faiss::idx_t>(numQueries), queries,
                 static_cast<faiss::idx_t>(k), result.rawScores_.data(),
                 result.labels_.data(), params);
  return result;
}

// ____________________________________________________________________________
std::vector<TensorIndexCachedIndex::FaissResult> TensorIndexCachedIndex::findNN(
    const ad_utility::TensorData& query, size_t k) const {
  std::vector<FaissResult> result;
  if (!index_) {
    return result;
  }
  if (query.size() != dimSize_) {
    throw std::runtime_error(absl::StrCat(
        "Query vector has a different size than the indexed vectors! "
        "Encountered ",
        query.size(), " and ", dimSize_));
  }
  std::vector<float> q = query.tensorData();
  if (metric_.normalize_) {
    faiss::fvec_renorm_L2(dimSize_, 1, q.data());
  }
  auto batch = search(q.data(), 1, k);
  for (size_t i = 0; i < batch.k_; ++i) {
    auto label = batch.labels_[i];
    if (label < 0) {
      // FAISS returns -1 for empty slots.
      continue;
    }
    auto idx = static_cast<size_t>(label);
    result.push_back(
        {idx,
         static_cast<float>(tensorIndex::rawScoreToDistance(
             config_.dist_, batch.rawScores_[i])),
         getRow(idx)});
  }
  return result;
}
