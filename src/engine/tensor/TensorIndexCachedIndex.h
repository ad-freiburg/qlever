// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_ENGINE_TENSORINDEXCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_TENSORINDEXCACHEDINDEX_H

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include "backports/span.h"
#include <string>
#include <vector>

#include "engine/tensor/TensorIndexConfig.h"
#include "global/Id.h"
#include "rdfTypes/TensorData.h"
#include "util/Exception.h"

// Forward declarations to keep this header light.
namespace faiss {
struct Index;
}
class Index;
class LocalVocab;

namespace tensorIndex {

// Retrieve the tensor stored in the given `id`, or `std::nullopt` if the `id`
// does not hold a tensor literal. For `VocabIndex` ids the precomputed tensor
// of the index's `TensorDataVocabulary` is used if available (fast path, no
// string parsing). All other ids that refer to a literal (local vocab, the
// secondary vocab, or a vocab without a tensor sub-vocabulary) are resolved to
// their literal which is then parsed.
std::optional<ad_utility::TensorData> idToTensor(const Index& index, Id id,
                                                 const LocalVocab& localVocab);

// The semantics of a distance metric. Every metric is evaluated via a "raw"
// score (which is what is ranked, and what FAISS computes), which is then
// converted into the user-visible distance value that is bound to the distance
// variable. Both the naive and the FAISS algorithms use exactly this
// definition, so their results agree (up to the approximation of FAISS).
//
//   metric     | vectors     | raw score         | ranking | bound value
//   -----------+-------------+-------------------+---------+-----------------
//   DOT        | as is       | <a, b>            | desc    | <a, b>
//   COSINE     | normalized  | <a', b'>          | desc    | cos(a, b)
//   ANGULAR    | normalized  | <a', b'>          | desc    | sqrt(2 - 2cos)
//   EUCLIDEAN  | as is       | |a - b|^2         | asc     | |a - b|
//   MANHATTAN  | as is       | |a - b|_1         | asc     | |a - b|_1
//   HAMMING    | as is       | #{i : a_i != b_i} | asc     | #{i: a_i != b_i}
//
// Zero vectors are left unchanged by the normalization, so their cosine
// similarity with any vector is 0 (consistent with
// `TensorData::cosineSimilarity`).
struct MetricInfo {
  bool higherIsBetter_;
  bool normalize_;
  // Whether FAISS supports this metric (otherwise the naive algorithm has to
  // be used).
  bool supportedByFaiss_;
};
MetricInfo getMetricInfo(TensorDistanceAlgorithm dist);

// Compute the raw score (see above) between two vectors of dimension `dim`.
float rawScore(TensorDistanceAlgorithm dist, const float* a, const float* b,
               size_t dim);

// Convert a raw score (see above) into the distance value that is bound to the
// distance variable.
double rawScoreToDistance(TensorDistanceAlgorithm dist, float raw);

// Return true iff the raw score `a` is better than the raw score `b`.
inline bool isBetter(const MetricInfo& info, float a, float b) {
  return info.higherIsBetter_ ? a > b : a < b;
}

// The tensors of one column of an `IdTable`, stored as one contiguous buffer of
// floats. Identical tensors (same `Id`) are only stored once. For each stored
// (unique) vector the rows of the `IdTable` that contain this vector are
// stored in CSR format: the rows of vector `i` are
// `rows_[rowOffsets_[i] ... rowOffsets_[i + 1]]`.
struct ExtractedTensors {
  size_t dim_ = 0;
  std::vector<float> data_;
  std::vector<size_t> rowOffsets_{0};
  std::vector<size_t> rows_;
  // Statistics about rows that were skipped.
  size_t numUndefined_ = 0;
  size_t numNonTensor_ = 0;
  size_t numDimensionMismatch_ = 0;

  size_t numVectors() const { return rowOffsets_.size() - 1; }
  size_t numRows() const { return rows_.size(); }
  bool empty() const { return numVectors() == 0; }
  const float* vector(size_t i) const { return data_.data() + i * dim_; }
  ql::span<const size_t> rowsOfVector(size_t i) const {
    return {rows_.data() + rowOffsets_[i],
            rows_.data() + rowOffsets_[i + 1]};
  }
};

// Extract the tensors from the `column`. If `requiredDimension` is set, only
// tensors of that (flattened) dimension are extracted, otherwise the dimension
// of the first tensor is used. If `normalize` is set, each vector is
// L2-normalized. `checkCancellation` is called regularly.
ExtractedTensors extractTensors(ql::span<const Id> column, const Index& index,
                                const LocalVocab& localVocab,
                                std::optional<size_t> requiredDimension,
                                bool normalize,
                                const std::function<void()>& checkCancellation);
}  // namespace tensorIndex

// This class holds a `FAISS` index that is built on the tensors of one column
// of an `IdTable` (the right side of a `TensorIndex` operation). Instances are
// immutable after construction, so they can be safely shared between
// concurrent queries (FAISS searches on a const index are thread-safe, all
// query-time parameters are passed per search).
//
// Built indices are kept in a global, thread-safe cache:
// - If the user specified a name (`experimentalRightCacheName`), the index is
//   stored under this name and never evicted.
// - Otherwise the index is stored under a key derived from the cache key of
//   the right child and the index parameters in a small LRU cache.
// In both cases, a `fingerprint` of the input (the cache key of the right
// child, the join column, the index parameters and the number of rows) is
// stored and validated on every lookup, such that a stale index is never used
// for a different input.
class TensorIndexCachedIndex {
 public:
  struct FaissResult {
    // The id of the vector inside the index.
    size_t idx;
    // The distance value (already converted, see `rawScoreToDistance`).
    float dist;
    // The (first) row of the indexed table that holds the vector.
    size_t row;
  };

  // The results of a batched search: for query `q` the results are at
  // positions `[q * k, (q + 1) * k)`, unused slots have a label of -1.
  struct BatchResult {
    size_t k_ = 0;
    std::vector<float> rawScores_;
    std::vector<int64_t> labels_;
  };

 private:
  std::unique_ptr<faiss::Index> index_;
  TensorIndexConfiguration config_;
  tensorIndex::MetricInfo metric_;
  // The vectors are stored deduplicated, these are the rows for each vector.
  std::vector<size_t> rowOffsets_{0};
  std::vector<size_t> rows_;
  size_t dimSize_ = 0;
  size_t numInputRows_ = 0;
  std::string fingerprint_;
  // Parameters that are applied at search time.
  size_t nlist_ = 0;

 public:
  // Constructor that builds an index from the tensors in the given column.
  // The `fingerprint` identifies the input (see above).
  TensorIndexCachedIndex(ql::span<const Id> column, const Index& index,
                         const LocalVocab& localVocab,
                         TensorIndexConfiguration config,
                         std::string fingerprint = "",
                         const std::function<void()>& checkCancellation = {});
  ~TensorIndexCachedIndex();
  TensorIndexCachedIndex(TensorIndexCachedIndex&&) noexcept;
  TensorIndexCachedIndex& operator=(TensorIndexCachedIndex&&) noexcept;

  // Return the index for `key` from the global cache if it exists and matches
  // the `fingerprint`, otherwise build it from the `column` and store it. If
  // `pinned` is true, the index is never evicted from the cache. If `key` is
  // empty, the index is not cached at all.
  static std::shared_ptr<const TensorIndexCachedIndex> fromKeyOrBuild(
      const std::string& key, const std::string& fingerprint, bool pinned,
      ql::span<const Id> column, const Index& index,
      const LocalVocab& localVocab, TensorIndexConfiguration config,
      const std::function<void()>& checkCancellation = {});

  // Return the index stored under `key`. Throw if there is no such index.
  static std::shared_ptr<const TensorIndexCachedIndex> fromKey(
      const std::string& key);

  // Remove all entries from the global cache (mostly for testing).
  static void clearCache();

  // Find the `k` nearest neighbors of a single query vector. Throw if the
  // dimension of the query doesn't match the dimension of the index.
  std::vector<FaissResult> findNN(const ad_utility::TensorData& query,
                                  size_t k) const;

  // Batched search for `numQueries` query vectors that are stored
  // contiguously in `queries` (each of dimension `dimension()`, already
  // normalized if the metric requires it). `k` is the number of requested
  // nearest vectors per query (clamped to the number of indexed vectors).
  BatchResult search(const float* queries, size_t numQueries, size_t k) const;

  // Return the FAISS index, or `nullptr` if no tensors were indexed.
  const faiss::Index* getIndex() const { return index_.get(); }

  // The number of (unique) indexed vectors.
  size_t numVectors() const { return rowOffsets_.size() - 1; }

  // The dimension of the indexed vectors (0 if empty).
  size_t dimension() const { return dimSize_; }

  // From a vector id (as returned by the search), obtain the rows of the
  // `IdTable` from which this index was created.
  ql::span<const size_t> getRows(size_t faissIdx) const {
    AD_CORRECTNESS_CHECK(faissIdx + 1 < rowOffsets_.size());
    return {rows_.data() + rowOffsets_[faissIdx],
            rows_.data() + rowOffsets_[faissIdx + 1]};
  }
  // The first row that holds the vector with the given id.
  size_t getRow(size_t faissIdx) const { return getRows(faissIdx).front(); }

  const TensorIndexConfiguration& getConfig() const { return config_; }
  const std::string& getFingerprint() const { return fingerprint_; }
  size_t numInputRows() const { return numInputRows_; }
  // The number of input rows that hold an indexed vector.
  size_t numIndexedRows() const { return rows_.size(); }

  // Approximate memory consumption in bytes.
  size_t getSizeInBytes() const;
};

#endif  // QLEVER_SRC_ENGINE_TENSORINDEXCACHEDINDEX_H
