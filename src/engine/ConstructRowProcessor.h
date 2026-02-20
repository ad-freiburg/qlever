// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/QueryExportTypes.h"
#include "util/CancellationHandle.h"
#include "util/stream_generator.h"

namespace qlever::constructExport {

// A stateful iterator that yields one `EvaluatedTriple` at a time via `get()`,
// consuming one result-table row range (`TableWithRange`) given at
// construction.
//
// Internally, rows are processed in batches: `ConstructBatchEvaluator`
// evaluates all variables in the batch at once (with LRU caching across
// batches), and `ConstructTripleInstantiator` then instantiates each
// preprocessed template triple for each row. Triples where any term is
// undefined (unbound variable) are silently skipped.
//
// The iteration state is two-dimensional: `batchStart_` tracks which batch is
// current, and `tripleIdx_` indexes into `currentBatchTriples_`, the
// materialised result of the current batch.
class ConstructRowProcessor : public ad_utility::InputRangeFromGet<
                                  qlever::constructExport::EvaluatedTriple> {
 public:
  using IdCache =
      ad_utility::util::LRUCacheWithStatistics<Id,
                                               std::optional<EvaluatedTerm>>;
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using InstantiatedTriple = qlever::constructExport::EvaluatedTriple;
  using PreprocessedConstructTemplate =
      qlever::constructExport::PreprocessedConstructTemplate;
  using BatchEvaluationResult = qlever::constructExport::BatchEvaluationResult;

  // Default batch size for processing rows.
  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  // Multiplier for computing ID cache capacity from batch size and variable
  // count. Provides headroom for cross-batch cache hits on repeated values.
  static constexpr size_t CACHE_CAPACITY_FACTOR = 32;

  static size_t getBatchSize() { return DEFAULT_BATCH_SIZE; }

  ConstructRowProcessor(
      const PreprocessedConstructTemplate& preprocessedTemplate,
      const Index& index, CancellationHandle cancellationHandle,
      const TableWithRange& table, size_t currentRowOffset);

  // Returns the next instantiated triple, or nullopt when exhausted.
  // Incomplete triples (with UNDEF components) are filtered out.
  std::optional<InstantiatedTriple> get() override;

 private:
  std::shared_ptr<IdCache> createIdCache() const;

  // Evaluate all variables for the current batch and instantiate all template
  // triples for every row. Triples with any undefined term are omitted.
  std::vector<EvaluatedTriple> computeBatch();

  const PreprocessedConstructTemplate& preprocessedTemplate_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;

  // Table data.
  TableConstRefWithVocab tableWithVocab_;
  std::vector<uint64_t> rowIndicesVec_;
  size_t currentRowOffset_;

  // Id cache for avoiding redundant vocabulary lookups.
  std::shared_ptr<IdCache> idCache_ = ConstructRowProcessor::createIdCache();

  // Iteration state.
  size_t batchSize_ = ConstructRowProcessor::getBatchSize();
  size_t batchStart_ = 0;
  size_t tripleIdx_ = 0;
  std::vector<EvaluatedTriple> currentBatchTriples_;
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H
