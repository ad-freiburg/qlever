// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructIdCache.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/QueryExportTypes.h"
#include "util/stream_generator.h"

// _____________________________________________________________________________
// Batch processor that iterates through result table rows and yields
// instantiated triples. Handles batch loading, evaluation, and iteration.
// Yields InstantiatedTriple structs; callers transform to desired output
// format.
class ConstructBatchProcessor
    : public ad_utility::InputRangeFromGet<InstantiatedTriple> {
 public:
  using IdCache = ConstructIdCache;
  using IdCacheStatsLogger = ConstructIdCacheStatsLogger;

  // Default batch size for processing rows.
  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  static size_t getBatchSize() { return DEFAULT_BATCH_SIZE; }

  // Constructor takes all data needed for processing.
  ConstructBatchProcessor(
      std::shared_ptr<const PreprocessedConstructTemplate> blueprint,
      const TableWithRange& table, size_t currentRowOffset);

  // Returns the next instantiated triple, or nullopt when exhausted.
  // Incomplete triples (with UNDEF components) are filtered out.
  std::optional<InstantiatedTriple> get() override;

 private:
  // Creates an `Id` cache with a statistics logger that logs at INFO level
  // when destroyed (after query execution completes).
  std::pair<std::shared_ptr<IdCache>, std::shared_ptr<IdCacheStatsLogger>>
  createIdCacheWithStats(size_t numRows) const;

  // Load a new batch of rows for evaluation if we don't have one.
  void loadBatchIfNeeded();

  // Process rows in the current batch, returning the next complete triple.
  std::optional<InstantiatedTriple> processCurrentBatch();

  // Process triples for the current row, returning the next complete triple.
  std::optional<InstantiatedTriple> processCurrentRow();

  // Advance to next row in batch.
  void advanceToNextRow();

  // Advance to next batch.
  void advanceToNextBatch();

  std::shared_ptr<const PreprocessedConstructTemplate>
      preprocessedConstructTemplate_;

  // Table data (copied/referenced for iteration lifetime).
  TableConstRefWithVocab tableWithVocab_;
  std::vector<uint64_t> rowIndicesVec_;
  size_t currentRowOffset_;

  // `Id` cache for avoiding redundant vocabulary lookups.
  std::shared_ptr<IdCache> idCache_;
  std::shared_ptr<IdCacheStatsLogger> statsLogger_;

  // Iteration state.
  size_t batchSize_;
  size_t batchStart_ = 0;
  size_t rowInBatchIdx_ = 0;
  size_t tripleIdx_ = 0;
  std::optional<BatchEvaluationCache> batchCache_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H
