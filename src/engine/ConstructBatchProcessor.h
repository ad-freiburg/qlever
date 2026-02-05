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
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"

// _____________________________________________________________________________
// Self-contained batch processor for generating formatted triples.
// Implements the `InputRangeFromGet` interface for lazy evaluation.
class ConstructBatchProcessor
    : public ad_utility::InputRangeFromGet<std::string> {
 public:
  using IdCache = ConstructIdCache;
  using IdCacheStatsLogger = ConstructIdCacheStatsLogger;
  using StringTriple = QueryExecutionTree::StringTriple;

  // Default batch size for processing rows.
  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  static size_t getBatchSize() { return DEFAULT_BATCH_SIZE; }

  // Constructor takes all data needed for processing.
  ConstructBatchProcessor(
      std::shared_ptr<const PreprocessedConstructTemplate> blueprint,
      const TableWithRange& table, ad_utility::MediaType format,
      size_t currentRowOffset);

  // Returns the next formatted triple, or nullopt when exhausted.
  std::optional<std::string> get() override;

  // Returns the next triple as StringTriple, or nullopt when exhausted.
  // Use either get() OR getStringTriple(), not both on the same instance.
  std::optional<StringTriple> getStringTriple();

 private:
  // Creates an `Id` cache with a statistics logger that logs at INFO level
  // when destroyed (after query execution completes).
  std::pair<std::shared_ptr<IdCache>, std::shared_ptr<IdCacheStatsLogger>>
  createIdCacheWithStats(size_t numRows) const;

  // Load a new batch of rows for evaluation if we don't have one.
  void loadBatchIfNeeded();

  // Process rows in the current batch, returning the next formatted triple.
  std::optional<std::string> processCurrentBatch();

  // Process triples for the current row, returning the next formatted triple.
  std::optional<std::string> processCurrentRow();

  // Process rows in the current batch, returning the next `StringTriple`.
  std::optional<StringTriple> processCurrentBatchAsStringTriple();

  // Process triples for the current row, returning the next `StringTriple`.
  std::optional<StringTriple> processCurrentRowAsStringTriple();

  // Advance to next row in batch.
  void advanceToNextRow();

  // Advance to next batch.
  void advanceToNextBatch();

  std::shared_ptr<const PreprocessedConstructTemplate>
      preprocessedConstructTemplate;

  // Output format (Turtle/CSV/TSV).
  ad_utility::MediaType format_;

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

// _____________________________________________________________________________
// Adapter to use ConstructBatchProcessor::getStringTriple() with
// InputRangeTypeErased<StringTriple>. This is a thin wrapper that delegates
// to the batch processor's getStringTriple() method.
class StringTripleBatchProcessor : public ad_utility::InputRangeFromGet<
                                       ConstructBatchProcessor::StringTriple> {
 public:
  explicit StringTripleBatchProcessor(
      std::unique_ptr<ConstructBatchProcessor> processor)
      : processor_(std::move(processor)) {}

  std::optional<ConstructBatchProcessor::StringTriple> get() override {
    return processor_->getStringTriple();
  }

 private:
  std::unique_ptr<ConstructBatchProcessor> processor_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H
