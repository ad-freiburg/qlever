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

#include "backports/span.h"
#include "engine/ConstructIdCache.h"
#include "engine/PreprocessedConstructTemplate.h"
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
  // Evaluates all `Variables` and `BlankNodes` for a batch of result-table
  // rows.
  BatchEvaluationCache evaluateBatchColumnOriented(
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset);

  // For each `Variable`, reads all `Id`s from its column across all batch
  // rows, converts them to strings (using `IdCache`), and stores pointers to
  // those strings in `BatchEvaluationCache`.
  void evaluateVariablesForBatch(BatchEvaluationCache& batchCache,
                                 const IdTable& idTable,
                                 const LocalVocab& localVocab,
                                 ql::span<const uint64_t> rowIndices,
                                 size_t currentRowOffset);

  // Evaluates all `BlankNodes` for a batch of rows. Uses precomputed
  // prefix/suffix, only concatenating the row number per row.
  void evaluateBlankNodesForBatch(BatchEvaluationCache& batchCache,
                                  ql::span<const uint64_t> rowIndices,
                                  size_t currentRowOffset) const;

  // Helper to get shared_ptr to the string resulting from evaluating the term
  // specified by `tripleIdx` (idx of the triple in the template triples) and
  // `pos` (position of the term in said template triple) on the row of the
  // result table specified by `rowIdxInBatch`.
  std::shared_ptr<const std::string> getTermStringPtr(
      size_t tripleIdx, size_t pos, const BatchEvaluationCache& batchCache,
      size_t rowIdxInBatch) const;

  // Creates an `Id` cache with a statistics logger that logs at INFO level
  // when destroyed (after query execution completes).
  std::pair<std::shared_ptr<IdCache>, std::shared_ptr<IdCacheStatsLogger>>
  createIdCacheWithStats(size_t numRows) const;

  // Formats a single triple according to the output format. Returns empty
  // string if any component is UNDEF.
  std::string formatTriple(
      const std::shared_ptr<const std::string>& subject,
      const std::shared_ptr<const std::string>& predicate,
      const std::shared_ptr<const std::string>& object) const;

  // Instantiates a single triple as StringTriple. Returns empty `StringTriple`
  // if any component is UNDEF.
  StringTriple instantiateTriple(
      const std::shared_ptr<const std::string>& subject,
      const std::shared_ptr<const std::string>& predicate,
      const std::shared_ptr<const std::string>& object) const;

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
