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
#include "engine/ConstructTripleInstantiator.h"
#include "engine/QueryExportTypes.h"
#include "util/CancellationHandle.h"
#include "util/stream_generator.h"

namespace qlever::constructExport {

// Processes the rows of the result table and yields `EvaluatedTriple`
// objects. This is done in batches of rows of the result table.
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
  void loadBatchIfNeeded();
  std::optional<InstantiatedTriple> processCurrentBatch();
  std::optional<InstantiatedTriple> processCurrentRow();
  void advanceToNextRow();
  void advanceToNextBatch();

  // Compute the blank node row id for the current row.
  size_t currentBlankNodeRowId() const;

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
  size_t rowInBatchIdx_ = 0;
  size_t tripleIdx_ = 0;
  std::optional<BatchEvaluationResult> batchEvaluationResult_;
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHPROCESSOR_H
