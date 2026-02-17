// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H

#include <util/LruCacheWithStatistics.h>

#include <vector>

#include "backports/span.h"
#include "engine/ConstructTypes.h"
#include "engine/idTable/IdTable.h"

namespace qlever::constructExport {

// Groups the table data needed for batch evaluation.
struct BatchEvaluationContext {
  const IdTable& idTable_;
  const LocalVocab& localVocab_;
  ql::span<const uint64_t> rowIndicesOfBatch_;
  size_t currentRowOffset_;
};

// Evaluates variables for a batch of result-table rows.
// Uses column-oriented access pattern for better cache locality.
class ConstructBatchEvaluator {
 public:
  using IdCache =
      ad_utility::util::LRUCacheWithStatistics<Id,
                                               std::optional<EvaluatedTerm>>;
  using BatchEvaluationResult = qlever::constructExport::BatchEvaluationResult;
  using EvaluatedTerm = qlever::constructExport::EvaluatedTerm;

  // Main entry point: evaluates all variables for a batch.
  // Uses the pre-collected unique column indices to createAndEvaluateBatch each
  // variable column across all batch rows.
  static BatchEvaluationResult evaluateBatch(
      const std::vector<size_t>& uniqueVariableColumns,
      const BatchEvaluationContext& evaluationContext, const Index& index,
      IdCache& idCache);

 private:
  // Evaluates a single variable column across all batch rows.
  static void evaluateVariableByColumn(
      std::vector<std::optional<EvaluatedTerm>>& columnResults,
      size_t idTableColumnIdx, const BatchEvaluationContext& evaluationContext,
      const Index& index, IdCache& idCache);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
