// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H

#include <vector>

#include "backports/span.h"
#include "engine/ConstructIdCache.h"
#include "engine/ConstructTypes.h"
#include "engine/idTable/IdTable.h"

// Forward declarations
class Index;
class LocalVocab;

// Groups the table data needed for batch evaluation:
// the IdTable, LocalVocab, row indices for the batch, and the row offset.
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
  using IdCache = ConstructIdCache;

  // Main entry point: evaluates all variables for a batch.
  // Uses the pre-collected unique column indices to evaluate each variable
  // column across all batch rows.
  static BatchEvaluationResult evaluateBatch(
      const std::vector<size_t>& uniqueVariableColumns,
      const BatchEvaluationContext& evaluationContext, const Index& index,
      IdCache& idCache);

 private:
  // Evaluates a single variable column across all batch rows.
  static void evaluateVariableByColumn(
      std::vector<EvaluatedTerm>& columnResults, size_t idTableColumnIdx,
      const BatchEvaluationContext& evaluationContext, const Index& index,
      IdCache& idCache);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
