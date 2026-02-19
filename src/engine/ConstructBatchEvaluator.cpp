// Copyright 2025 The QLever Authors, in particular:
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include "engine/ConstructQueryEvaluator.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    const std::vector<size_t>& uniqueVariableColumns,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  // Evaluate each unique variable across all batch rows.
  for (size_t variableColumnIdx : uniqueVariableColumns) {
    batchResult.variablesByColumn_[variableColumnIdx] =
        evaluateVariableByColumn(variableColumnIdx, evaluationContext,
                                 localVocab, index, idCache);
  }

  return batchResult;
}

// _____________________________________________________________________________
std::vector<std::optional<EvaluatedTerm>>
ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  std::vector<std::optional<EvaluatedTerm>> columnResults(
      evaluationContext.numRows());

  for (size_t rowInBatch = 0; rowInBatch < evaluationContext.numRows();
       ++rowInBatch) {
    size_t rowIdx = evaluationContext.firstRow_ + rowInBatch;

    const Id& id = evaluationContext.idTable_(rowIdx, idTableColumnIdx);

    auto computeValue =
        [&index, &localVocab](const Id& id) -> std::optional<EvaluatedTerm> {
      auto value = ConstructQueryEvaluator::evaluateId(id, index, localVocab);

      if (value.has_value()) {
        return std::make_shared<const std::string>(std::move(*value));
      }
      return std::nullopt;
    };

    columnResults[rowInBatch] = idCache.getOrCompute(id, computeValue);
  }

  return columnResults;
}

}  // namespace qlever::constructExport
