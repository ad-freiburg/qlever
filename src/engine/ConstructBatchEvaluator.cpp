// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructBatchEvaluator.h"

#include "engine/ConstructQueryEvaluator.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    const std::vector<size_t>& uniqueVariableColumns,
    const BatchEvaluationContext& evaluationContext, const Index& index,
    IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  // Evaluate each unique variable across all batch rows.
  for (size_t variableColumnIdx : uniqueVariableColumns) {
    auto& columnResults = batchResult.variablesByColumn_[variableColumnIdx];
    columnResults.resize(batchResult.numRows_);

    evaluateVariableByColumn(columnResults, variableColumnIdx,
                             evaluationContext, index, idCache);
  }

  return batchResult;
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::evaluateVariableByColumn(
    std::vector<std::optional<EvaluatedTerm>>& columnResults,
    size_t idTableColumnIdx, const BatchEvaluationContext& evaluationContext,
    const Index& index, IdCache& idCache) {
  for (size_t rowInBatch = 0; rowInBatch < evaluationContext.numRows();
       ++rowInBatch) {
    size_t rowIdx = evaluationContext.firstRow_ + rowInBatch;

    const Id& id = evaluationContext.idTable_(rowIdx, idTableColumnIdx);

    auto computeValue = [&index, &evaluationContext](
                            const Id& id) -> std::optional<EvaluatedTerm> {
      auto value = ConstructQueryEvaluator::evaluateId(
          id, index, evaluationContext.localVocab_);

      if (value.has_value()) {
        return std::make_shared<const std::string>(std::move(*value));
      }
      return std::nullopt;
    };

    columnResults[rowInBatch] = idCache.getOrCompute(id, computeValue);
  }
}

}  // namespace qlever::constructExport
