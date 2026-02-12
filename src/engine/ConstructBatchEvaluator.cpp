// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructBatchEvaluator.h"

#include "engine/ConstructQueryEvaluator.h"

using namespace qlever::constructExport;

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    const std::vector<size_t>& uniqueVariableColumns,
    const BatchEvaluationContext& evaluationContext, const Index& index,
    IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.rowIndicesOfBatch_.size();

  // Evaluate each unique variable across all batch rows.
  for (size_t variableColumnIdx : uniqueVariableColumns) {
    auto& columnResults = batchResult.variablesByColumn_[variableColumnIdx];
    columnResults.resize(batchResult.numRows_, Undef{});
    evaluateVariableByColumn(columnResults, variableColumnIdx,
                             evaluationContext, index, idCache);
  }

  return batchResult;
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::evaluateVariableByColumn(
    std::vector<EvaluatedTerm>& columnResults, size_t idTableColumnIdx,
    const BatchEvaluationContext& evaluationContext, const Index& index,
    IdCache& idCache) {
  for (size_t rowInBatch = 0;
       rowInBatch < evaluationContext.rowIndicesOfBatch_.size(); ++rowInBatch) {
    const uint64_t rowIdx = evaluationContext.rowIndicesOfBatch_[rowInBatch];
    Id id = evaluationContext.idTable_(rowIdx, idTableColumnIdx);

    auto computeValue = [&index, &evaluationContext](const Id& id) {
      auto value = ConstructQueryEvaluator::evaluateId(
          id, index, evaluationContext.localVocab_);
      if (value.has_value()) {
        return EvaluatedTerm{
            std::make_shared<const std::string>(std::move(*value))};
      }
      return EvaluatedTerm{Undef{}};
    };

    columnResults[rowInBatch] = idCache.getOrCompute(id, computeValue);
  }
}
