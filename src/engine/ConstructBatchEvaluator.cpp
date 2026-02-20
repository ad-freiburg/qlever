// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include "engine/ConstructQueryEvaluator.h"
#include "util/Views.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    const std::vector<size_t>& variableColumnIndices,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  for (size_t variableColumnIdx : variableColumnIndices) {
    batchResult.variablesByColumn_[variableColumnIdx] =
        evaluateVariableByColumn(variableColumnIdx, evaluationContext,
                                 localVocab, index, idCache);
  }

  return batchResult;
}

// _____________________________________________________________________________
EvaluatedVariableValues ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  EvaluatedVariableValues columnResults;
  columnResults.reserve(ctx.numRows());

  auto computeValue = [&index,
                       &localVocab](Id id) -> std::optional<EvaluatedTerm> {
    auto value = ConstructQueryEvaluator::evaluateId(id, index, localVocab);
    if (value.has_value()) {
      return std::make_shared<const std::string>(std::move(*value));
    }
    return std::nullopt;
  };

  for (size_t rowIdx : ql::views::iota(ctx.firstRow_, ctx.endRow_)) {
    const Id& id = ctx.idTable_(rowIdx, idTableColumnIdx);
    columnResults.push_back(idCache.getOrCompute(id, computeValue));
  }

  return columnResults;
}

}  // namespace qlever::constructExport
