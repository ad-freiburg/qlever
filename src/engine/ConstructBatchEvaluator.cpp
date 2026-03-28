// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include "engine/ConstructQueryEvaluator.h"
#include "util/Exception.h"
#include "util/Views.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    ql::span<const size_t> variableColumnIndices,
    const BatchEvaluationContext& evaluationContext,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.numRows();

  for (size_t variableColumnIdx : variableColumnIndices) {
    auto [it, wasNew] = batchResult.variablesByColumn_.emplace(
        variableColumnIdx,
        evaluateVariableByColumn(variableColumnIdx, evaluationContext,
                                 localVocab, index, idCache));
    AD_CORRECTNESS_CHECK(wasNew);
  }

  return batchResult;
}

// _____________________________________________________________________________
std::optional<EvaluatedTerm> ConstructBatchEvaluator::idToEvaluatedTerm(
    const Index& index, Id id, const LocalVocab& localVocab) {
  auto optStr = ConstructQueryEvaluator::evaluateId(id, index, localVocab);
  if (!optStr.has_value()) return std::nullopt;
  return std::make_shared<const std::string>(std::move(optStr.value()));
}

// _____________________________________________________________________________
EvaluatedVariableValues ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  decltype(auto) col = ctx.idTable_.getColumn(idTableColumnIdx);

  const auto& evaluateRow = [&idCache, &col, &index,
                             &localVocab](size_t rowIdx) {
    return idCache.getOrCompute(col[rowIdx], [&index, &localVocab](Id id) {
      return idToEvaluatedTerm(index, id, localVocab);
    });
  };

  return ql::views::iota(ctx.firstRow_, ctx.endRow_) |
         ql::views::transform(evaluateRow) | ::ranges::to<std::vector>();
}

}  // namespace qlever::constructExport
