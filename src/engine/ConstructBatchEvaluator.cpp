// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructBatchEvaluator.h"

#include "index/ExportIds.h"
#include "util/Views.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
std::optional<EvaluatedTerm>
ConstructBatchEvaluator::stringAndTypeToEvaluatedTerm(
    std::optional<std::pair<std::string, const char*>> optStringAndType) {
  if (!optStringAndType.has_value()) return std::nullopt;
  auto& [str, type] = optStringAndType.value();
  return std::make_shared<const EvaluatedTermData>(std::move(str), type);
}

// _____________________________________________________________________________
std::optional<EvaluatedTerm> ConstructBatchEvaluator::idToEvaluatedTerm(
    const Index& index, Id id, const LocalVocab& localVocab) {
  return stringAndTypeToEvaluatedTerm(
      ql::exportIds::idToStringAndType(index, id, localVocab));
}

// _____________________________________________________________________________
EvaluatedVariableValues ConstructBatchEvaluator::evaluateVariableByColumn(
    size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
    const LocalVocab& localVocab, const Index& index, IdCache& idCache) {
  decltype(auto) col = ctx.idTable_.getColumn(idTableColumnIdx);
  const size_t numRows = ctx.numRows();

  // Build a `(Id, rowInBatch)` index vector and sort by `Id`. This ensures
  // that `VocabIndex` IDs form a contiguous, sorted block (see
  // `idsToStringAndType`), converting vocabulary lookups from random-access
  // reads to sequential reads for I/O locality.
  std::vector<std::pair<Id, size_t>> sortedIndices;
  sortedIndices.reserve(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    sortedIndices.emplace_back(col[ctx.firstRow_ + i], i);
  }
  ql::ranges::sort(sortedIndices, [](const auto& a, const auto& b) {
    return a.first < b.first;
  });

  // Phase 1: check the cache for each sorted ID. Scatter hits directly to
  // `result`; collect misses for batch resolution.
  EvaluatedVariableValues result(numRows);
  std::vector<Id> missIds;
  std::vector<size_t> missRows;
  for (const auto& [id, rowInBatch] : sortedIndices) {
    auto cached = idCache.tryGet(id);
    if (cached) {
      result[rowInBatch] = cached.value();
    } else {
      missIds.push_back(id);
      missRows.push_back(rowInBatch);
    }
  }

  // Phase 2: batch-resolve cache misses. `missIds` is sorted (collected from
  // `sortedIndices`), satisfying the `idsToStringAndType` precondition for
  // sequential VocabIndex I/O. Each miss is inserted into the cache via
  // `getOrCompute`; duplicate IDs in `missIds` are handled correctly: the
  // second occurrence is already in cache and the lambda is not called.
  auto missResolved =
      ql::exportIds::idsToStringAndType(index, missIds, localVocab);
  for (size_t i = 0; i < missIds.size(); ++i) {
    result[missRows[i]] =
        idCache.getOrCompute(missIds[i], [&missResolved, i](const Id&) {
          return ConstructBatchEvaluator::stringAndTypeToEvaluatedTerm(
              std::move(missResolved[i]));
        });
  }
  return result;
}

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

}  // namespace qlever::constructExport
