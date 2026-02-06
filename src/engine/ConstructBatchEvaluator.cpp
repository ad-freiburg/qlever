// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructBatchEvaluator.h"

#include <absl/strings/str_cat.h>

#include "engine/ConstructQueryEvaluator.h"
#include "parser/data/ConstructQueryExportContext.h"

// _____________________________________________________________________________
BatchEvaluationResult ConstructBatchEvaluator::evaluateBatch(
    const PreprocessedConstructTemplate& preprocessedConstructTemplate,
    const BatchEvaluationContext& evaluationContext, IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = evaluationContext.rowIndicesOfBatch_.size();

  instantiateVariablesForBatch(batchResult, preprocessedConstructTemplate,
                               evaluationContext, idCache);
  instantiateBlankNodesForBatch(batchResult, preprocessedConstructTemplate,
                                evaluationContext);

  return batchResult;
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiateVariablesForBatch(
    BatchEvaluationResult& batchResult,
    const PreprocessedConstructTemplate& preprocessedConstructTemplate,
    const BatchEvaluationContext& evaluationContext, IdCache& idCache) {
  const size_t numRows = evaluationContext.rowIndicesOfBatch_.size();
  const auto& variablesToInstantiate =
      preprocessedConstructTemplate.variablesToInstantiate_;

  // Initialize `variableInstantiations_`: [varIdx][rowInBatch]
  // Default value is Undef{}, representing unbound variables.
  batchResult.variableInstantiations_.resize(variablesToInstantiate.size(),
                                             numRows, Undef{});

  const VariableToColumnMap& varCols =
      preprocessedConstructTemplate.variableColumns_.get();
  const Index& idx = preprocessedConstructTemplate.index_.get();

  // Evaluate variables column-by-column for better cache locality.
  for (size_t varIdx = 0; varIdx < variablesToInstantiate.size(); ++varIdx) {
    const auto& varInfo = variablesToInstantiate[varIdx];

    if (!varInfo.columnIndex_.has_value()) {
      // Variable not in result table: all values remain Undef.
      continue;
    }

    instantiateSingleVariableForBatch(
        batchResult.variableInstantiations_[varIdx],
        varInfo.columnIndex_.value(), evaluationContext, varCols, idx, idCache);
  }
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiateSingleVariableForBatch(
    std::vector<InstantiatedTerm>& columnResults, size_t colIdx,
    const BatchEvaluationContext& evaluationContext,
    const VariableToColumnMap& varCols, const Index& idx, IdCache& idCache) {
  // Read all IDs from this column for all rows in the batch,
  // and look up their values in the cache.
  for (size_t rowIdxInBatch = 0;
       rowIdxInBatch < evaluationContext.rowIndicesOfBatch_.size();
       ++rowIdxInBatch) {
    const uint64_t rowIdx = evaluationContext.rowIndicesOfBatch_[rowIdxInBatch];
    Id id = evaluationContext.idTable_(rowIdx, colIdx);

    auto computeValue = [&colIdx, &rowIdx, &evaluationContext, &varCols,
                         &idx](const Id&) {
      return computeVariableInstantiation(colIdx, rowIdx, evaluationContext,
                                          varCols, idx);
    };

    columnResults[rowIdxInBatch] = idCache.getOrCompute(id, computeValue);
  }
}

// _____________________________________________________________________________
InstantiatedTerm ConstructBatchEvaluator::computeVariableInstantiation(
    size_t colIdx, size_t rowIdx,
    const BatchEvaluationContext& evaluationContext,
    const VariableToColumnMap& varCols, const Index& idx) {
  ConstructQueryExportContext context{rowIdx,
                                      evaluationContext.idTable_,
                                      evaluationContext.localVocab_,
                                      varCols,
                                      idx,
                                      evaluationContext.currentRowOffset_};

  auto value =
      ConstructQueryEvaluator::evaluateVariableByColumnIndex(colIdx, context);

  if (value.has_value()) {
    return std::make_shared<const std::string>(std::move(*value));
  }
  return Undef{};
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiateBlankNodesForBatch(
    BatchEvaluationResult& batchResult,
    const PreprocessedConstructTemplate& preprocessedConstructTemplate,
    const BatchEvaluationContext& evaluationContext) {
  const size_t numRows = evaluationContext.rowIndicesOfBatch_.size();
  const auto& blankNodesToInstantiate =
      preprocessedConstructTemplate.blankNodesToInstantiate_;

  // Initialize blank node values: [blankNodeIdx][rowInBatch]
  batchResult.blankNodeInstantiations_.resize(blankNodesToInstantiate.size(),
                                              numRows);

  // Evaluate blank nodes using precomputed prefix and suffix.
  // Only the row number needs to be concatenated per row.
  // Format: prefix + (currentRowOffset + rowIdx) + suffix
  for (size_t blankIdx = 0; blankIdx < blankNodesToInstantiate.size();
       ++blankIdx) {
    const BlankNodeFormatInfo& formatInfo = blankNodesToInstantiate[blankIdx];
    auto& columnValues = batchResult.blankNodeInstantiations_[blankIdx];

    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = evaluationContext.rowIndicesOfBatch_[rowInBatch];
      // Use precomputed prefix and suffix, only concatenate row number.
      columnValues[rowInBatch] = absl::StrCat(
          formatInfo.prefix_, evaluationContext.currentRowOffset_ + rowIdx,
          formatInfo.suffix_);
    }
  }
}
