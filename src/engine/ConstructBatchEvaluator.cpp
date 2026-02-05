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
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    IdCache& idCache) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = rowIndices.size();

  instantiateVariablesForBatch(batchResult, preprocessedConstructTemplate,
                               idTable, localVocab, rowIndices,
                               currentRowOffset, idCache);
  instantiateBlankNodesForBatch(batchResult, preprocessedConstructTemplate,
                                rowIndices, currentRowOffset);

  return batchResult;
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiateVariablesForBatch(
    BatchEvaluationResult& batchResult,
    const PreprocessedConstructTemplate& preprocessedConstructTemplate,
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    IdCache& idCache) {
  const size_t numRows = rowIndices.size();
  const auto& variablesToInstantiate =
      preprocessedConstructTemplate.variablesToInstantiate_;

  // Initialize `variableInstantiations_`: [varIdx][rowInBatch]
  // Default value is Undef{}, representing unbound variables.
  batchResult.variableInstantiations_.resize(variablesToInstantiate.size());
  for (auto& column : batchResult.variableInstantiations_) {
    column.resize(numRows, Undef{});
  }

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
        varInfo.columnIndex_.value(), idTable, localVocab, rowIndices,
        currentRowOffset, varCols, idx, idCache);
  }
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiateSingleVariableForBatch(
    std::vector<InstantiatedTerm>& columnResults, size_t colIdx,
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    const VariableToColumnMap& varCols, const Index& idx, IdCache& idCache) {
  // Read all IDs from this column for all rows in the batch,
  // and look up their values in the cache.
  for (size_t rowIdxInBatch = 0; rowIdxInBatch < rowIndices.size();
       ++rowIdxInBatch) {
    const uint64_t rowIdx = rowIndices[rowIdxInBatch];
    Id id = idTable(rowIdx, colIdx);

    columnResults[rowIdxInBatch] = idCache.getOrCompute(id, [&](const Id&) {
      return computeVariableInstantiation(colIdx, rowIdx, idTable, localVocab,
                                          currentRowOffset, varCols, idx);
    });
  }
}

// _____________________________________________________________________________
InstantiatedTerm ConstructBatchEvaluator::computeVariableInstantiation(
    size_t colIdx, size_t rowIdx, const IdTable& idTable,
    const LocalVocab& localVocab, size_t currentRowOffset,
    const VariableToColumnMap& varCols, const Index& idx) {
  ConstructQueryExportContext context{rowIdx,  idTable, localVocab,
                                      varCols, idx,     currentRowOffset};

  auto value =
      ConstructQueryEvaluator::evaluateVariableWithColumnIndex(colIdx, context);

  if (value.has_value()) {
    return std::make_shared<const std::string>(std::move(*value));
  }
  return Undef{};
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiateBlankNodesForBatch(
    BatchEvaluationResult& batchResult,
    const PreprocessedConstructTemplate& blueprint,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset) {
  const size_t numRows = rowIndices.size();
  const auto& blankNodesToEvaluate = blueprint.blankNodesToInstantiate_;

  // Initialize blank node values: [blankNodeIdx][rowInBatch]
  batchResult.blankNodeValues_.resize(blankNodesToEvaluate.size());
  for (auto& column : batchResult.blankNodeValues_) {
    column.resize(numRows);
  }

  // Evaluate blank nodes using precomputed prefix and suffix.
  // Only the row number needs to be concatenated per row.
  // Format: prefix + (currentRowOffset + rowIdx) + suffix
  for (size_t blankIdx = 0; blankIdx < blankNodesToEvaluate.size();
       ++blankIdx) {
    const BlankNodeFormatInfo& formatInfo = blankNodesToEvaluate[blankIdx];
    auto& columnValues = batchResult.blankNodeValues_[blankIdx];

    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      // Use precomputed prefix and suffix, only concatenate row number.
      columnValues[rowInBatch] = absl::StrCat(
          formatInfo.prefix_, currentRowOffset + rowIdx, formatInfo.suffix_);
    }
  }
}
