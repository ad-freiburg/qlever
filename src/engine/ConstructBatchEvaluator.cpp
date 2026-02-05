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
    IdCache& idCache, ConstructIdCacheStatsLogger& statsLogger) {
  BatchEvaluationResult batchResult;
  batchResult.numRows_ = rowIndices.size();

  instantiatesVariablesForBatch(batchResult, preprocessedConstructTemplate,
                                idTable, localVocab, rowIndices,
                                currentRowOffset, idCache, statsLogger);
  instantiateBlankNodesForBatch(batchResult, preprocessedConstructTemplate,
                                rowIndices, currentRowOffset);

  return batchResult;
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiatesVariablesForBatch(
    BatchEvaluationResult& batchResult,
    const PreprocessedConstructTemplate& preprocessedConstructTemplate,
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    IdCache& idCache, ConstructIdCacheStatsLogger& statsLogger) {
  const size_t numRows = rowIndices.size();
  const auto& variablesToInstantiate =
      preprocessedConstructTemplate.variablesToInstantiate_;

  // Initialize `instantiatedVariables_`: [varIdx][rowInBatch]
  // Default value is Undef{}, representing unbound variables.
  batchResult.instantiatedVariables_.resize(variablesToInstantiate.size());
  for (auto& column : batchResult.instantiatedVariables_) {
    column.resize(numRows, Undef{});
  }

  const VariableToColumnMap& varCols =
      preprocessedConstructTemplate.variableColumns_.get();
  const Index& idx = preprocessedConstructTemplate.index_.get();
  auto& cacheStats = statsLogger.stats();

  // Evaluate variables column-by-column for better cache locality.
  for (size_t varIdx = 0; varIdx < variablesToInstantiate.size(); ++varIdx) {
    const auto& varInfo = variablesToInstantiate[varIdx];

    if (!varInfo.columnIndex_.has_value()) {
      // variable not in result table: all values of that variable in the
      // result triple remain Undef.
      continue;
    }

    evaluateSingleVariableForBatch(batchResult.instantiatedVariables_[varIdx],
                                   varInfo.columnIndex_.value(), idTable,
                                   localVocab, rowIndices, currentRowOffset,
                                   varCols, idx, idCache, cacheStats);
  }
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::evaluateSingleVariableForBatch(
    std::vector<InstantiatedVariable>& columnResults, size_t colIdx,
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    const VariableToColumnMap& varCols, const Index& idx, IdCache& idCache,
    ConstructIdCacheStats& cacheStats) {
  // Read all IDs from this column for all rows in the batch,
  // look up their string values in the cache, and share them with the batch.
  for (size_t rowIdxInBatch = 0; rowIdxInBatch < rowIndices.size();
       ++rowIdxInBatch) {
    const uint64_t rowIdx = rowIndices[rowIdxInBatch];
    Id id = idTable(rowIdx, colIdx);

    // Use LRU cache's getOrCompute: returns cached value or computes and
    // caches it.
    size_t missesBefore = cacheStats.misses_;

    const std::shared_ptr<const std::string>& cachedValue =
        idCache.getOrCompute(id, [&cacheStats, &varCols, &idx, &colIdx, rowIdx,
                                  &idTable, &localVocab,
                                  currentRowOffset](const Id&) {
          ++cacheStats.misses_;

          ConstructQueryExportContext context{
              rowIdx, idTable, localVocab, varCols, idx, currentRowOffset};

          auto value = ConstructQueryEvaluator::evaluateVariableWithColumnIndex(
              colIdx, context);

          if (value.has_value()) {
            return std::make_shared<const std::string>(std::move(*value));
          }
          return std::shared_ptr<const std::string>{};
        });

    if (cacheStats.misses_ == missesBefore) {
      ++cacheStats.hits_;
    }

    if (cachedValue) {
      columnResults[rowIdxInBatch] = cachedValue;
    } else {
      columnResults[rowIdxInBatch] = Undef{};
    }
  }
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::instantiateBlankNodesForBatch(
    BatchEvaluationResult& batchResult,
    const PreprocessedConstructTemplate& blueprint,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset) {
  const size_t numRows = rowIndices.size();
  const auto& blankNodesToEvaluate = blueprint.blankNodesToEvaluate_;

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
