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
BatchEvaluationCache ConstructBatchEvaluator::evaluateBatch(
    const PreprocessedConstructTemplate& blueprint, const IdTable& idTable,
    const LocalVocab& localVocab, ql::span<const uint64_t> rowIndices,
    size_t currentRowOffset, IdCache& idCache,
    ConstructIdCacheStatsLogger& statsLogger) {
  BatchEvaluationCache batchCache;
  batchCache.numRows_ = rowIndices.size();

  evaluateVariablesForBatch(batchCache, blueprint, idTable, localVocab,
                            rowIndices, currentRowOffset, idCache, statsLogger);
  evaluateBlankNodesForBatch(batchCache, blueprint, rowIndices,
                             currentRowOffset);

  return batchCache;
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::evaluateVariablesForBatch(
    BatchEvaluationCache& batchCache,
    const PreprocessedConstructTemplate& blueprint, const IdTable& idTable,
    const LocalVocab& localVocab, ql::span<const uint64_t> rowIndices,
    size_t currentRowOffset, IdCache& idCache,
    ConstructIdCacheStatsLogger& statsLogger) {
  const size_t numRows = rowIndices.size();
  auto& cacheStats = statsLogger.stats();
  const auto& variablesToEvaluate = blueprint.variablesToEvaluate_;

  // Initialize variable strings: [varIdx][rowInBatch]
  // shared_ptr defaults to nullptr, representing UNDEF values.
  batchCache.variableStrings_.resize(variablesToEvaluate.size());
  for (auto& column : batchCache.variableStrings_) {
    column.resize(numRows);
  }

  // Evaluate variables column-by-column for better cache locality.
  // The IdTable is accessed sequentially for each column.
  for (size_t varIdx = 0; varIdx < variablesToEvaluate.size(); ++varIdx) {
    const auto& varInfo = variablesToEvaluate[varIdx];
    auto& columnStrings = batchCache.variableStrings_[varIdx];

    if (!varInfo.columnIndex_.has_value()) {
      // Variable not in result - all values are nullptr (already default).
      continue;
    }

    const size_t colIdx = varInfo.columnIndex_.value();

    // Read all IDs from this column for all rows in the batch,
    // look up their string values in the cache, and share them with the batch.
    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      Id id = idTable(rowIdx, colIdx);

      // Use LRU cache's getOrCompute: returns cached value or computes and
      // caches it.
      size_t missesBefore = cacheStats.misses_;
      const VariableToColumnMap& varCols = blueprint.variableColumns_.get();
      const Index& idx = blueprint.index_.get();
      const std::shared_ptr<const std::string>& cachedValue =
          idCache.getOrCompute(id, [&cacheStats, &varCols, &idx, &colIdx,
                                    rowIdx, &idTable, &localVocab,
                                    currentRowOffset](const Id&) {
            ++cacheStats.misses_;
            ConstructQueryExportContext context{
                rowIdx, idTable, localVocab, varCols, idx, currentRowOffset};
            auto value = ConstructQueryEvaluator::evaluateWithColumnIndex(
                colIdx, context);
            if (value.has_value()) {
              return std::make_shared<const std::string>(std::move(*value));
            }
            return std::shared_ptr<const std::string>{};
          });

      if (cacheStats.misses_ == missesBefore) {
        ++cacheStats.hits_;
      }

      columnStrings[rowInBatch] = cachedValue;
    }
  }
}

// _____________________________________________________________________________
void ConstructBatchEvaluator::evaluateBlankNodesForBatch(
    BatchEvaluationCache& batchCache,
    const PreprocessedConstructTemplate& blueprint,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset) {
  const size_t numRows = rowIndices.size();
  const auto& blankNodesToEvaluate = blueprint.blankNodesToEvaluate_;

  // Initialize blank node values: [blankNodeIdx][rowInBatch]
  batchCache.blankNodeValues_.resize(blankNodesToEvaluate.size());
  for (auto& column : batchCache.blankNodeValues_) {
    column.resize(numRows);
  }

  // Evaluate blank nodes using precomputed prefix and suffix.
  // Only the row number needs to be concatenated per row.
  // Format: prefix + (currentRowOffset + rowIdx) + suffix
  for (size_t blankIdx = 0; blankIdx < blankNodesToEvaluate.size();
       ++blankIdx) {
    const BlankNodeFormatInfo& formatInfo = blankNodesToEvaluate[blankIdx];
    auto& columnValues = batchCache.blankNodeValues_[blankIdx];

    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      // Use precomputed prefix and suffix, only concatenate row number.
      columnValues[rowInBatch] = absl::StrCat(
          formatInfo.prefix_, currentRowOffset + rowIdx, formatInfo.suffix_);
    }
  }
}
