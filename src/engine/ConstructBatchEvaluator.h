// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H

#include <memory>
#include <vector>

#include "backports/span.h"
#include "engine/ConstructIdCache.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTypes.h"
#include "engine/idTable/IdTable.h"

// Forward declarations
class LocalVocab;

// _____________________________________________________________________________
// Evaluates Variables and BlankNodes for a batch of result-table rows.
// Uses column-oriented access pattern for variables for better cache locality.
class ConstructBatchEvaluator {
 public:
  using IdCache = ConstructIdCache;

  // Main entry point: evaluates all `Variable` objects and `BlankNode` objects
  // for a batch.
  // Column-oriented access pattern for variables:
  //   for each variable V occurring in the template triples:
  //     for each row R in batch:
  //       read idTable[column(V)][R]
  static BatchEvaluationResult evaluateBatch(
      const PreprocessedConstructTemplate& preprocessedConstructTemplate,
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      IdCache& idCache, ConstructIdCacheStatsLogger& statsLogger);

 private:
  // For each `Variable`, reads all `Id`s from its column across all batch
  // rows, converts them to strings (using `IdCache`), and stores pointers to
  // those strings in `BatchEvaluationResult`.
  static void evaluateVariablesForBatch(
      BatchEvaluationResult& batchResult,
      const PreprocessedConstructTemplate& preprocessedConstructTemplate,
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      IdCache& idCache, ConstructIdCacheStatsLogger& statsLogger);

  // Evaluates a single variable column across all batch rows.
  // Reads IDs from the column, looks up/computes string values via cache.
  static void evaluateSingleVariableForBatch(
      std::vector<std::shared_ptr<const std::string>>& columnStrings,
      size_t colIdx, const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      const VariableToColumnMap& varCols, const Index& idx, IdCache& idCache,
      ConstructIdCacheStats& cacheStats);

  // Evaluates all `BlankNode` objects for a batch of rows. Uses precomputed
  // prefix/suffix, only concatenating the row number per row.
  static void evaluateBlankNodesForBatch(
      BatchEvaluationResult& batchResult,
      const PreprocessedConstructTemplate& blueprint,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
