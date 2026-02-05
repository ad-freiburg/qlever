// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H

#include <memory>

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

  // Main entry point: evaluates all Variables and BlankNodes for a batch.
  // Column-oriented access pattern for variables:
  //   for each variable V occurring in the template triples:
  //     for each row R in batch:
  //       read idTable[column(V)][R]    <-- Sequential reads within a column
  //
  // This is more cache-friendly than row-oriented access, because the memory
  // layout of `IdTable` is column-major.
  static BatchEvaluationCache evaluateBatch(
      const PreprocessedConstructTemplate& blueprint, const IdTable& idTable,
      const LocalVocab& localVocab, ql::span<const uint64_t> rowIndices,
      size_t currentRowOffset, IdCache& idCache,
      ConstructIdCacheStatsLogger& statsLogger);

 private:
  // For each `Variable`, reads all `Id`s from its column across all batch
  // rows, converts them to strings (using `IdCache`), and stores pointers to
  // those strings in `BatchEvaluationCache`.
  static void evaluateVariablesForBatch(
      BatchEvaluationCache& batchCache,
      const PreprocessedConstructTemplate& blueprint, const IdTable& idTable,
      const LocalVocab& localVocab, ql::span<const uint64_t> rowIndices,
      size_t currentRowOffset, IdCache& idCache,
      ConstructIdCacheStatsLogger& statsLogger);

  // Evaluates all `BlankNodes` for a batch of rows. Uses precomputed
  // prefix/suffix, only concatenating the row number per row.
  static void evaluateBlankNodesForBatch(
      BatchEvaluationCache& batchCache,
      const PreprocessedConstructTemplate& blueprint,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
