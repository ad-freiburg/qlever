// Copyright 2025 The QLever Authors, in particular:
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H

#include <util/LruCacheWithStatistics.h>

#include <vector>

#include "engine/ConstructTypes.h"
#include "engine/idTable/IdTable.h"

namespace qlever::constructExport {

using IdCache =
    ad_utility::util::LRUCacheWithStatistics<Id, std::optional<EvaluatedTerm>>;

// Identifies a contiguous sub-range of an `IdTable` that forms one batch.
struct BatchEvaluationContext {
  const IdTable& idTable_;
  size_t firstRow_;
  size_t endRow_;  // exclusive

  size_t numRows() const { return endRow_ - firstRow_; }
};

// Resolves `Id` values in variable columns to their string representations
// (IRI, literal, etc.) via `ConstructQueryEvaluator::evaluateId`.
//
// The evaluation is column-oriented: for each variable (identified by their
// `IdTable` column), all rows in the batch are evaluated before moving to the
// next column.
//
// An `IdCache` (LRU cache keyed by `Id`) avoids redundant evaluation of the
// same `Id` across rows and batches.
class ConstructBatchEvaluator {
 public:
  // Evaluate all `uniqueVariableColumns` for the rows in `evaluationContext`.
  // Results are indexed by column and then by row-within-batch (0-based
  // relative to `firstRow_`).
  static BatchEvaluationResult evaluateBatch(
      const std::vector<size_t>& uniqueVariableColumns,
      const BatchEvaluationContext& evaluationContext,
      const LocalVocab& localVocab, const Index& index, IdCache& idCache);

 private:
  // Evaluate a single variable across all rows in the batch. A variable is
  // identified by its column in the `IdTable` (which is contained in
  // `BatchEvaluationContext`). For each row, the `Id` at
  // `(rowIdx, idTableColumnIdx)` is looked up in `IdCache`; on a cache miss,
  // `ConstructQueryEvaluator::evaluateId` is called and the result is cached.
  static std::vector<std::optional<EvaluatedTerm>> evaluateVariableByColumn(
      size_t idTableColumnIdx, const BatchEvaluationContext& evaluationContext,
      const LocalVocab& localVocab, const Index& index, IdCache& idCache);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
