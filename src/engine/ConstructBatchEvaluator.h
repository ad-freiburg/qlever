// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H

#include <optional>
#include <vector>

#include "engine/ConstructTypes.h"
#include "engine/LocalVocab.h"
#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/LruCacheWithStatistics.h"

namespace qlever::constructExport {

// `EvaluatedVariableValues` is used to store the evaluation results
// (`EvaluatedTerm`s) for the values of a single variable across all rows in a
// batch. The i-th element corresponds to the i-th row in the batch, and is
// `nullopt` iff the variable was unbound for that row.
using EvaluatedVariableValues = std::vector<std::optional<EvaluatedTerm>>;

// Result of batch-evaluating all variables for a batch of rows. Stores the
// evaluated values per variable column and the number of rows in the batch.
struct BatchEvaluationResult {
  // `variablesByColumn_` maps a column index of the `Result` that is being
  // evaluated to the `EvaluatedVariableValues` for the variable that is stored
  // in that column. We use a hash map (instead of a dense vector) because the
  // set of evaluated columns may be sparse: some variables in the WHERE-clause
  // (in the `IdTable`) may not appear in the CONSTRUCT template and are thus
  // not evaluated.
  ad_utility::HashMap<size_t, EvaluatedVariableValues> variablesByColumn_;
  size_t numRows_ = 0;

  const std::optional<EvaluatedTerm>& getVariable(size_t columnIndex,
                                                  size_t rowInBatch) const {
    return variablesByColumn_.at(columnIndex).at(rowInBatch);
  }
};

using IdCache =
    ad_utility::util::LRUCacheWithStatistics<Id, std::optional<EvaluatedTerm>>;

// Identifies a contiguous sub-range of rows of an `IdTable` that forms one
// batch.
struct BatchEvaluationContext {
  const IdTable& idTable_;
  size_t firstRow_;
  size_t endRow_;  // exclusive

  BatchEvaluationContext(const IdTable& idTable, size_t firstRow, size_t endRow)
      : idTable_(idTable), firstRow_(firstRow), endRow_(endRow) {
    AD_CONTRACT_CHECK(firstRow <= endRow);
    AD_CONTRACT_CHECK(endRow <= idTable.numRows());
  }

  size_t numRows() const { return endRow_ - firstRow_; }
};

// Resolves `Id` values in variable columns to their string representations
// (IRI, literal, etc.) via `ConstructQueryEvaluator::evaluateId`.
//
// The evaluation is column-oriented: for each variable (identified by their
// `IdTable` column), all rows in the batch are evaluated before moving to the
// next variable.
//
// An `IdCache` (LRU cache keyed by `Id`) avoids redundant evaluation of the
// same `Id` across rows and batches.
class ConstructBatchEvaluator {
 public:
  // Evaluates the variables identified by `variableColumnIndices` for all rows
  // in `evaluationContext`. Each entry in `variableColumnIndices` is an
  // `IdTable` column index representing a variable in the CONSTRUCT template.
  static BatchEvaluationResult evaluateBatch(
      ql::span<const size_t> variableColumnIndices,
      const BatchEvaluationContext& evaluationContext,
      const LocalVocab& localVocab, const Index& index, IdCache& idCache);

 private:
  // Evaluate a single variable (identified by its `IdTable` column index)
  // across all rows in the batch.
  static EvaluatedVariableValues evaluateVariableByColumn(
      size_t idTableColumnIdx, const BatchEvaluationContext& ctx,
      const LocalVocab& localVocab, const Index& index, IdCache& idCache);

  // Convert a single `Id` to its `EvaluatedTerm` string representation.
  // Returns `std::nullopt` if the `Id` has no string representation.
  static std::optional<EvaluatedTerm> idToEvaluatedTerm(
      const Index& index, Id id, const LocalVocab& localVocab);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTBATCHEVALUATOR_H
