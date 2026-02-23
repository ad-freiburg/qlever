// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTROWPROCESSOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTROWPROCESSOR_H

#include <optional>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/QueryExportTypes.h"
#include "util/CancellationHandle.h"
#include "util/InputRangeUtils.h"

namespace qlever::constructExport {

// A stateful iterator over the CONSTRUCT results for a single `TableWithRange`
// given at construction. For each result row from the WHERE clause, every
// template triple in the CONSTRUCT clause is instantiated. The iterator yields
// one `EvaluatedTriple` at a time via `get()`.
//
// Internally, rows are processed in batches: `ConstructBatchEvaluator`
// evaluates all variables in the batch at once (with LRU caching across
// batches), and `ConstructTripleInstantiator` then instantiates each
// preprocessed template triple for each row.
class ConstructRowProcessor : public ad_utility::InputRangeFromGet<
                                  qlever::constructExport::EvaluatedTriple> {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  // Default batch size for processing rows.
  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  // Multiplier for computing ID cache capacity from batch size and variable
  // count. Provides headroom for cross-batch cache hits on repeated values.
  static constexpr size_t CACHE_CAPACITY_FACTOR = 32;

  ConstructRowProcessor(
      const PreprocessedConstructTemplate& preprocessedTemplate,
      const Index& index, CancellationHandle cancellationHandle,
      const TableWithRange& table, size_t currentRowOffset);

  // Returns the next instantiated triple, or nullopt when exhausted.
  // Incomplete triples (with UNDEF components) are filtered out.
  std::optional<EvaluatedTriple> get() override;

 private:
  // Evaluate all variables for the current batch and instantiate all template
  // triples for every row. Triples with any undefined term are omitted.
  std::vector<EvaluatedTriple> computeBatch(size_t batchStart);

  const PreprocessedConstructTemplate& preprocessedTemplate_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;

  // Table data.
  TableConstRefWithVocab tableWithVocab_;
  size_t numRows_;   // Total number of rows to process.
  size_t firstRow_;  // Index of the first row in the IdTable.
  size_t currentRowOffset_;

  // LRU cache for avoiding redundant vocabulary lookups across batches.
  IdCache idCache_;

  // Lazy range driving the batch iteration. Initialized in the constructor
  // body after all other members are ready.
  ad_utility::InputRangeTypeErased<EvaluatedTriple> innerRange_;
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTROWPROCESSOR_H
