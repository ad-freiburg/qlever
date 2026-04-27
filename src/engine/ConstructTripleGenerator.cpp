// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "util/Views.h"

namespace qlever::constructExport {

using ad_utility::InputRangeTypeErased;
using StringTriple = QueryExecutionTree::StringTriple;

//______________________________________________________________________________
IdCache ConstructTripleGenerator::makeIdCache(
    const PreprocessedConstructTemplate& tmpl) {
  return IdCache{std::max(tmpl.uniqueVariableColumns_.size(), size_t{1}) *
                 CACHE_ENTRIES_PER_VARIABLE};
}

// Evaluate a single batch of rows from `table`, starting at `batchIdx *
// BATCH_SIZE`. Cancellation is checked once at the start of the batch.
static std::vector<EvaluatedTriple> computeBatch(
    const TableWithRange& table,
    const PreprocessedConstructTemplate& preprocessedTemplate,
    const Index& index, IdCache& cache, size_t numRowsOfTable,
    size_t tableRowOffset, CancellationHandle cancellationHandle,
    size_t batchIdx) {
  cancellationHandle->throwIfCancelled();
  const size_t batchStart = batchIdx * ConstructTripleGenerator::BATCH_SIZE;
  const size_t firstRowOfTable =
      numRowsOfTable > 0 ? *ql::ranges::begin(table.view_) : 0;
  const size_t batchEnd = std::min(
      batchStart + ConstructTripleGenerator::BATCH_SIZE, numRowsOfTable);
  BatchEvaluationContext ctx{table.tableWithVocab_.idTable(),
                             firstRowOfTable + batchStart,
                             firstRowOfTable + batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate.uniqueVariableColumns_, ctx,
      table.tableWithVocab_.localVocab(), index, cache);

  const size_t blankNodeBaseId = tableRowOffset + firstRowOfTable + batchStart;
  return instantiateBatch(preprocessedTemplate, batchResult, blankNodeBaseId);
}

//______________________________________________________________________________
InputRangeTypeErased<EvaluatedTriple> ConstructTripleGenerator::evaluateTables(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    const Index& index, CancellationHandle cancellationHandle,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset) {
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns);
  IdCache cache = makeIdCache(preprocessedTemplate);

  auto processTable = [preprocessedTemplate = std::move(preprocessedTemplate),
                       &index, cancellationHandle, cache = std::move(cache),
                       accumulatedRowOffset =
                           rowOffset](const TableWithRange& table) mutable {
    const size_t numRowsOfTable = ql::ranges::size(table.view_);
    // Snapshot the offset for this table, then advance it for the next table.
    const size_t tableRowOffset = accumulatedRowOffset;
    accumulatedRowOffset += numRowsOfTable;
    // Ceiling division: ensure the last partial batch is not dropped.
    const size_t numBatches = (numRowsOfTable + BATCH_SIZE - 1) / BATCH_SIZE;
    return ranges::views::chunk(table.view_, BATCH_SIZE) |
           ql::views::transform(
               [&, numRowsOfTable, tableRowOffset](size_t batchIdx) {
                 return computeBatch(table, preprocessedTemplate, index, cache,
                                     numRowsOfTable, tableRowOffset,
                                     cancellationHandle, batchIdx);
               }) |
           ql::views::join;
  };

  auto pipeline = allView(std::move(rowIndices)) |
                  ql::views::transform(std::move(processTable)) |
                  ql::views::join;
  return InputRangeTypeErased(std::move(pipeline));
}

//______________________________________________________________________________
InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    const Triples& templateTriples, const VariableToColumnMap& variableColums,
    const Index& index, CancellationHandle cancellationhandle,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    ad_utility::MediaType mediaType) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColums, index, cancellationhandle,
                     std::move(rowIndices), rowOffset);

  auto transformer = [mediaType](const EvaluatedTriple& triple) {
    return formatTriple(triple, mediaType);
  };
  return InputRangeTypeErased(allView(std::move(evaluatedTriples)) |
                              ql::views::transform(transformer));
}

//______________________________________________________________________________
InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriples(
    const Triples& templateTriples, const VariableToColumnMap& variableColums,
    const Index& index, CancellationHandle cancellationhandle,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColums, index, cancellationhandle,
                     std::move(rowIndices), rowOffset);

  auto transformer = [](const EvaluatedTriple& triple) {
    return createStringTriple(triple);
  };
  return InputRangeTypeErased(allView(std::move(evaluatedTriples)) |
                              ql::views::transform(transformer));
}

}  // namespace qlever::constructExport
