// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleGenerator.h"

#include "backports/concepts.h"
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

// Evaluate the rows covered by `batch.view_`. Cancellation is checked once at
// the start.
CPP_template(typename ChunkView)(requires ranges::range<ChunkView>) static std::
    vector<EvaluatedTriple> computeBatch(
        const TableConstRefWithVocab& tableWithVocab, ChunkView batch,
        const PreprocessedConstructTemplate& preprocessedTemplate,
        const Index& index, IdCache& cache, size_t tableRowOffset,
        CancellationHandle cancellationHandle) {
  cancellationHandle->throwIfCancelled();
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(batch));

  const size_t batchBegin = *ql::ranges::begin(batch);
  const size_t batchEnd =
      batchBegin + static_cast<size_t>(ql::ranges::size(batch));

  BatchEvaluationContext ctx{tableWithVocab.idTable(), batchBegin, batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate.uniqueVariableColumns_, ctx,
      tableWithVocab.localVocab(), index, cache);

  const size_t blankNodeBaseId = tableRowOffset + batchBegin;
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

    return ranges::views::chunk(table.view_, BATCH_SIZE) |
           ql::views::transform([&table, &preprocessedTemplate, &index, &cache,
                                 cancellationHandle,
                                 tableRowOffset](auto chunkView) {
             return computeBatch(table.tableWithVocab_, chunkView,
                                 preprocessedTemplate, index, cache,
                                 tableRowOffset, cancellationHandle);
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
