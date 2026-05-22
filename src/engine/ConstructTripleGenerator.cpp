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
#include "global/RuntimeParameters.h"
#include "util/Parameters.h"
#include "util/Views.h"

namespace qlever::constructExport {

using ad_utility::InputRangeTypeErased;
using StringTriple = QueryExecutionTree::StringTriple;
using ad_utility::DeduplicationMode;

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
        CancellationHandle cancellationHandle, SeenTriples& seenTriples,
        const DeduplicationMode& mode, size_t queryOffset) {
  cancellationHandle->throwIfCancelled();
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(batch));

  const size_t batchBegin = *ql::ranges::begin(batch);
  const size_t batchEnd =
      batchBegin + static_cast<size_t>(ql::ranges::size(batch));

  if (const auto* bw = std::get_if<DeduplicationMode::BatchWise>(&mode.value)) {
    // `tableRowOffset` + `batchbegin` gives the global row index across all
    // `IdTable` blocks. We subtract `queryOffset` (the OFFSET clause value) so
    // that window boundaries are aligned to the start of the actually processed
    // result rows, not the absolute start of the full result.
    if ((tableRowOffset + batchBegin - queryOffset) % bw->batchSize == 0) {
      seenTriples.clear();
    }
  }

  BatchEvaluationContext ctx{tableWithVocab.idTable(), batchBegin, batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate.uniqueVariableColumns_, ctx,
      tableWithVocab.localVocab(), index, cache);

  const size_t blankNodeBaseId = tableRowOffset + batchBegin;
  return instantiateBatch(preprocessedTemplate, batchResult, blankNodeBaseId,
                          mode, std::ref(seenTriples), std::ref(ctx));
}

//______________________________________________________________________________
InputRangeTypeErased<EvaluatedTriple> ConstructTripleGenerator::evaluateTables(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    const Index& index, CancellationHandle cancellationHandle,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t queryOffset,
    DeduplicationMode mode) {
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns);
  IdCache cache = makeIdCache(preprocessedTemplate);

  SeenTriples seenTriples;

  // Row index terminology used in this lambda and in `computeBatch`:
  //
  // TODO<ms2144>: The row index handling in this pipeline is confusing and
  // should be cleaned up and documented more carefully in the future.
  // `queryOffset`: the value of the SPARQL OFFSET clause. Row indices in the
  // result start at this value.
  // `accumulatedRowOffset`: The global row index of the first row of the
  // current `IdTable` block, accumulated across all previously processed
  // blocks. Starts at `queryOffset`.
  // `batchBegin`: the row index of the first row of the current evaluation
  // chunk within the current `IdTable` block. For the first block, this may be
  // non-zero if the OFFSET clause falls within that block; for all
  // subsequent blocks it is 0.
  // `accumulatedRowOffset + batchBegin`: intended to be the global row index of
  // the first row of the current chunk across all blocks.
  // `accumulatedRowOffset + batchBegin - queryOffset`: intended to be the index
  // of the first row of the current chunk relative to the start of the
  // processed result, used for aligning deduplication windows. However, due to
  // the confusion above, this may not be entirely correct.
  auto processTable = [preprocessedTemplate = std::move(preprocessedTemplate),
                       &index, cancellationHandle, cache = std::move(cache),
                       seenTriples = std::move(seenTriples),
                       accumulatedRowOffset = queryOffset, queryOffset,
                       mode](const TableWithRange& table) mutable {
    const size_t numRowsOfTable = ql::ranges::size(table.view_);

    // Snapshot the offset for this table, then advance it for the next table.
    const size_t tableRowOffset = accumulatedRowOffset;
    accumulatedRowOffset += numRowsOfTable;

    return ranges::views::chunk(table.view_, BATCH_SIZE) |
           ql::views::transform([&table, &preprocessedTemplate, &index, &cache,
                                 cancellationHandle, &seenTriples,
                                 tableRowOffset, mode,
                                 queryOffset](auto chunkView) {
             return computeBatch(table.tableWithVocab_, chunkView,
                                 preprocessedTemplate, index, cache,
                                 tableRowOffset, cancellationHandle,
                                 seenTriples, mode, queryOffset);
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
    ad_utility::MediaType mediaType, DeduplicationMode mode) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColums, index, cancellationhandle,
                     std::move(rowIndices), rowOffset, mode);

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
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    DeduplicationMode mode) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColums, index, cancellationhandle,
                     std::move(rowIndices), rowOffset, mode);

  auto transformer = [](const EvaluatedTriple& triple) {
    return createStringTriple(triple);
  };
  return InputRangeTypeErased(allView(std::move(evaluatedTriples)) |
                              ql::views::transform(transformer));
}

}  // namespace qlever::constructExport
