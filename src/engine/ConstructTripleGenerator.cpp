// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleGenerator.h"

#include <iterator>

#include "backports/concepts.h"
#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructDeduplicationFilter.h"
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
        CancellationHandle cancellationHandle,
        ConstructDeduplicationState& deduplicationState,
        bool& groundTriplesEmitted, const DeduplicationMode& mode) {
  cancellationHandle->throwIfCancelled();
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(batch));

  const size_t batchBegin = *ql::ranges::begin(batch);
  const size_t batchEnd =
      batchBegin + static_cast<size_t>(ql::ranges::size(batch));

  BatchEvaluationContext ctx{tableWithVocab.idTable(), batchBegin, batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate.uniqueVariableColumns_, ctx,
      tableWithVocab.localVocab(), index, cache);

  // The absolute (query-global) row index of the batch's first row, used to
  // generate blank node IDs that stay unique across batches and tables.
  const size_t blankNodeBaseId = tableRowOffset + batchBegin;

  // Here "deduplication" means suppressing duplicate CONSTRUCT triples in the
  // result set (the same instantiated triple produced by multiple result rows).
  // `None` mode emits every instantiated triple and ignores duplicates; the
  // other modes use the deduplicating overload, which consults
  // `deduplicationState` (one filter per template triple) and drops duplicates.
  std::vector<EvaluatedTriple> instantiated =
      std::holds_alternative<DeduplicationMode::None>(mode.value)
          ? instantiateBatch(preprocessedTemplate, batchResult, blankNodeBaseId)
          : instantiateBatch(preprocessedTemplate, batchResult, blankNodeBaseId,
                             deduplicationState, ctx);

  // Ground triples produce the identical triple for every solution, so they are
  // emitted exactly once, before the first non-ground triple, on the first
  // non-empty batch (`computeBatch` is only called for non-empty batches).
  // This holds for every mode, including `None`.
  if (groundTriplesEmitted || preprocessedTemplate.groundTriples_.empty()) {
    return instantiated;
  }
  groundTriplesEmitted = true;
  std::vector<EvaluatedTriple> triples = preprocessedTemplate.groundTriples_;
  triples.insert(triples.end(), std::make_move_iterator(instantiated.begin()),
                 std::make_move_iterator(instantiated.end()));
  return triples;
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

  // One per-triple filter per template triple, initialized for the given mode.
  ConstructDeduplicationState deduplicationState = makeDeduplicationState(
      mode, preprocessedTemplate.preprocessedTriples_.size());

  // `queryOffset`: the value of the SPARQL OFFSET clause, used to initialize
  // `accumulatedRowOffset` so that blank node IDs are globally unique across
  // paginated results.
  // `accumulatedRowOffset`: the global row index of the first row of the
  // current `IdTable` block, accumulated across all previously processed
  // blocks. Starts at `queryOffset`.
  // `batchBegin`: the row index of the first row of the current evaluation
  // chunk within the current `IdTable` block.
  // `groundTriplesEmitted`: Whether the ground template triples have already
  // been emitted. They are emitted once for the whole query, so this flag
  // persists across all batches and tables (the lambda owning it outlives every
  // call).
  auto processTable = [preprocessedTemplate = std::move(preprocessedTemplate),
                       &index, cancellationHandle, cache = std::move(cache),
                       deduplicationState = std::move(deduplicationState),
                       accumulatedRowOffset = queryOffset,
                       groundTriplesEmitted = false,
                       mode](const TableWithRange& table) mutable {
    const size_t numRowsOfTable = ql::ranges::size(table.view_);

    // Snapshot the offset for this table, then advance it for the next table.
    const size_t tableRowOffset = accumulatedRowOffset;
    accumulatedRowOffset += numRowsOfTable;

    return ranges::views::chunk(table.view_, BATCH_SIZE) |
           ql::views::transform([&table, &preprocessedTemplate, &index, &cache,
                                 cancellationHandle, &deduplicationState,
                                 &groundTriplesEmitted, tableRowOffset,
                                 mode](auto chunkView) {
             return computeBatch(
                 table.tableWithVocab_, chunkView, preprocessedTemplate, index,
                 cache, tableRowOffset, cancellationHandle, deduplicationState,
                 groundTriplesEmitted, mode);
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
