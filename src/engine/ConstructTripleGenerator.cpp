// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructDeduplicator.h"
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

namespace {

// Filter `triples` through `deduplicator`, dropping duplicates.  The
// deduplicator is mutated in-place so that its state persists across batches.
// Only called when `DeduplicationMode` is not `None`.
static std::vector<EvaluatedTriple> filterDuplicates(
    std::vector<EvaluatedTriple> triples,
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationContext& ctx, size_t batchBegin,
    ConstructDeduplicator& deduplicator) {
  std::vector<EvaluatedTriple> deduped;
  deduped.reserve(triples.size());
  for (size_t row = 0; row < ctx.numRows(); ++row) {
    const size_t rowIdx = batchBegin + row;
    for (size_t t = 0; t < tmpl.preprocessedTriples_.size(); ++t) {
      const size_t idx = row * tmpl.preprocessedTriples_.size() + t;
      if (deduplicator.isNew(t, rowIdx, tmpl, ctx)) {
        deduped.push_back(std::move(triples[idx]));
      }
    }
  }
  return deduped;
}

// Evaluate the rows covered by `batch.view_`. Cancellation is checked once at
// the start.
CPP_template(typename ChunkView)(requires ranges::range<ChunkView>)
    std::vector<EvaluatedTriple> computeBatch(
        const TableConstRefWithVocab& tableWithVocab, ChunkView batch,
        const PreprocessedConstructTemplate& preprocessedTemplate,
        const Index& index, IdCache& cache, size_t tableRowOffset,
        const CancellationHandle& cancellationHandle) {
  cancellationHandle->throwIfCancelled();
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(batch));

  const size_t batchBegin = *ql::ranges::begin(batch);
  const size_t batchEnd =
      batchBegin + static_cast<size_t>(ql::ranges::size(batch));

  const BatchEvaluationContext ctx{tableWithVocab.idTable(), batchBegin,
                                   batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate.uniqueVariableColumns_, ctx,
      tableWithVocab.localVocab(), index, cache);

  const size_t blankNodeBaseId = tableRowOffset + batchBegin;
  return instantiateBatch(preprocessedTemplate, batchResult, blankNodeBaseId);
}
}  // namespace

//______________________________________________________________________________
InputRangeTypeErased<EvaluatedTriple> ConstructTripleGenerator::evaluateTables(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    const Index& index, CancellationHandle cancellationHandle,
    ad_utility::InputRangeTypeErased<TableWithRange> rowIndices,
    size_t rowOffset, const QueryExecutionContext& qec,
    ad_utility::DeduplicationMode mode) {
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns, index);
  IdCache cache = makeIdCache(preprocessedTemplate);

  // Deduplicator persists across batches within a single query.
  std::shared_ptr<ConstructDeduplicator> deduplicator;
  if (!std::holds_alternative<DeduplicationMode::None>(mode.value_)) {
    deduplicator = std::make_shared<ConstructDeduplicator>(mode, qec);
  }

  auto preprocessedTemplatePtr =
      std::make_shared<const PreprocessedConstructTemplate>(
          std::move(preprocessedTemplate));

  auto processTable =
      [preprocessedTemplate = std::move(preprocessedTemplatePtr), &index,
       cancellationHandle, cache = std::move(cache),
       deduplicator = std::move(deduplicator),
       accumulatedRowOffset = rowOffset](const TableWithRange& table) mutable {
        const size_t numRowsOfTable = ql::ranges::size(table.view_);

        const size_t tableRowOffset = accumulatedRowOffset;
        accumulatedRowOffset += numRowsOfTable;

        return ranges::views::chunk(table.view_, BATCH_SIZE) |
               ql::views::transform([&table, &preprocessedTemplate, &index,
                                     &cache, cancellationHandle, tableRowOffset,
                                     &deduplicator](auto chunkView) {
                 auto triples = computeBatch(
                     table.tableWithVocab_, chunkView, *preprocessedTemplate,
                     index, cache, tableRowOffset, cancellationHandle);
                 if (deduplicator) {
                   const size_t batchBegin = *ql::ranges::begin(chunkView);
                   const BatchEvaluationContext ctx{
                       table.tableWithVocab_.idTable(), batchBegin,
                       batchBegin +
                           static_cast<size_t>(ql::ranges::size(chunkView))};
                   return filterDuplicates(std::move(triples),
                                           *preprocessedTemplate, ctx,
                                           batchBegin, *deduplicator);
                 }
                 return triples;
               }) |
               ql::views::join;
      };

  auto pipeline = std::move(rowIndices) |
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
    ad_utility::MediaType mediaType, const QueryExecutionContext& qec,
    ad_utility::DeduplicationMode mode) {
  auto evaluatedTriples = evaluateTables(
      templateTriples, variableColums, index, std::move(cancellationhandle),
      std::move(rowIndices), rowOffset, qec, mode);

  auto transformer = [mediaType](const EvaluatedTriple& triple) {
    return formatTriple(triple, mediaType);
  };
  return InputRangeTypeErased(std::move(evaluatedTriples) |
                              ql::views::transform(transformer));
}

//______________________________________________________________________________
InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriples(
    const Triples& templateTriples, const VariableToColumnMap& variableColums,
    const Index& index, CancellationHandle cancellationhandle,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    const QueryExecutionContext& qec, ad_utility::DeduplicationMode mode) {
  auto evaluatedTriples = evaluateTables(
      templateTriples, variableColums, index, std::move(cancellationhandle),
      std::move(rowIndices), rowOffset, qec, mode);

  auto transformer = [](const EvaluatedTriple& triple) {
    return createStringTriple(triple);
  };
  return InputRangeTypeErased(std::move(evaluatedTriples) |
                              ql::views::transform(transformer));
}

}  // namespace qlever::constructExport
