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

// Bundles the pieces `computeBatch` needs beyond the batch itself: the
// preprocessed template, the index, the ID cache, the cancellation handle,
// and (optionally) the deduplicator to consult while instantiating.
struct BatchEvalContext {
  const PreprocessedConstructTemplate& preprocessedTemplate_;
  const Index& index_;
  IdCache& cache_;
  const CancellationHandle& cancellationHandle_;
  std::optional<std::reference_wrapper<ConstructDeduplicator>> deduplicator_;
};

// Evaluate the rows covered by `batch.view_`. Cancellation is checked once at
// the start. When `context.deduplicator_` is set, duplicate triples are
// dropped as they are instantiated (see `instantiateBatch`'s
// `DeduplicationParams`).
CPP_template(typename ChunkView)(requires ranges::range<ChunkView>)
    std::vector<EvaluatedTriple> computeBatch(
        const TableConstRefWithVocab& tableWithVocab, ChunkView batch,
        const BatchEvalContext& context, size_t tableRowOffset) {
  context.cancellationHandle_->throwIfCancelled();
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(batch));

  const size_t batchBegin = *ql::ranges::begin(batch);
  const size_t batchEnd =
      batchBegin + static_cast<size_t>(ql::ranges::size(batch));

  const BatchEvaluationContext ctx{tableWithVocab.idTable(), batchBegin,
                                   batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      context.preprocessedTemplate_.uniqueVariableColumns_, ctx,
      tableWithVocab.localVocab(), context.index_, context.cache_);

  const size_t blankNodeBaseId = tableRowOffset + batchBegin;
  std::optional<DeduplicationParams> deduplication;
  if (context.deduplicator_) {
    deduplication.emplace(DeduplicationParams{*context.deduplicator_, ctx});
  }
  return instantiateBatch(context.preprocessedTemplate_, batchResult,
                          blankNodeBaseId, deduplication);
}

// Chunks `table.view_` into batches and evaluates each one. Rebuilds the
// (cheap) deduplicator reference per chunk so no reference into this
// function's locals is captured beyond the chunk's own evaluation.
auto processTableBatches(
    const TableWithRange& table,
    const PreprocessedConstructTemplate& preprocessedTemplate,
    const Index& index, const CancellationHandle& cancellationHandle,
    IdCache& cache, const std::shared_ptr<ConstructDeduplicator>& deduplicator,
    size_t tableRowOffset) {
  return ranges::views::chunk(table.view_,
                              ConstructTripleGenerator::BATCH_SIZE) |
         ql::views::transform([&table, &preprocessedTemplate, &index, &cache,
                               cancellationHandle, &deduplicator,
                               tableRowOffset](auto chunkView) {
           std::optional<std::reference_wrapper<ConstructDeduplicator>>
               deduplicatorRef;
           if (deduplicator) {
             deduplicatorRef.emplace(*deduplicator);
           }
           BatchEvalContext context{preprocessedTemplate, index, cache,
                                    cancellationHandle, deduplicatorRef};
           return computeBatch(table.tableWithVocab_, chunkView, context,
                               tableRowOffset);
         }) |
         ql::views::join;
}
}  // namespace

//______________________________________________________________________________
InputRangeTypeErased<EvaluatedTriple> ConstructTripleGenerator::evaluateTables(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    ad_utility::InputRangeTypeErased<TableWithRange> rowIndices,
    size_t rowOffset, EvaluationConfig config) {
  auto preprocessedTemplate = ConstructTemplatePreprocessor::preprocess(
      templateTriples, variableColumns, config.index_);
  IdCache cache = makeIdCache(preprocessedTemplate);

  std::shared_ptr<ConstructDeduplicator> deduplicator;
  if (!std::holds_alternative<DeduplicationMode::None>(config.mode_.value_)) {
    deduplicator =
        std::make_shared<ConstructDeduplicator>(config.mode_, config.qec_);
  }

  auto preprocessedTemplatePtr =
      std::make_shared<const PreprocessedConstructTemplate>(
          std::move(preprocessedTemplate));

  auto processTable =
      [preprocessedTemplate = std::move(preprocessedTemplatePtr),
       index = &config.index_, cancellationHandle = config.cancellationHandle_,
       cache = std::move(cache), deduplicator = std::move(deduplicator),
       accumulatedRowOffset = rowOffset](const TableWithRange& table) mutable {
        const size_t numRowsOfTable = ql::ranges::size(table.view_);

        const size_t tableRowOffset = accumulatedRowOffset;
        accumulatedRowOffset += numRowsOfTable;

        return processTableBatches(table, *preprocessedTemplate, *index,
                                   cancellationHandle, cache, deduplicator,
                                   tableRowOffset);
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
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    ad_utility::MediaType mediaType, EvaluationConfig config) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColums, std::move(rowIndices),
                     rowOffset, std::move(config));

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
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
    EvaluationConfig config) {
  auto evaluatedTriples =
      evaluateTables(templateTriples, variableColums, std::move(rowIndices),
                     rowOffset, std::move(config));

  auto transformer = [](const EvaluatedTriple& triple) {
    return createStringTriple(triple);
  };
  return InputRangeTypeErased(std::move(evaluatedTriples) |
                              ql::views::transform(transformer));
}

}  // namespace qlever::constructExport
