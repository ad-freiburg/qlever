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
#include "util/InputRangeUtils.h"
#include "util/Views.h"

namespace qlever::constructExport {

using ad_utility::InputRangeTypeErased;
using StringTriple = QueryExecutionTree::StringTriple;

IdCache ConstructTripleGenerator::makeIdCache(
    const PreprocessedConstructTemplate& tmpl) {
  return IdCache{std::max(tmpl.uniqueVariableColumns_.size(), size_t{1}) *
                 CACHE_ENTRIES_PER_VARIABLE};
}

InputRangeTypeErased<EvaluatedTriple>
ConstructTripleGenerator::evaluateTableWithRange(
    const PreprocessedConstructTemplate& tmpl, const Index& index,
    CancellationHandle cancellationHandle, const TableWithRange& table,
    size_t rowOffset, IdCache& cache) {
  const size_t numRowsOfTable = ql::ranges::distance(table.view_);
  const size_t firstRowOfTable =
      numRowsOfTable > 0 ? *ql::ranges::begin(table.view_) : 0;
  const size_t numBatches =
      (numRowsOfTable + DEFAULT_BATCH_SIZE - 1) / DEFAULT_BATCH_SIZE;

  // this lambda computes the result for a batch of the `TableWithRange`.
  auto computeBatch = [&](int batchIdx) {
    const size_t batchStart = batchIdx * DEFAULT_BATCH_SIZE;
    const size_t batchEnd =
        std::min(batchStart + DEFAULT_BATCH_SIZE, numRowsOfTable);

    BatchEvaluationContext ctx{table.tableWithVocab_.idTable(),
                               firstRowOfTable + batchStart,
                               firstRowOfTable + batchEnd};

    auto batchResult = ConstructBatchEvaluator::evaluateBatch(
        tmpl.uniqueVariableColumns_, ctx, table.tableWithVocab_.localVocab(),
        index, cache);

    const size_t blankNodeBaseId = rowOffset + firstRowOfTable + batchStart;
    return instantiateBatch(tmpl, batchResult, blankNodeBaseId);
  };

  return InputRangeTypeErased(ql::views::iota(size_t{0}, numBatches) |
                              ql::views::transform(computeBatch) |
                              ql::views::join);
}

//______________________________________________________________________________
InputRangeTypeErased<EvaluatedTriple> ConstructTripleGenerator::evaluateTables(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    const Index& index, CancellationHandle cancellationhandle,
    InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset) {
  auto tmpl = ConstructTemplatePreprocessor::preprocess(templateTriples,
                                                        variableColumns);
  IdCache cache = makeIdCache(tmpl);

  // TODO<ms2144>: 1) I dont understand what
  // CachingContinuableTransformInputRange is and why we need to use it here.
  // TODO<ms2144>: 2) what does ad_utility::allView do and why do we need it
  // here?
  // TODO<ms2144>: 3) what does ad_utility::LoopControl::yieldAll do and why do
  // we need it here?

  return InputRangeTypeErased(ad_utility::CachingContinuableTransformInputRange(
      ad_utility::allView(std::move(rowIndices)),
      [&tmpl, &index, cancellationhandle, rowOffset,
       cache = std::move(cache)](const TableWithRange& table) mutable {
        return ad_utility::LoopControl<EvaluatedTriple>::yieldAll(
            evaluateTableWithRange(tmpl, index, cancellationhandle, table,
                                   rowOffset, cache));
      }));
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

  return InputRangeTypeErased(ad_utility::CachingTransformInputRange(
      ad_utility::allView(std::move(evaluatedTriples)),
      [mediaType](const EvaluatedTriple& triple) {
        return formatTriple(triple, mediaType);
      }));
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

  return InputRangeTypeErased(ad_utility::CachingTransformInputRange(
      ad_utility::allView(std::move(evaluatedTriples)),
      [](const EvaluatedTriple& triple) {
        return createStringTriple(triple);
      }));
}

}  // namespace qlever::constructExport
