// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructRowProcessor.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
IdCache ConstructRowProcessor::makeIdCache(
    const PreprocessedConstructTemplate& tmpl) {
  return IdCache(std::max(tmpl.uniqueVariableColumns_.size(), size_t{1}) *
                 CACHE_ENTRIES_PER_VARIABLE);
}

// _____________________________________________________________________________
ConstructRowProcessor::ConstructRowProcessor(
    const PreprocessedConstructTemplate& preprocessedTemplate,
    const Index& index, CancellationHandle cancellationHandle,
    const TableWithRange& table, size_t currentRowOffset)
    : preprocessedTemplate_(preprocessedTemplate),
      index_(index),
      cancellationHandle_(std::move(cancellationHandle)),
      tableWithVocab_(table.tableWithVocab_),
      rowIndices_(table.view_),
      currentRowOffset_(currentRowOffset),
      idCache_(makeIdCache(preprocessedTemplate)),
      innerRange_(makeInnerRange()) {}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<EvaluatedTriple>
ConstructRowProcessor::makeInnerRange() {
  const size_t numBatches =
      (numRows() + DEFAULT_BATCH_SIZE - 1) / DEFAULT_BATCH_SIZE;

  return ad_utility::InputRangeTypeErased{
      ad_utility::CachingContinuableTransformInputRange(
          ql::views::iota(size_t{0}, numBatches),
          [this](size_t batchIdx) -> ad_utility::LoopControl<EvaluatedTriple> {
            cancellationHandle_->throwIfCancelled();
            return ad_utility::LoopControl<EvaluatedTriple>::yieldAll(
                computeBatch(batchIdx * DEFAULT_BATCH_SIZE));
          })};
}

// _____________________________________________________________________________
std::optional<EvaluatedTriple> ConstructRowProcessor::get() {
  return innerRange_.get();
}

// _____________________________________________________________________________
std::vector<EvaluatedTriple> ConstructRowProcessor::computeBatch(
    size_t batchStart) {
  const size_t batchEnd = std::min(batchStart + DEFAULT_BATCH_SIZE, numRows());

  BatchEvaluationContext batchContext{tableWithVocab_.idTable(),
                                      firstRow() + batchStart,
                                      firstRow() + batchEnd};
  auto batchResult =
      evaluateBatch(preprocessedTemplate_.uniqueVariableColumns_, batchContext,
                    tableWithVocab_.localVocab(), index_.get(), idCache_);

  const size_t blankNodeBaseId = currentRowOffset_ + firstRow() + batchStart;
  return instantiateBatch(preprocessedTemplate_, batchResult, blankNodeBaseId);
}

}  // namespace qlever::constructExport
