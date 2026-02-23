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
ConstructRowProcessor::ConstructRowProcessor(
    const PreprocessedConstructTemplate& preprocessedTemplate,
    const Index& index, CancellationHandle cancellationHandle,
    const TableWithRange& table, size_t currentRowOffset)
    : preprocessedTemplate_(preprocessedTemplate),
      index_(index),
      cancellationHandle_(std::move(cancellationHandle)),
      tableWithVocab_(table.tableWithVocab_),
      numRows_(static_cast<size_t>(ql::ranges::distance(table.view_))),
      firstRow_(numRows_ > 0
                    ? static_cast<size_t>(*ql::ranges::begin(table.view_))
                    : 0),
      currentRowOffset_(currentRowOffset),
      idCache_(DEFAULT_BATCH_SIZE *
               std::max(preprocessedTemplate.uniqueVariableColumns_.size(),
                        size_t{1}) *
               CACHE_CAPACITY_FACTOR) {
  const size_t numBatches =
      (numRows_ + DEFAULT_BATCH_SIZE - 1) / DEFAULT_BATCH_SIZE;
  innerRange_ = ad_utility::InputRangeTypeErased<EvaluatedTriple>{
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
  const size_t batchEnd = std::min(batchStart + DEFAULT_BATCH_SIZE, numRows_);

  BatchEvaluationContext batchContext{
      tableWithVocab_.idTable(), firstRow_ + batchStart, firstRow_ + batchEnd};

  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate_.uniqueVariableColumns_, batchContext,
      tableWithVocab_.localVocab(), index_.get(), idCache_);

  std::vector<EvaluatedTriple> triples;
  triples.reserve(batchResult.numRows_ *
                  preprocessedTemplate_.preprocessedTriples_.size());
  for (size_t rowInBatch = 0; rowInBatch < batchResult.numRows_; ++rowInBatch) {
    const size_t blankNodeRowId =
        currentRowOffset_ + firstRow_ + batchStart + rowInBatch;
    for (const auto& triple : preprocessedTemplate_.preprocessedTriples_) {
      auto subject = ConstructTripleInstantiator::instantiateTerm(
          triple[0], batchResult, rowInBatch, blankNodeRowId);
      auto predicate = ConstructTripleInstantiator::instantiateTerm(
          triple[1], batchResult, rowInBatch, blankNodeRowId);
      auto object = ConstructTripleInstantiator::instantiateTerm(
          triple[2], batchResult, rowInBatch, blankNodeRowId);
      if (subject && predicate && object) {
        triples.push_back(EvaluatedTriple{*subject, *predicate, *object});
      }
    }
  }
  return triples;
}

}  // namespace qlever::constructExport
