// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

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
      rowIndicesVec_(ql::ranges::begin(table.view_),
                     ql::ranges::end(table.view_)),
      currentRowOffset_(currentRowOffset) {}

// _____________________________________________________________________________
std::optional<EvaluatedTriple> ConstructRowProcessor::get() {
  while (true) {
    if (tripleIdx_ < currentBatchTriples_.size()) {
      return std::move(currentBatchTriples_[tripleIdx_++]);
    }
    if (batchStart_ >= rowIndicesVec_.size()) return std::nullopt;
    cancellationHandle_->throwIfCancelled();
    currentBatchTriples_ = computeBatch();
    batchStart_ += batchSize_;
    tripleIdx_ = 0;
  }
}

// _____________________________________________________________________________
std::vector<EvaluatedTriple> ConstructRowProcessor::computeBatch() {
  const size_t batchEnd =
      std::min(batchStart_ + batchSize_, rowIndicesVec_.size());
  BatchEvaluationContext batchContext{
      tableWithVocab_.idTable(), tableWithVocab_.localVocab(),
      rowIndicesVec_[batchStart_], rowIndicesVec_[batchEnd - 1] + 1};
  auto batchResult = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate_.uniqueVariableColumns_, batchContext, index_.get(),
      *idCache_);

  std::vector<EvaluatedTriple> triples;
  for (size_t rowInBatch = 0; rowInBatch < batchResult.numRows_; ++rowInBatch) {
    const size_t blankNodeRowId =
        currentRowOffset_ + rowIndicesVec_[batchStart_ + rowInBatch];
    for (const auto& triple : preprocessedTemplate_.preprocessedTriples_) {
      auto subject = ConstructTripleInstantiator::instantiateTerm(
          triple[0], batchResult, rowInBatch, blankNodeRowId);
      auto predicate = ConstructTripleInstantiator::instantiateTerm(
          triple[1], batchResult, rowInBatch, blankNodeRowId);
      auto object = ConstructTripleInstantiator::instantiateTerm(
          triple[2], batchResult, rowInBatch, blankNodeRowId);
      if (subject && predicate && object) {
        triples.push_back(InstantiatedTriple{*subject, *predicate, *object});
      }
    }
  }
  return triples;
}

// _____________________________________________________________________________
std::shared_ptr<ConstructRowProcessor::IdCache>
ConstructRowProcessor::createIdCache() const {
  const size_t numVars = preprocessedTemplate_.uniqueVariableColumns_.size();
  const size_t capacity =
      getBatchSize() * std::max(numVars, size_t{1}) * CACHE_CAPACITY_FACTOR;
  return std::make_shared<IdCache>(capacity);
}

}  // namespace qlever::constructExport
