// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructRowProcessor.h"

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
std::optional<InstantiatedTriple> ConstructRowProcessor::get() {
  while (batchStart_ < rowIndicesVec_.size()) {
    cancellationHandle_->throwIfCancelled();

    loadBatchIfNeeded();

    if (auto result = processCurrentBatch()) {
      return result;
    }

    advanceToNextBatch();
  }

  return std::nullopt;
}

// _____________________________________________________________________________
void ConstructRowProcessor::loadBatchIfNeeded() {
  if (batchEvaluationResult_.has_value()) {
    return;
  }
  const size_t batchEnd =
      std::min(batchStart_ + batchSize_, rowIndicesVec_.size());

  auto batchRowIndices = ql::span<const uint64_t>(
      rowIndicesVec_.data() + batchStart_, batchEnd - batchStart_);

  BatchEvaluationContext batchContext{tableWithVocab_.idTable(),
                                      tableWithVocab_.localVocab(),
                                      batchRowIndices, currentRowOffset_};
  batchEvaluationResult_ = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate_.uniqueVariableColumns_, batchContext, index_.get(),
      *idCache_);

  rowInBatchIdx_ = 0;
  tripleIdx_ = 0;
}

// _____________________________________________________________________________
std::optional<InstantiatedTriple> ConstructRowProcessor::processCurrentBatch() {
  while (rowInBatchIdx_ < batchEvaluationResult_->numRows_) {
    if (auto result = processCurrentRow()) {
      return result;
    }
    advanceToNextRow();
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<InstantiatedTriple> ConstructRowProcessor::processCurrentRow() {
  const size_t blankNodeRowId = currentBlankNodeRowId();

  while (tripleIdx_ < preprocessedTemplate_.preprocessedTriples_.size()) {
    const auto& triple = preprocessedTemplate_.preprocessedTriples_[tripleIdx_];

    auto subject = ConstructTripleInstantiator::instantiateTerm(
        triple[0], *batchEvaluationResult_, rowInBatchIdx_, blankNodeRowId);
    auto predicate = ConstructTripleInstantiator::instantiateTerm(
        triple[1], *batchEvaluationResult_, rowInBatchIdx_, blankNodeRowId);
    auto object = ConstructTripleInstantiator::instantiateTerm(
        triple[2], *batchEvaluationResult_, rowInBatchIdx_, blankNodeRowId);

    auto instantiatedTriple = InstantiatedTriple{subject, predicate, object};

    ++tripleIdx_;

    if (instantiatedTriple.isComplete()) {
      return instantiatedTriple;
    }
  }

  return std::nullopt;
}

// _____________________________________________________________________________
void ConstructRowProcessor::advanceToNextRow() {
  ++rowInBatchIdx_;
  tripleIdx_ = 0;
}

// _____________________________________________________________________________
void ConstructRowProcessor::advanceToNextBatch() {
  batchStart_ += batchSize_;
  batchEvaluationResult_.reset();
}

// _____________________________________________________________________________
size_t ConstructRowProcessor::currentBlankNodeRowId() const {
  return currentRowOffset_ + rowIndicesVec_[batchStart_ + rowInBatchIdx_];
}

// _____________________________________________________________________________
std::shared_ptr<ConstructRowProcessor::IdCache>
ConstructRowProcessor::createIdCache() const {
  const size_t numVars = preprocessedTemplate_.uniqueVariableColumns_.size();
  const size_t capacity =
      getBatchSize() * std::max(numVars, size_t{1}) * CACHE_CAPACITY_FACTOR;
  return std::make_shared<IdCache>(capacity);
}
