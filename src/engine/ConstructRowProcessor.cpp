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

  BatchEvaluationContext batchContext{
      tableWithVocab_.idTable(), tableWithVocab_.localVocab(),
      rowIndicesVec_[batchStart_], rowIndicesVec_[batchEnd - 1] + 1};

  batchEvaluationResult_ = ConstructBatchEvaluator::evaluateBatch(
      preprocessedTemplate_.uniqueVariableColumns_, batchContext, index_.get(),
      *idCache_);

  rowInBatchIdx_ = 0;
  tripleIdx_ = 0;
}

// _____________________________________________________________________________
// TODO<ms2144>: comment from code review: "This gets much simpler,
// if you synchronously create a batch (as a vector of instantiated triple etc.)
// and then on the outside somewhere sprinkle in a views::join . The callback
// hell on the level of rows is unnecessary. Then we can see, how we can get
// the interface even nicer." I dont really get it. Think about what that means
// and try to implement it.
std::optional<EvaluatedTriple> ConstructRowProcessor::processCurrentBatch() {
  while (rowInBatchIdx_ < batchEvaluationResult_->numRows_) {
    if (auto result = processCurrentRow()) {
      return result;
    }
    advanceToNextRow();
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<EvaluatedTriple> ConstructRowProcessor::processCurrentRow() {
  const size_t blankNodeRowId = currentBlankNodeRowId();

  while (tripleIdx_ < preprocessedTemplate_.preprocessedTriples_.size()) {
    const auto& triple = preprocessedTemplate_.preprocessedTriples_[tripleIdx_];

    auto subject = ConstructTripleInstantiator::instantiateTerm(
        triple[0], *batchEvaluationResult_, rowInBatchIdx_, blankNodeRowId);
    auto predicate = ConstructTripleInstantiator::instantiateTerm(
        triple[1], *batchEvaluationResult_, rowInBatchIdx_, blankNodeRowId);
    auto object = ConstructTripleInstantiator::instantiateTerm(
        triple[2], *batchEvaluationResult_, rowInBatchIdx_, blankNodeRowId);

    ++tripleIdx_;

    // Skip triples where any term is undefined.
    if (!subject.has_value() || !predicate.has_value() || !object.has_value()) {
      continue;
    }

    return InstantiatedTriple{*subject, *predicate, *object};
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

}  // namespace qlever::constructExport
