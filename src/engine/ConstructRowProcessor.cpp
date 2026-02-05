// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructRowProcessor.h"

// _____________________________________________________________________________
ConstructRowProcessor::ConstructRowProcessor(
    std::shared_ptr<const PreprocessedConstructTemplate>
        preprocessedConstructTemplate,
    const TableWithRange& table, size_t currentRowOffset)
    : preprocessedConstructTemplate_(std::move(preprocessedConstructTemplate)),
      tableWithVocab_(table.tableWithVocab_),
      rowIndicesVec_(ql::ranges::begin(table.view_),
                     ql::ranges::end(table.view_)),
      currentRowOffset_(currentRowOffset),
      batchSize_(ConstructRowProcessor::getBatchSize()) {
  idCache_ = createIdCache();
  // Logger references the cache's stats; logs when destroyed.
  statsLogger_ =
      std::make_unique<IdCacheStatsLogger>(rowIndicesVec_.size(), *idCache_);
}

// _____________________________________________________________________________
std::optional<InstantiatedTriple> ConstructRowProcessor::get() {
  while (batchStart_ < rowIndicesVec_.size()) {
    preprocessedConstructTemplate_->cancellationHandle_->throwIfCancelled();

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

  batchEvaluationResult_ = ConstructBatchEvaluator::evaluateBatch(
      *preprocessedConstructTemplate_, tableWithVocab_.idTable(),
      tableWithVocab_.localVocab(), batchRowIndices, currentRowOffset_,
      *idCache_);

  // After we are done processing the batch, reset the indices for iterating
  // over the rows/triples of the batch.
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
  // TODO<ms2144>: why do we return std::nullopt here?
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<InstantiatedTriple> ConstructRowProcessor::processCurrentRow() {
  while (tripleIdx_ < preprocessedConstructTemplate_->numTemplateTriples()) {
    auto subject = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 0, *preprocessedConstructTemplate_, *batchEvaluationResult_,
        rowInBatchIdx_);

    auto predicate = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 1, *preprocessedConstructTemplate_, *batchEvaluationResult_,
        rowInBatchIdx_);
    auto object = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 2, *preprocessedConstructTemplate_, *batchEvaluationResult_,
        rowInBatchIdx_);

    auto triple = InstantiatedTriple{subject, predicate, object};

    ++tripleIdx_;

    if (triple.isComplete()) {
      return triple;
    }
    // Triple was incomplete (has UNDEF components), continue to next pattern.
  }

  // TODO<ms2144> what does this return mean?
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
std::shared_ptr<ConstructRowProcessor::IdCache>
ConstructRowProcessor::createIdCache() const {
  const size_t numVars =
      preprocessedConstructTemplate_->variablesToInstantiate_.size();
  // Capacity: at least batch_size * num_vars * 2, or minimum capacity.
  const size_t minCapacityForBatch =
      ConstructRowProcessor::getBatchSize() * std::max(numVars, size_t{1}) * 2;
  const size_t capacity =
      std::max(CONSTRUCT_ID_CACHE_MIN_CAPACITY, minCapacityForBatch);
  return std::make_shared<IdCache>(capacity);
}
