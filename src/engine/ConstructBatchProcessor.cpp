// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructBatchProcessor.h"

// _____________________________________________________________________________
ConstructBatchProcessor::ConstructBatchProcessor(
    std::shared_ptr<const PreprocessedConstructTemplate> blueprint,
    const TableWithRange& table, ad_utility::MediaType format,
    size_t currentRowOffset)
    : preprocessedConstructTemplate(std::move(blueprint)),
      format_(format),
      tableWithVocab_(table.tableWithVocab_),
      rowIndicesVec_(ql::ranges::begin(table.view_),
                     ql::ranges::end(table.view_)),
      currentRowOffset_(currentRowOffset),
      batchSize_(ConstructBatchProcessor::getBatchSize()) {
  auto [cache, logger] = createIdCacheWithStats(rowIndicesVec_.size());
  idCache_ = std::move(cache);
  statsLogger_ = std::move(logger);
}

// _____________________________________________________________________________
std::optional<std::string> ConstructBatchProcessor::get() {
  while (batchStart_ < rowIndicesVec_.size()) {
    preprocessedConstructTemplate->cancellationHandle_->throwIfCancelled();

    loadBatchIfNeeded();

    if (auto result = processCurrentBatch()) {
      return result;
    }

    advanceToNextBatch();
  }

  return std::nullopt;
}

// _____________________________________________________________________________
void ConstructBatchProcessor::loadBatchIfNeeded() {
  if (batchCache_.has_value()) {
    return;
  }
  const size_t batchEnd =
      std::min(batchStart_ + batchSize_, rowIndicesVec_.size());

  auto batchRowIndices = ql::span<const uint64_t>(
      rowIndicesVec_.data() + batchStart_, batchEnd - batchStart_);

  batchCache_ = ConstructBatchEvaluator::evaluateBatch(
      *preprocessedConstructTemplate, tableWithVocab_.idTable(),
      tableWithVocab_.localVocab(), batchRowIndices, currentRowOffset_,
      *idCache_, *statsLogger_);

  // After we are done processing the batch, reset the indices for iterating
  // over the rows/triples of the batch.
  rowInBatchIdx_ = 0;
  tripleIdx_ = 0;
}

// _____________________________________________________________________________
std::optional<std::string> ConstructBatchProcessor::processCurrentBatch() {
  while (rowInBatchIdx_ < batchCache_->numRows_) {
    if (auto result = processCurrentRow()) {
      return result;
    }
    advanceToNextRow();
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<std::string> ConstructBatchProcessor::processCurrentRow() {
  while (tripleIdx_ < preprocessedConstructTemplate->numTemplateTriples()) {
    auto subject = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 0, *preprocessedConstructTemplate, *batchCache_,
        rowInBatchIdx_);
    auto predicate = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 1, *preprocessedConstructTemplate, *batchCache_,
        rowInBatchIdx_);
    auto object = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 2, *preprocessedConstructTemplate, *batchCache_,
        rowInBatchIdx_);

    ++tripleIdx_;

    std::string formatted = ConstructTripleInstantiator::formatTriple(
        subject, predicate, object, format_);
    if (!formatted.empty()) {
      return formatted;
    }
    // Triple was UNDEF (incomplete), continue to next triple pattern.
  }

  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<ConstructBatchProcessor::StringTriple>
ConstructBatchProcessor::getStringTriple() {
  while (batchStart_ < rowIndicesVec_.size()) {
    preprocessedConstructTemplate->cancellationHandle_->throwIfCancelled();

    loadBatchIfNeeded();

    if (auto result = processCurrentBatchAsStringTriple()) {
      return result;
    }

    advanceToNextBatch();
  }

  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<ConstructBatchProcessor::StringTriple>
ConstructBatchProcessor::processCurrentBatchAsStringTriple() {
  while (rowInBatchIdx_ < batchCache_->numRows_) {
    if (auto result = processCurrentRowAsStringTriple()) {
      return result;
    }
    advanceToNextRow();
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<ConstructBatchProcessor::StringTriple>
ConstructBatchProcessor::processCurrentRowAsStringTriple() {
  while (tripleIdx_ < preprocessedConstructTemplate->numTemplateTriples()) {
    auto subject = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 0, *preprocessedConstructTemplate, *batchCache_,
        rowInBatchIdx_);
    auto predicate = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 1, *preprocessedConstructTemplate, *batchCache_,
        rowInBatchIdx_);
    auto object = ConstructTripleInstantiator::instantiateTerm(
        tripleIdx_, 2, *preprocessedConstructTemplate, *batchCache_,
        rowInBatchIdx_);

    ++tripleIdx_;

    StringTriple triple = ConstructTripleInstantiator::instantiateTriple(
        subject, predicate, object);
    if (!triple.isEmpty()) {
      return triple;
    }
    // Triple was UNDEF (incomplete), continue to next triple pattern.
  }

  return std::nullopt;
}

// _____________________________________________________________________________
void ConstructBatchProcessor::advanceToNextRow() {
  ++rowInBatchIdx_;
  tripleIdx_ = 0;
}

// _____________________________________________________________________________
void ConstructBatchProcessor::advanceToNextBatch() {
  batchStart_ += batchSize_;
  batchCache_.reset();
}

// _____________________________________________________________________________
std::pair<std::shared_ptr<ConstructBatchProcessor::IdCache>,
          std::shared_ptr<ConstructBatchProcessor::IdCacheStatsLogger>>
ConstructBatchProcessor::createIdCacheWithStats(size_t numRows) const {
  // Cache capacity is sized to maximize cross-batch cache hits on repeated
  // values (e.g., predicates that appear in many rows).
  const size_t numVars =
      preprocessedConstructTemplate->variablesToEvaluate_.size();
  const size_t minCapacityForBatch = ConstructBatchProcessor::getBatchSize() *
                                     std::max(numVars, size_t{1}) * 2;
  const size_t capacity =
      std::max(CONSTRUCT_ID_CACHE_MIN_CAPACITY, minCapacityForBatch);
  auto idCache = std::make_shared<IdCache>(capacity);
  auto statsLogger = std::make_shared<IdCacheStatsLogger>(numRows, capacity);
  return {std::move(idCache), std::move(statsLogger)};
}
