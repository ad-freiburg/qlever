// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructBatchProcessor.h"

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructQueryEvaluator.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "rdfTypes/RdfEscaping.h"

// _____________________________________________________________________________
ConstructBatchProcessor::ConstructBatchProcessor(
    std::shared_ptr<const InstantiationBlueprint> blueprint,
    const TableWithRange& table, ad_utility::MediaType format,
    size_t currentRowOffset)
    : blueprint_(std::move(blueprint)),
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
    blueprint_->cancellationHandle_->throwIfCancelled();

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

  batchCache_ = evaluateBatchColumnOriented(tableWithVocab_.idTable(),
                                            tableWithVocab_.localVocab(),
                                            batchRowIndices, currentRowOffset_);

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
  while (tripleIdx_ < blueprint_->numTemplateTriples()) {
    auto subject =
        getTermStringPtr(tripleIdx_, 0, *batchCache_, rowInBatchIdx_);
    auto predicate =
        getTermStringPtr(tripleIdx_, 1, *batchCache_, rowInBatchIdx_);
    auto object = getTermStringPtr(tripleIdx_, 2, *batchCache_, rowInBatchIdx_);

    ++tripleIdx_;

    std::string formatted = formatTriple(subject, predicate, object);
    if (!formatted.empty()) {
      return formatted;
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
// Batch Evaluation (Column-Oriented Processing of multiple result-table rows)
//
// Evaluates Variables and BlankNodes for a batch of rows.
// Column-oriented access pattern for variables:
//   for each variable V occurring in the template triples:
//     for each row R in batch:
//       read idTable[column(V)][R]    <-- Sequential reads within a column
//
// This is more cache-friendly than row-oriented access, because the memory
// layout of `IdTable` is column-major.
BatchEvaluationCache ConstructBatchProcessor::evaluateBatchColumnOriented(
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset) {
  BatchEvaluationCache batchCache;
  batchCache.numRows_ = rowIndices.size();

  evaluateVariablesForBatch(batchCache, idTable, localVocab, rowIndices,
                            currentRowOffset);
  evaluateBlankNodesForBatch(batchCache, rowIndices, currentRowOffset);

  return batchCache;
}

// _____________________________________________________________________________
void ConstructBatchProcessor::evaluateVariablesForBatch(
    BatchEvaluationCache& batchCache, const IdTable& idTable,
    const LocalVocab& localVocab, ql::span<const uint64_t> rowIndices,
    size_t currentRowOffset) {
  const size_t numRows = rowIndices.size();
  auto& cacheStats = statsLogger_->stats();
  const auto& variablesToEvaluate = blueprint_->variablesToEvaluate_;

  // Initialize variable strings: [varIdx][rowInBatch]
  // shared_ptr defaults to nullptr, representing UNDEF values.
  batchCache.variableStrings_.resize(variablesToEvaluate.size());
  for (auto& column : batchCache.variableStrings_) {
    column.resize(numRows);
  }

  // Evaluate variables column-by-column for better cache locality.
  // The IdTable is accessed sequentially for each column.
  for (size_t varIdx = 0; varIdx < variablesToEvaluate.size(); ++varIdx) {
    const auto& varInfo = variablesToEvaluate[varIdx];
    auto& columnStrings = batchCache.variableStrings_[varIdx];

    if (!varInfo.columnIndex_.has_value()) {
      // Variable not in result - all values are nullptr (already default).
      continue;
    }

    const size_t colIdx = varInfo.columnIndex_.value();

    // Read all IDs from this column for all rows in the batch,
    // look up their string values in the cache, and share them with the batch.
    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      Id id = idTable(rowIdx, colIdx);

      // Use LRU cache's getOrCompute: returns cached value or computes and
      // caches it.
      size_t missesBefore = cacheStats.misses_;
      const VariableToColumnMap& varCols = blueprint_->variableColumns_.get();
      const Index& idx = blueprint_->index_.get();
      const std::shared_ptr<const std::string>& cachedValue =
          idCache_->getOrCompute(id, [&cacheStats, &varCols, &idx, &colIdx,
                                      rowIdx, &idTable, &localVocab,
                                      currentRowOffset](const Id&) {
            ++cacheStats.misses_;
            ConstructQueryExportContext context{
                rowIdx, idTable, localVocab, varCols, idx, currentRowOffset};
            auto value = ConstructQueryEvaluator::evaluateWithColumnIndex(
                colIdx, context);
            if (value.has_value()) {
              return std::make_shared<const std::string>(std::move(*value));
            }
            return std::shared_ptr<const std::string>{};
          });

      if (cacheStats.misses_ == missesBefore) {
        ++cacheStats.hits_;
      }

      columnStrings[rowInBatch] = cachedValue;
    }
  }
}

// _____________________________________________________________________________
void ConstructBatchProcessor::evaluateBlankNodesForBatch(
    BatchEvaluationCache& batchCache, ql::span<const uint64_t> rowIndices,
    size_t currentRowOffset) const {
  const size_t numRows = rowIndices.size();
  const auto& blankNodesToEvaluate = blueprint_->blankNodesToEvaluate_;

  // Initialize blank node values: [blankNodeIdx][rowInBatch]
  batchCache.blankNodeValues_.resize(blankNodesToEvaluate.size());
  for (auto& column : batchCache.blankNodeValues_) {
    column.resize(numRows);
  }

  // Evaluate blank nodes using precomputed prefix and suffix.
  // Only the row number needs to be concatenated per row.
  // Format: prefix + (currentRowOffset + rowIdx) + suffix
  for (size_t blankIdx = 0; blankIdx < blankNodesToEvaluate.size();
       ++blankIdx) {
    const BlankNodeFormatInfo& formatInfo = blankNodesToEvaluate[blankIdx];
    auto& columnValues = batchCache.blankNodeValues_[blankIdx];

    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      // Use precomputed prefix and suffix, only concatenate row number.
      columnValues[rowInBatch] = absl::StrCat(
          formatInfo.prefix_, currentRowOffset + rowIdx, formatInfo.suffix_);
    }
  }
}

// _____________________________________________________________________________
std::shared_ptr<const std::string> ConstructBatchProcessor::getTermStringPtr(
    size_t tripleIdx, size_t pos, const BatchEvaluationCache& batchCache,
    size_t rowIdxInBatch) const {
  const TriplePatternInfo& info = blueprint_->triplePatternInfos_[tripleIdx];
  const TriplePatternInfo::TermLookupInfo& lookup = info.lookups_[pos];

  switch (lookup.type) {
    case TriplePatternInfo::TermType::CONSTANT: {
      return std::make_shared<std::string>(
          blueprint_->precomputedConstants_[tripleIdx][pos]);
    }
    case TriplePatternInfo::TermType::VARIABLE: {
      // Variable shared_ptr are stored in the batch cache, eliminating
      // hash lookups during instantiation.
      return batchCache.getVariableString(lookup.index, rowIdxInBatch);
    }
    case TriplePatternInfo::TermType::BLANK_NODE: {
      // Blank node values are always valid (computed for each row).
      return std::make_shared<const std::string>(
          batchCache.getBlankNodeValue(lookup.index, rowIdxInBatch));
    }
  }
  // TODO<ms2144>: I do not think it is good to ever return a nullptr.
  // We should probably throw an exception here or sth.
  return nullptr;  // Unreachable
}

// _____________________________________________________________________________
std::pair<std::shared_ptr<ConstructBatchProcessor::IdCache>,
          std::shared_ptr<ConstructBatchProcessor::IdCacheStatsLogger>>
ConstructBatchProcessor::createIdCacheWithStats(size_t numRows) const {
  // Cache capacity is sized to maximize cross-batch cache hits on repeated
  // values (e.g., predicates that appear in many rows).
  const size_t numVars = blueprint_->variablesToEvaluate_.size();
  const size_t minCapacityForBatch = ConstructBatchProcessor::getBatchSize() *
                                     std::max(numVars, size_t{1}) * 2;
  const size_t capacity =
      std::max(CONSTRUCT_ID_CACHE_MIN_CAPACITY, minCapacityForBatch);
  auto idCache = std::make_shared<IdCache>(capacity);
  auto statsLogger = std::make_shared<IdCacheStatsLogger>(numRows, capacity);
  return {std::move(idCache), std::move(statsLogger)};
}

// _____________________________________________________________________________
// Formats triples for different output formats without intermediate
// `StringTriple` allocations.
std::string ConstructBatchProcessor::formatTriple(
    const std::shared_ptr<const std::string>& subject,
    const std::shared_ptr<const std::string>& predicate,
    const std::shared_ptr<const std::string>& object) const {
  if (!subject || !predicate || !object) {
    return {};
  }

  switch (format_) {
    case ad_utility::MediaType::turtle: {
      // Only escape literals (strings starting with "). IRIs and blank nodes
      // are used as-is, avoiding an unnecessary string copy.
      if (ql::starts_with(*object, "\"")) {
        return absl::StrCat(*subject, " ", *predicate, " ",
                            RdfEscaping::validRDFLiteralFromNormalized(*object),
                            " .\n");
      }
      return absl::StrCat(*subject, " ", *predicate, " ", *object, " .\n");
    }
    case ad_utility::MediaType::csv: {
      return absl::StrCat(RdfEscaping::escapeForCsv(*subject), ",",
                          RdfEscaping::escapeForCsv(*predicate), ",",
                          RdfEscaping::escapeForCsv(*object), "\n");
    }
    case ad_utility::MediaType::tsv: {
      return absl::StrCat(RdfEscaping::escapeForTsv(*subject), "\t",
                          RdfEscaping::escapeForTsv(*predicate), "\t",
                          RdfEscaping::escapeForTsv(*object), "\n");
    }
    default:
      return {};  // TODO<ms2144>: add proper error throwing here?
  }
}
