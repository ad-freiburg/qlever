// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include <absl/strings/str_cat.h>

#include <cstdlib>

#include "engine/ExportQueryExecutionTrees.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/Log.h"

using ad_utility::InputRangeTypeErased;
using enum PositionInTriple;
using StringTriple = ConstructTripleGenerator::StringTriple;

// _____________________________________________________________________________
size_t ConstructTripleGenerator::getBatchSize() {
  static const size_t batchSize = []() {
    const char* envVal = std::getenv("QLEVER_CONSTRUCT_BATCH_SIZE");
    if (envVal != nullptr) {
      try {
        size_t val = std::stoull(envVal);
        if (val > 0) {
          AD_LOG_INFO << "Using CONSTRUCT batch size from environment: " << val
                      << "\n";
          return val;
        }
      } catch (...) {
        // Fall through to default
      }
    }
    return DEFAULT_BATCH_SIZE;
  }();
  return batchSize;
}

// _____________________________________________________________________________
void ConstructTripleGenerator::analyzeTemplate() {
  precomputedConstants_.resize(templateTriples_.size());
  triplePatternInfos_.resize(templateTriples_.size());

  for (size_t tripleIdx = 0; tripleIdx < templateTriples_.size(); ++tripleIdx) {
    const auto& triple = templateTriples_[tripleIdx];
    TriplePatternInfo& info = triplePatternInfos_[tripleIdx];

    for (size_t pos = 0; pos < 3; ++pos) {
      const GraphTerm& term = triple[pos];
      PositionInTriple role = static_cast<PositionInTriple>(pos);

      if (std::holds_alternative<Iri>(term)) {
        // Precompute IRI value
        auto val = ConstructQueryEvaluator::evaluate(std::get<Iri>(term));
        precomputedConstants_[tripleIdx][pos] = std::move(val);
        info.resolutions[pos] = {TermSource::CONSTANT, tripleIdx};

      } else if (std::holds_alternative<Literal>(term)) {
        // Precompute Literal value
        auto val =
            ConstructQueryEvaluator::evaluate(std::get<Literal>(term), role);
        precomputedConstants_[tripleIdx][pos] = std::move(val);
        info.resolutions[pos] = {TermSource::CONSTANT, tripleIdx};

      } else if (std::holds_alternative<Variable>(term)) {
        // Track Variable for per-row evaluation with pre-computed column index
        const Variable& var = std::get<Variable>(term);
        if (!variableToIndex_.contains(var)) {
          size_t idx = variablesToEvaluate_.size();
          variableToIndex_[var] = idx;
          // Pre-compute the column index to avoid hash lookups during
          // evaluation
          std::optional<size_t> columnIndex;
          if (variableColumns_.get().contains(var)) {
            columnIndex = variableColumns_.get().at(var).columnIndex_;
          }
          variablesToEvaluate_.push_back({var, columnIndex});
        }
        info.resolutions[pos] = {TermSource::VARIABLE, variableToIndex_[var]};

      } else if (std::holds_alternative<BlankNode>(term)) {
        // Track BlankNode for per-row evaluation with precomputed format
        const BlankNode& blankNode = std::get<BlankNode>(term);
        const std::string& label = blankNode.label();
        if (!blankNodeLabelToIndex_.contains(label)) {
          size_t idx = blankNodesToEvaluate_.size();
          blankNodeLabelToIndex_[label] = idx;
          // Precompute prefix ("_:g" or "_:u") and suffix ("_" + label)
          // so we only need to concatenate the row number per row
          BlankNodeFormatInfo formatInfo;
          formatInfo.prefix = blankNode.isGenerated() ? "_:g" : "_:u";
          formatInfo.suffix = absl::StrCat("_", label);
          blankNodesToEvaluate_.push_back(std::move(formatInfo));
        }
        info.resolutions[pos] = {TermSource::BLANK_NODE,
                                 blankNodeLabelToIndex_[label]};
      }
    }
  }
}

// _____________________________________________________________________________
ConstructTripleGenerator::ConstructTripleGenerator(
    Triples constructTriples, std::shared_ptr<const Result> result,
    const VariableToColumnMap& variableColumns, const Index& index,
    CancellationHandle cancellationHandle)
    : templateTriples_(std::move(constructTriples)),
      result_(std::move(result)),
      variableColumns_(variableColumns),
      index_(index),
      cancellationHandle_(std::move(cancellationHandle)) {
  // Analyze template: precompute constants and identify variables/blank nodes.
  analyzeTemplate();
}

// _____________________________________________________________________________
ConstructTripleGenerator::RowEvaluationCache
ConstructTripleGenerator::evaluateRowTerms(
    const ConstructQueryExportContext& context, IdCache& idCache,
    IdCacheStats& cacheStats) const {
  RowEvaluationCache rowCache;
  const IdTable& idTable = context.idTable_;
  size_t rowIdx = context.resultTableRowIndex_;

  // Evaluate all Variables for this row using pre-computed column indices
  // and caching ID-to-string conversions
  rowCache.variableValues.reserve(variablesToEvaluate_.size());
  for (const auto& varInfo : variablesToEvaluate_) {
    if (!varInfo.columnIndex.has_value()) {
      // Variable not in result
      rowCache.variableValues.push_back(std::nullopt);
      continue;
    }

    // Get the Id from the table
    Id id = idTable(rowIdx, varInfo.columnIndex.value());

    // Check the cache first
    auto it = idCache.find(id);
    if (it != idCache.end()) {
      // Cache hit - use the cached value
      ++cacheStats.hits;
      rowCache.variableValues.push_back(it->second);
    } else {
      // Cache miss - compute and cache the result
      ++cacheStats.misses;
      auto value = ConstructQueryEvaluator::evaluateWithColumnIndex(
          varInfo.columnIndex, context);
      idCache[id] = value;
      rowCache.variableValues.push_back(std::move(value));
    }
  }

  // Evaluate all BlankNodes for this row using precomputed prefix and suffix
  // Note: BlankNodes are not cached because their value depends on the row
  rowCache.blankNodeValues.reserve(blankNodesToEvaluate_.size());
  for (const BlankNodeFormatInfo& formatInfo : blankNodesToEvaluate_) {
    rowCache.blankNodeValues.push_back(absl::StrCat(
        formatInfo.prefix, context._rowOffset + context.resultTableRowIndex_,
        formatInfo.suffix));
  }

  return rowCache;
}

// _____________________________________________________________________________
ConstructTripleGenerator::BatchEvaluationCache
ConstructTripleGenerator::evaluateBatchColumnOriented(
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    IdCache& idCache, IdCacheStats& cacheStats) const {
  BatchEvaluationCache batchCache;
  const size_t numRows = rowIndices.size();
  batchCache.numRows = numRows;

  // Initialize variable Ids: [varIdx][rowInBatch]
  // We store Id values here and look up strings from idCache during
  // instantiation. This avoids storing strings in both idCache and batchCache.
  batchCache.variableIds.resize(variablesToEvaluate_.size());
  for (auto& column : batchCache.variableIds) {
    column.resize(numRows);
  }

  // Evaluate variables column-by-column for better cache locality
  // The IdTable is accessed sequentially for each column
  for (size_t varIdx = 0; varIdx < variablesToEvaluate_.size(); ++varIdx) {
    const auto& varInfo = variablesToEvaluate_[varIdx];
    auto& columnIds = batchCache.variableIds[varIdx];

    if (!varInfo.columnIndex.has_value()) {
      // Variable not in result - all values are nullopt (already default)
      continue;
    }

    const size_t colIdx = varInfo.columnIndex.value();

    // Read all IDs from this column for all rows in the batch
    // and ensure their string values are in the cache
    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      Id id = idTable(rowIdx, colIdx);

      // Store the Id in the batch cache
      columnIds[rowInBatch] = id;

      // Ensure the string value is in idCache (compute if not present)
      auto it = idCache.find(id);
      if (it != idCache.end()) {
        ++cacheStats.hits;
      } else {
        ++cacheStats.misses;
        // Build minimal context for ID-to-string conversion
        ConstructQueryExportContext context{
            rowIdx,       idTable,         localVocab, variableColumns_.get(),
            index_.get(), currentRowOffset};
        auto value = ConstructQueryEvaluator::evaluateWithColumnIndex(
            varInfo.columnIndex, context);
        idCache[id] = std::move(value);
      }
    }
  }

  // Initialize blank node values: [blankNodeIdx][rowInBatch]
  batchCache.blankNodeValues.resize(blankNodesToEvaluate_.size());
  for (auto& column : batchCache.blankNodeValues) {
    column.resize(numRows);
  }

  // Evaluate blank nodes using precomputed prefix and suffix.
  // Only the row number needs to be concatenated per row.
  // Format: prefix + (currentRowOffset + rowIdx) + suffix
  for (size_t blankIdx = 0; blankIdx < blankNodesToEvaluate_.size();
       ++blankIdx) {
    const BlankNodeFormatInfo& formatInfo = blankNodesToEvaluate_[blankIdx];
    auto& columnValues = batchCache.blankNodeValues[blankIdx];

    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      // Use precomputed prefix and suffix, only concatenate row number
      columnValues[rowInBatch] = absl::StrCat(
          formatInfo.prefix, currentRowOffset + rowIdx, formatInfo.suffix);
    }
  }

  return batchCache;
}

// _____________________________________________________________________________
StringTriple ConstructTripleGenerator::instantiateTripleFromBatch(
    size_t tripleIdx, const BatchEvaluationCache& batchCache, size_t rowInBatch,
    const std::vector<const std::string*>& variableStrings) const {
  const TriplePatternInfo& info = triplePatternInfos_[tripleIdx];

  // Helper lambda to get a pointer to the string value for a position.
  // Returns nullptr if the value is UNDEF.
  // Variable strings are pre-looked-up in variableStrings cache.
  auto getStringPtr = [&](size_t pos) -> const std::string* {
    const TermResolution& resolution = info.resolutions[pos];

    switch (resolution.source) {
      case TermSource::CONSTANT: {
        const auto& opt = precomputedConstants_[tripleIdx][pos];
        return opt.has_value() ? &opt.value() : nullptr;
      }
      case TermSource::VARIABLE: {
        // Use pre-computed variable string pointer (already looked up from
        // idCache once per variable per row)
        return variableStrings[resolution.index];
      }
      case TermSource::BLANK_NODE: {
        const auto& opt =
            batchCache.getBlankNodeValue(resolution.index, rowInBatch);
        return opt.has_value() ? &opt.value() : nullptr;
      }
    }

    return nullptr;  // Unreachable
  };

  // Get pointers to all components, returning early if any is UNDEF
  const std::string* subject = getStringPtr(0);
  if (!subject) return StringTriple{};

  const std::string* predicate = getStringPtr(1);
  if (!predicate) return StringTriple{};

  const std::string* object = getStringPtr(2);
  if (!object) return StringTriple{};

  return StringTriple{*subject, *predicate, *object};
}

// _____________________________________________________________________________
StringTriple ConstructTripleGenerator::instantiateTriple(
    size_t tripleIdx, const RowEvaluationCache& cache) const {
  const TriplePatternInfo& info = triplePatternInfos_[tripleIdx];

  // Helper lambda to get a pointer to the string value for a position.
  // Returns nullptr if the value is UNDEF.
  auto getStringPtr = [&](size_t pos) -> const std::string* {
    const TermResolution& resolution = info.resolutions[pos];
    const std::optional<std::string>* optPtr = nullptr;

    switch (resolution.source) {
      case TermSource::CONSTANT:
        optPtr = &precomputedConstants_[tripleIdx][pos];
        break;
      case TermSource::VARIABLE:
        optPtr = &cache.variableValues[resolution.index];
        break;
      case TermSource::BLANK_NODE:
        optPtr = &cache.blankNodeValues[resolution.index];
        break;
    }

    return optPtr->has_value() ? &optPtr->value() : nullptr;
  };

  // Get pointers to all components, returning early if any is UNDEF
  const std::string* subject = getStringPtr(0);
  if (!subject) return StringTriple{};

  const std::string* predicate = getStringPtr(1);
  if (!predicate) return StringTriple{};

  const std::string* object = getStringPtr(2);
  if (!object) return StringTriple{};

  return StringTriple{*subject, *predicate, *object};
}

// _____________________________________________________________________________
auto ConstructTripleGenerator::generateStringTriplesForResultTable(
    const TableWithRange& table) {
  const auto tableWithVocab = table.tableWithVocab_;
  size_t currentRowOffset = rowOffset_;
  size_t numRows = tableWithVocab.idTable().numRows();
  rowOffset_ += numRows;

  // Create a shared ID cache for this table to avoid redundant ID-to-string
  // conversions when the same ID appears in multiple rows.
  // Using shared_ptr allows the cache to be captured by the lambda and shared
  // across all batch processing calls.
  auto idCache = std::make_shared<IdCache>();

  // Track cache statistics for performance analysis.
  // The shared_ptr ensures stats survive until all rows are processed.
  // We use a custom destructor to log the stats when processing completes.
  // Only log for batches with significant activity (>10000 lookups).
  auto cacheStats = std::shared_ptr<IdCacheStats>(
      new IdCacheStats(), [numRows](IdCacheStats* stats) {
        if (stats->totalLookups() > 10000) {
          AD_LOG_INFO << "ID Cache Stats - Rows: " << numRows
                      << ", Lookups: " << stats->totalLookups()
                      << ", Hits: " << stats->hits
                      << ", Misses: " << stats->misses
                      << ", Hit Rate: " << (stats->hitRate() * 100.0) << "%"
                      << ", Unique IDs: " << stats->misses << "\n";
        }
        delete stats;
      });

  // Collect row indices into a vector for batching
  // This allows us to process rows in batches with column-oriented access
  auto rowIndicesVec = std::make_shared<std::vector<uint64_t>>(
      ql::ranges::begin(table.view_), ql::ranges::end(table.view_));

  // Calculate number of batches
  size_t totalRows = rowIndicesVec->size();
  size_t numBatches = (totalRows + getBatchSize() - 1) / getBatchSize();

  // Process batches lazily using column-oriented evaluation.
  // Each batch materializes its triples into a vector to reduce view nesting
  // overhead. This trades a small bounded memory increase per batch for
  // significantly reduced iterator indirection and better cache locality.
  auto processBatch = [this, tableWithVocab, currentRowOffset, idCache,
                       cacheStats, rowIndicesVec,
                       totalRows](size_t batchIdx) mutable {
    cancellationHandle_->throwIfCancelled();

    // Calculate batch bounds
    size_t batchStart = batchIdx * getBatchSize();
    size_t batchEnd = std::min(batchStart + getBatchSize(), totalRows);

    // Use span for zero-copy access to row indices for this batch
    ql::span<const uint64_t> batchRowIndices(rowIndicesVec->data() + batchStart,
                                             batchEnd - batchStart);

    // Evaluate all variables and blank nodes for this batch using
    // column-oriented access for better cache locality
    BatchEvaluationCache batchCache = evaluateBatchColumnOriented(
        tableWithVocab.idTable(), tableWithVocab.localVocab(), batchRowIndices,
        currentRowOffset, *idCache, *cacheStats);

    // Materialize triples for this batch into a vector.
    // This reduces view nesting from 4 levels to 2 levels, eliminating
    // significant iterator overhead for each triple access.
    std::vector<StringTriple> batchTriples;
    batchTriples.reserve(batchCache.numRows * templateTriples_.size());

    // Pre-allocate cache for variable string pointers (reused across rows)
    std::vector<const std::string*> variableStrings(
        variablesToEvaluate_.size());

    for (size_t rowInBatch = 0; rowInBatch < batchCache.numRows; ++rowInBatch) {
      // Pre-compute string pointers for all variables in this row.
      // This avoids repeated idCache lookups when the same variable
      // appears multiple times in the template.
      for (size_t varIdx = 0; varIdx < variablesToEvaluate_.size(); ++varIdx) {
        const auto& optId = batchCache.getVariableId(varIdx, rowInBatch);
        if (!optId.has_value()) {
          variableStrings[varIdx] = nullptr;  // Variable not in result
          continue;
        }
        auto it = idCache->find(optId.value());
        if (it == idCache->end() || !it->second.has_value()) {
          variableStrings[varIdx] = nullptr;  // UNDEF value
        } else {
          variableStrings[varIdx] = &it->second.value();
        }
      }

      // Now instantiate all triples using the cached variable strings
      for (size_t tripleIdx = 0; tripleIdx < templateTriples_.size();
           ++tripleIdx) {
        auto triple = instantiateTripleFromBatch(tripleIdx, batchCache,
                                                 rowInBatch, variableStrings);
        if (!triple.isEmpty()) {
          batchTriples.push_back(std::move(triple));
        }
      }
    }

    return batchTriples;
  };

  // Process all batches lazily and flatten the results.
  // Now only 2 levels of view nesting instead of 4.
  return ql::views::iota(size_t{0}, numBatches) |
         ql::views::transform(processBatch) | ql::views::join;
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<QueryExecutionTree::StringTriple>
ConstructTripleGenerator::generateStringTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  // The `resultSizeMultiplicator`(last argument of `getRowIndices`) is
  // explained by the following: For each result from the WHERE clause, we
  // produce up to `constructTriples.size()` triples. We do not account for
  // triples that are filtered out because one of the components is UNDEF (it
  // would require materializing the whole result)
  auto rowIndices = ExportQueryExecutionTrees::getRowIndices(
      limitAndOffset, *result, resultSize, constructTriples.size());

  ConstructTripleGenerator generator(
      constructTriples, std::move(result), qet.getVariableColumns(),
      qet.getQec()->getIndex(), std::move(cancellationHandle));

  // Transform the range of tables into a flattened range of triples.
  // We move the generator into the transformation lambda to extend its
  // lifetime. Because the transformation is stateful (it tracks rowOffset_),
  // the lambda must be marked 'mutable'.
  auto tableTriples = ql::views::transform(
      ad_utility::OwningView{std::move(rowIndices)},
      [generator = std::move(generator)](const TableWithRange& table) mutable {
        // The generator now handles the:
        // Table -> Rows -> Triple Patterns -> StringTriples
        return generator.generateStringTriplesForResultTable(table);
      });
  return InputRangeTypeErased(ql::views::join(std::move(tableTriples)));
}
