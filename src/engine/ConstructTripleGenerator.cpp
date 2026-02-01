// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include <absl/strings/str_cat.h>

#include <cstdlib>
#include <iomanip>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Log.h"

using ad_utility::InputRangeTypeErased;
using enum PositionInTriple;
using StringTriple = ConstructTripleGenerator::StringTriple;

// ============================================================================
// Configuration
// ============================================================================

// _____________________________________________________________________________
size_t ConstructTripleGenerator::getBatchSize() {
  static const size_t batchSize = []() {
    const char* envVal = std::getenv("QLEVER_CONSTRUCT_BATCH_SIZE");
    if (envVal != nullptr) {
      try {
        size_t val = std::stoull(envVal);
        if (val > 0) {
          AD_LOG_INFO << "CONSTRUCT batch size from environment: " << val
                      << "\n";
          return val;
        }
        AD_LOG_WARN << "QLEVER_CONSTRUCT_BATCH_SIZE must be > 0, got: "
                    << envVal << ", using default: " << DEFAULT_BATCH_SIZE
                    << "\n";
      } catch (const std::exception& e) {
        AD_LOG_WARN << "Invalid QLEVER_CONSTRUCT_BATCH_SIZE value: " << envVal
                    << " (" << e.what()
                    << "), using default: " << DEFAULT_BATCH_SIZE << "\n";
      }
    } else {
      AD_LOG_INFO << "CONSTRUCT batch size: " << DEFAULT_BATCH_SIZE
                  << " (default)\n";
    }
    return DEFAULT_BATCH_SIZE;
  }();
  return batchSize;
}

// ============================================================================
// Template Analysis (Precomputation Phase)
// ============================================================================
// Called once at construction to analyze the CONSTRUCT triple patterns.
// For each pattern, we determine how each term (subject, predicate, object)
// will be resolved:
//   - CONSTANT: IRIs and Literals are evaluated once and stored
//   - VARIABLE: Column index is pre-computed for O(1) access per row
//   - BLANK_NODE: Format prefix/suffix are pre-built (row number varies)
//
// This analysis enables fast per-row instantiation without repeated parsing
// or hash map lookups in the hot path.
// ============================================================================

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

// ============================================================================
// Batch Evaluation (Column-Oriented Processing)
// ============================================================================
// Evaluates Variables and BlankNodes for a batch of rows using column-oriented
// access for improved CPU cache locality:
//
// Column-oriented access pattern:
//   for each variable V:
//     for each row R in batch:
//       read idTable[R][column(V)]    <-- Sequential reads within a column
//
// This is more cache-friendly than row-oriented access because:
//   - CPU prefetchers work better with sequential memory access
//   - Each column's data is contiguous in the IdTable
//   - We process one cache line's worth of IDs before moving to the next
//
// The batch size (default 64) is tuned so the working set (IDs + cached
// strings for batch rows) fits in L2 cache, avoiding cache thrashing.
//
// Note: BlankNodes are evaluated row-by-row because their values include
// the row number and cannot be cached across rows.
// ============================================================================

// _____________________________________________________________________________
ConstructTripleGenerator::BatchEvaluationCache
ConstructTripleGenerator::evaluateBatchColumnOriented(
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    IdCache& idCache, IdCacheStatsLogger& statsLogger) const {
  BatchEvaluationCache batchCache;
  const size_t numRows = rowIndices.size();
  batchCache.numRows = numRows;

  // Get reference to stats for tracking hits/misses
  auto& cacheStats = statsLogger.stats();

  // Initialize variable string pointers: [varIdx][rowInBatch]
  // We store pointers directly into idCache to avoid a second hash lookup
  // during instantiation. Pointers are stable within a batch since we don't
  // modify idCache between evaluation and instantiation (no rehashing).
  batchCache.variableStringPtrs.resize(variablesToEvaluate_.size());
  for (auto& column : batchCache.variableStringPtrs) {
    column.resize(numRows, nullptr);
  }

  // Evaluate variables column-by-column for better cache locality
  // The IdTable is accessed sequentially for each column
  for (size_t varIdx = 0; varIdx < variablesToEvaluate_.size(); ++varIdx) {
    const auto& varInfo = variablesToEvaluate_[varIdx];
    auto& columnPtrs = batchCache.variableStringPtrs[varIdx];

    if (!varInfo.columnIndex.has_value()) {
      // Variable not in result - all pointers are nullptr (already default)
      continue;
    }

    const size_t colIdx = varInfo.columnIndex.value();

    // Read all IDs from this column for all rows in the batch,
    // ensure their string values are in the cache, and store pointers directly
    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      Id id = idTable(rowIdx, colIdx);

      // Use LRU cache's getOrCompute: returns cached value or computes and
      // caches it. The compute lambda is only called on cache miss.
      // Pointers are stable within a batch because:
      // 1. LRU only evicts entries not accessed in the current batch
      // 2. Cache capacity >= batch_size * num_variables (ensured in creation)
      size_t missesBefore = cacheStats.misses;
      const std::string& cachedValue = idCache.getOrCompute(id, [&](const Id&) {
        ++cacheStats.misses;
        // Build minimal context for ID-to-string conversion
        ConstructQueryExportContext context{
            rowIdx,       idTable,         localVocab, variableColumns_.get(),
            index_.get(), currentRowOffset};
        auto value = ConstructQueryEvaluator::evaluateWithColumnIndex(
            varInfo.columnIndex, context);
        // Use empty string as sentinel for UNDEF values
        return value.value_or(std::string{});
      });

      // Track hits (compute lambda not called means it was a cache hit)
      if (cacheStats.misses == missesBefore) {
        ++cacheStats.hits;
      }

      // Store pointer directly (empty string means UNDEF)
      columnPtrs[rowInBatch] = cachedValue.empty() ? nullptr : &cachedValue;
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

// ============================================================================
// Triple Instantiation
// ============================================================================
// Converts precomputed term values into concrete StringTriples or formatted
// strings. Each term position (subject, predicate, object) is resolved based
// on its TermSource:
//
//   CONSTANT    -> Use precomputed string (evaluated once at construction)
//   VARIABLE    -> Lookup in variableStrings (pointer into IdCache)
//   BLANK_NODE  -> Use batch cache (row-specific, includes row number)
//
// Key optimization: Variable strings are looked up once per variable per row
// (via lookupVariableStrings) and reused across all triple patterns that
// reference the same variable in that row.
//
// If any term is UNDEF (nullopt/nullptr), the entire triple is skipped.
// This implements SPARQL CONSTRUCT semantics where incomplete triples are
// not included in the output.
// ============================================================================

// _____________________________________________________________________________
StringTriple ConstructTripleGenerator::instantiateTripleFromBatch(
    size_t tripleIdx, const BatchEvaluationCache& batchCache, size_t rowInBatch,
    const std::vector<const std::string*>& variableStrings) const {
  // Get pointers to all components, returning early if any is UNDEF
  const std::string* subject =
      getTermStringPtr(tripleIdx, 0, batchCache, rowInBatch, variableStrings);
  if (!subject) return StringTriple{};

  const std::string* predicate =
      getTermStringPtr(tripleIdx, 1, batchCache, rowInBatch, variableStrings);
  if (!predicate) return StringTriple{};

  const std::string* object =
      getTermStringPtr(tripleIdx, 2, batchCache, rowInBatch, variableStrings);
  if (!object) return StringTriple{};

  return StringTriple{*subject, *predicate, *object};
}

// _____________________________________________________________________________
auto ConstructTripleGenerator::generateStringTriplesForResultTable(
    const TableWithRange& table) {
  const auto tableWithVocab = table.tableWithVocab_;
  const size_t currentRowOffset = rowOffset_;
  const size_t numRows = tableWithVocab.idTable().numRows();
  rowOffset_ += numRows;

  auto [idCache, statsLogger] = createIdCacheWithStats(numRows);

  auto rowIndicesVec = std::make_shared<std::vector<uint64_t>>(
      ql::ranges::begin(table.view_), ql::ranges::end(table.view_));

  const size_t totalRows = rowIndicesVec->size();
  const size_t numBatches = (totalRows + getBatchSize() - 1) / getBatchSize();

  // Process batches lazily. Each batch materializes triples into a vector
  // to reduce view nesting overhead.
  auto processBatch = [this, tableWithVocab, currentRowOffset, idCache,
                       statsLogger, rowIndicesVec,
                       totalRows](size_t batchIdx) mutable {
    cancellationHandle_->throwIfCancelled();

    const size_t batchStart = batchIdx * getBatchSize();
    const size_t batchEnd = std::min(batchStart + getBatchSize(), totalRows);
    ql::span<const uint64_t> batchRowIndices(rowIndicesVec->data() + batchStart,
                                             batchEnd - batchStart);

    BatchEvaluationCache batchCache = evaluateBatchColumnOriented(
        tableWithVocab.idTable(), tableWithVocab.localVocab(), batchRowIndices,
        currentRowOffset, *idCache, *statsLogger);

    std::vector<StringTriple> batchTriples;
    batchTriples.reserve(batchCache.numRows * templateTriples_.size());

    std::vector<const std::string*> variableStrings(
        variablesToEvaluate_.size());

    for (size_t rowInBatch = 0; rowInBatch < batchCache.numRows; ++rowInBatch) {
      lookupVariableStrings(batchCache, rowInBatch, *idCache, variableStrings);

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

  return ql::views::iota(size_t{0}, numBatches) |
         ql::views::transform(processBatch) | ql::views::join;
}

// _____________________________________________________________________________
const std::string* ConstructTripleGenerator::getTermStringPtr(
    size_t tripleIdx, size_t pos, const BatchEvaluationCache& batchCache,
    size_t rowInBatch,
    const std::vector<const std::string*>& variableStrings) const {
  const TriplePatternInfo& info = triplePatternInfos_[tripleIdx];
  const TermResolution& resolution = info.resolutions[pos];

  switch (resolution.source) {
    case TermSource::CONSTANT: {
      const auto& opt = precomputedConstants_[tripleIdx][pos];
      return opt.has_value() ? &opt.value() : nullptr;
    }
    case TermSource::VARIABLE: {
      // Variable string pointers are already stored directly in the batch
      // cache, eliminating hash lookups during instantiation
      return variableStrings[resolution.index];
    }
    case TermSource::BLANK_NODE: {
      // Blank node values are always valid (computed for each row)
      return &batchCache.getBlankNodeValue(resolution.index, rowInBatch);
    }
  }

  return nullptr;  // Unreachable
}

// ============================================================================
// ID Cache Helpers
// ============================================================================
// The ID cache maps Id values to their string representations, avoiding
// redundant vocabulary lookups when the same entity appears multiple times
// in the result set. High cache hit rates are common for queries with
// repeated values (e.g., same predicate or shared subjects).
//
// Statistics logging helps identify queries where caching is effective
// (high hit rate) vs. queries with mostly unique values (low hit rate).
// ============================================================================

// _____________________________________________________________________________
ConstructTripleGenerator::IdCacheStatsLogger::~IdCacheStatsLogger() {
  // Only log if there were a meaningful number of lookups
  if (stats_.totalLookups() > 1000) {
    AD_LOG_INFO << "CONSTRUCT IdCache stats - Rows: " << numRows_
                << ", Capacity: " << cacheCapacity_
                << ", Lookups: " << stats_.totalLookups()
                << ", Hits: " << stats_.hits << ", Misses: " << stats_.misses
                << ", Hit rate: " << std::fixed << std::setprecision(1)
                << (stats_.hitRate() * 100.0) << "%" << std::endl;
  }
}

// _____________________________________________________________________________
std::pair<std::shared_ptr<ConstructTripleGenerator::IdCache>,
          std::shared_ptr<ConstructTripleGenerator::IdCacheStatsLogger>>
ConstructTripleGenerator::createIdCacheWithStats(size_t numRows) const {
  // Ensure cache capacity is large enough to hold the working set of a single
  // batch (batch_size * num_variables) to guarantee pointer stability within
  // a batch. Add headroom for cross-batch cache hits on repeated values.
  const size_t numVars = variablesToEvaluate_.size();
  const size_t minCapacityForBatch =
      getBatchSize() * std::max(numVars, size_t{1}) * 2;
  const size_t capacity = std::max(MIN_CACHE_CAPACITY, minCapacityForBatch);
  auto idCache = std::make_shared<IdCache>(capacity);
  auto statsLogger = std::make_shared<IdCacheStatsLogger>(numRows, capacity);
  return {std::move(idCache), std::move(statsLogger)};
}

// _____________________________________________________________________________
void ConstructTripleGenerator::lookupVariableStrings(
    const BatchEvaluationCache& batchCache, size_t rowInBatch,
    [[maybe_unused]] const IdCache& idCache,
    std::vector<const std::string*>& variableStrings) const {
  // String pointers are already stored in the batch cache during evaluation,
  // eliminating the need for hash lookups here. Just copy the pointers.
  for (size_t varIdx = 0; varIdx < variablesToEvaluate_.size(); ++varIdx) {
    variableStrings[varIdx] = batchCache.getVariableString(varIdx, rowInBatch);
  }
}

// ============================================================================
// Output Formatting
// ============================================================================
// Formats triples for different output formats without intermediate
// StringTriple allocations. This is the key performance optimization:
//
// Old approach (StringTriple-based):
//   Row -> StringTriple{s,p,o} (allocation) -> formatForOutput (copy)
//
// New approach (direct formatting):
//   Row -> formatTriple(s*,p*,o*) -> yield string (single allocation)
//
// Format-specific handling:
//   TURTLE: Subject Predicate Object .\n (with literal escaping)
//   CSV:    "s","p","o"\n (RFC 4180 escaping)
//   TSV:    s\tp\to\n (minimal escaping)
//
// The stream_generator (co_yield) coalesces small strings into ~1MB buffers
// for efficient streaming to the HTTP response.
// ============================================================================

// _____________________________________________________________________________
std::string ConstructTripleGenerator::formatTriple(
    const std::string* subject, const std::string* predicate,
    const std::string* object, ConstructOutputFormat format) const {
  if (!subject || !predicate || !object) {
    return {};
  }

  switch (format) {
    case ConstructOutputFormat::TURTLE: {
      // Only escape literals (strings starting with "). IRIs and blank nodes
      // are used as-is, avoiding an unnecessary string copy.
      if (ql::starts_with(*object, "\"")) {
        return absl::StrCat(*subject, " ", *predicate, " ",
                            RdfEscaping::validRDFLiteralFromNormalized(*object),
                            " .\n");
      }
      return absl::StrCat(*subject, " ", *predicate, " ", *object, " .\n");
    }
    case ConstructOutputFormat::CSV: {
      return absl::StrCat(RdfEscaping::escapeForCsv(*subject), ",",
                          RdfEscaping::escapeForCsv(*predicate), ",",
                          RdfEscaping::escapeForCsv(*object), "\n");
    }
    case ConstructOutputFormat::TSV: {
      return absl::StrCat(RdfEscaping::escapeForTsv(*subject), "\t",
                          RdfEscaping::escapeForTsv(*predicate), "\t",
                          RdfEscaping::escapeForTsv(*object), "\n");
    }
  }
  return {};  // Unreachable
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    const TableWithRange& table, ConstructOutputFormat format) {
  // Capture state needed for iteration
  struct FormattedTripleRange
      : public ad_utility::InputRangeFromGet<std::string> {
    // References to generator state
    const ConstructTripleGenerator& generator_;
    ConstructOutputFormat format_;

    // Table data
    TableConstRefWithVocab tableWithVocab_;
    std::vector<uint64_t> rowIndicesVec_;
    size_t currentRowOffset_;

    // ID cache for avoiding redundant lookups into `IdTable`.
    std::shared_ptr<IdCache> idCache_;
    std::shared_ptr<IdCacheStatsLogger> statsLogger_;

    // Iteration state
    size_t batchSize_;
    size_t batchStart_ = 0;
    size_t rowInBatch_ = 0;
    size_t tripleIdx_ = 0;
    std::optional<BatchEvaluationCache> batchCache_;
    std::vector<const std::string*> variableStrings_;

    FormattedTripleRange(const ConstructTripleGenerator& generator,
                         const TableWithRange& table,
                         ConstructOutputFormat format, size_t currentRowOffset)
        : generator_(generator),
          format_(format),
          tableWithVocab_(table.tableWithVocab_),
          rowIndicesVec_(ql::ranges::begin(table.view_),
                         ql::ranges::end(table.view_)),
          currentRowOffset_(currentRowOffset),
          batchSize_(ConstructTripleGenerator::getBatchSize()),
          variableStrings_(generator.variablesToEvaluate_.size()) {
      auto [cache, logger] =
          generator_.createIdCacheWithStats(rowIndicesVec_.size());
      idCache_ = std::move(cache);
      statsLogger_ = std::move(logger);
    }

    std::optional<std::string> get() override {
      // Find the next non-empty formatted triple
      while (batchStart_ < rowIndicesVec_.size()) {
        generator_.cancellationHandle_->throwIfCancelled();

        // Load new batch if needed
        if (!batchCache_.has_value()) {
          const size_t batchEnd =
              std::min(batchStart_ + batchSize_, rowIndicesVec_.size());
          ql::span<const uint64_t> batchRowIndices(
              rowIndicesVec_.data() + batchStart_, batchEnd - batchStart_);

          batchCache_ = generator_.evaluateBatchColumnOriented(
              tableWithVocab_.idTable(), tableWithVocab_.localVocab(),
              batchRowIndices, currentRowOffset_, *idCache_, *statsLogger_);
          rowInBatch_ = 0;
          tripleIdx_ = 0;
        }

        // Process current batch
        while (rowInBatch_ < batchCache_->numRows) {
          // Lookup variable strings for current row (once per row)
          if (tripleIdx_ == 0) {
            generator_.lookupVariableStrings(*batchCache_, rowInBatch_,
                                             *idCache_, variableStrings_);
          }

          // Process triples for current row
          while (tripleIdx_ < generator_.templateTriples_.size()) {
            const std::string* subject = generator_.getTermStringPtr(
                tripleIdx_, 0, *batchCache_, rowInBatch_, variableStrings_);
            const std::string* predicate = generator_.getTermStringPtr(
                tripleIdx_, 1, *batchCache_, rowInBatch_, variableStrings_);
            const std::string* object = generator_.getTermStringPtr(
                tripleIdx_, 2, *batchCache_, rowInBatch_, variableStrings_);

            ++tripleIdx_;

            std::string formatted =
                generator_.formatTriple(subject, predicate, object, format_);
            if (!formatted.empty()) {
              return formatted;
            }
          }

          // Move to next row
          ++rowInBatch_;
          tripleIdx_ = 0;
        }

        // Move to next batch
        batchStart_ += batchSize_;
        batchCache_.reset();
      }

      return std::nullopt;  // Done
    }
  };

  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  return ad_utility::InputRangeTypeErased<std::string>{
      std::make_unique<FormattedTripleRange>(*this, table, format,
                                             currentRowOffset)};
}

// ============================================================================
// Public Interface
// ============================================================================
// Entry points for generating CONSTRUCT query output:
//
// generateStringTriples (static):
//   - Returns a lazy range of StringTriple objects
//   - Used when caller needs structured access to triple components
//   - Triples are generated on-demand as the range is consumed
//
// generateFormattedTriples (instance method):
//   - Returns a stream_generator yielding pre-formatted strings
//   - More efficient for streaming output (avoids StringTriple allocation)
//   - Called via ExportQueryExecutionTrees for HTTP responses
//
// Both methods:
//   - Process result tables lazily (streaming, not materializing)
//   - Maintain rowOffset_ state for correct blank node numbering
//   - Support cancellation via cancellationHandle_
// ============================================================================

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
        // conceptually, the generator now handles the following pipeline:
        // table -> rows -> triple patterns -> string triples
        return generator.generateStringTriplesForResultTable(table);
      });
  return InputRangeTypeErased(ql::views::join(std::move(tableTriples)));
}
