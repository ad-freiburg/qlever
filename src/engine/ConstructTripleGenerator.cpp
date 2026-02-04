// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructBatchProcessor.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/InstantiationBlueprint.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "rdfTypes/RdfEscaping.h"

using ad_utility::InputRangeTypeErased;
using enum PositionInTriple;
using StringTriple = ConstructTripleGenerator::StringTriple;

// Called once at construction to preprocess the CONSTRUCT triple patterns.
// For each term, we precompute the following, based on the `TermType`:
// - CONSTANT: `Iri`s and `Literal`s are evaluated once and stored
// - VARIABLE: Column index of variable in `IdTable` is precomputed.
// - BLANK_NODE: Format prefix/suffix are pre-built (row number varies)
void ConstructTripleGenerator::preprocessTemplate() {
  // Create the context that will be shared with ConstructBatchProcessor
  // instances.
  blueprint_ = std::make_shared<InstantiationBlueprint>(
      index_.get(), variableColumns_.get(), cancellationHandle_);

  // Resize context arrays for template analysis.
  blueprint_->precomputedConstants_.resize(templateTriples_.size());
  blueprint_->triplePatternInfos_.resize(templateTriples_.size());

  for (size_t tripleIdx = 0; tripleIdx < templateTriples_.size(); ++tripleIdx) {
    const auto& triple = templateTriples_[tripleIdx];
    TriplePatternInfo& info = blueprint_->triplePatternInfos_[tripleIdx];

    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      auto role = static_cast<PositionInTriple>(pos);
      info.lookups_[pos] = analyzeTerm(triple[pos], tripleIdx, pos, role);
    }
  }
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeTerm(
    const GraphTerm& term, size_t tripleIdx, size_t pos,
    PositionInTriple role) {
  if (std::holds_alternative<Iri>(term)) {
    return analyzeIriTerm(std::get<Iri>(term), tripleIdx, pos);
  } else if (std::holds_alternative<Literal>(term)) {
    return analyzeLiteralTerm(std::get<Literal>(term), tripleIdx, pos, role);
  } else if (std::holds_alternative<Variable>(term)) {
    return analyzeVariableTerm(std::get<Variable>(term));
  } else if (std::holds_alternative<BlankNode>(term)) {
    return analyzeBlankNodeTerm(std::get<BlankNode>(term));
  }
  // Unreachable for valid GraphTerm
  // TODO<ms2144> add compile time error throw here.
  return {TriplePatternInfo::TermType::CONSTANT, 0};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeIriTerm(
    const Iri& iri, size_t tripleIdx, size_t pos) {
  // evaluate(Iri) always returns a valid string
  blueprint_->precomputedConstants_[tripleIdx][pos] =
      ConstructQueryEvaluator::evaluate(iri);
  return {TriplePatternInfo::TermType::CONSTANT, tripleIdx};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeLiteralTerm(
    const Literal& literal, size_t tripleIdx, size_t pos,
    PositionInTriple role) {
  // evaluate(Literal) returns optional - only store if valid
  auto value = ConstructQueryEvaluator::evaluate(literal, role);
  if (value.has_value()) {
    blueprint_->precomputedConstants_[tripleIdx][pos] = std::move(*value);
  }
  return {TriplePatternInfo::TermType::CONSTANT, tripleIdx};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeVariableTerm(
    const Variable& var) {
  if (!variableToIndex_.contains(var)) {
    size_t idx = blueprint_->variablesToEvaluate_.size();
    variableToIndex_[var] = idx;
    // Pre-compute the column index corresponding to the variable in the
    // `IdTable`.
    std::optional<size_t> columnIndex;
    if (variableColumns_.get().contains(var)) {
      columnIndex = variableColumns_.get().at(var).columnIndex_;
    }
    blueprint_->variablesToEvaluate_.emplace_back(
        VariableWithColumnIndex{var, columnIndex});
  }
  return {TriplePatternInfo::TermType::VARIABLE, variableToIndex_[var]};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo
ConstructTripleGenerator::analyzeBlankNodeTerm(const BlankNode& blankNode) {
  const std::string& label = blankNode.label();
  if (!blankNodeLabelToIndex_.contains(label)) {
    size_t idx = blueprint_->blankNodesToEvaluate_.size();
    blankNodeLabelToIndex_[label] = idx;
    // Precompute prefix ("_:g" or "_:u") and suffix ("_" + label)
    // so we only need to concatenate the row number per row
    BlankNodeFormatInfo formatInfo;
    formatInfo.prefix_ = blankNode.isGenerated() ? "_:g" : "_:u";
    formatInfo.suffix_ = absl::StrCat("_", label);
    blueprint_->blankNodesToEvaluate_.push_back(std::move(formatInfo));
  }
  return {TriplePatternInfo::TermType::BLANK_NODE,
          blankNodeLabelToIndex_[label]};
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
  preprocessTemplate();
}

// _____________________________________________________________________________
// Batch Evaluation (Column-Oriented Processing or multiple result-table rows)
//
// Evaluates Variables and BlankNodes for a batch of rows.
// Column-oriented access pattern for variables:
//   for each variable V occurring in the template triples:
//     for each row R in batch:
//       read idTable[column(V)][R]    <-- Sequential reads within a column
//
// This is more cache-friendly than row-oriented access, because the memory
// layout of `IdTable` is column-major.
//
// Note: BlankNodes are evaluated row-by-row because their values include
// the row number and cannot be cached across rows.
BatchEvaluationCache ConstructTripleGenerator::evaluateBatchColumnOriented(
    const IdTable& idTable, const LocalVocab& localVocab,
    ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
    IdCache& idCache, IdCacheStatsLogger& statsLogger) const {
  BatchEvaluationCache batchCache;
  batchCache.numRows_ = rowIndices.size();

  evaluateVariablesForBatch(batchCache, idTable, localVocab, rowIndices,
                            currentRowOffset, idCache, statsLogger);
  evaluateBlankNodesForBatch(batchCache, rowIndices, currentRowOffset);

  return batchCache;
}

// _____________________________________________________________________________
void ConstructTripleGenerator::evaluateVariablesForBatch(
    BatchEvaluationCache& batchCache, const IdTable& idTable,
    const LocalVocab& localVocab, ql::span<const uint64_t> rowIndices,
    size_t currentRowOffset, IdCache& idCache,
    IdCacheStatsLogger& statsLogger) const {
  const size_t numRows = rowIndices.size();
  auto& cacheStats = statsLogger.stats();
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
      // Variable not in result - all values are nullptr (already default)
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
      const VariableToColumnMap& varCols = variableColumns_.get();
      const Index& idx = index_.get();
      const std::shared_ptr<const std::string>& cachedValue =
          idCache.getOrCompute(id, [&cacheStats, &varCols, &idx, &colIdx,
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

      // Share ownership with IdCache (no string copy)
      columnStrings[rowInBatch] = cachedValue;
    }
  }
}

// _____________________________________________________________________________
void ConstructTripleGenerator::evaluateBlankNodesForBatch(
    BatchEvaluationCache& batchCache, ql::span<const uint64_t> rowIndices,
    size_t currentRowOffset) const {
  const size_t numRows = rowIndices.size();
  const auto& blankNodesToEvaluate = blueprint_->blankNodesToEvaluate_;

  // Initialize blank node values: [blankNodeIdx][rowInBatch]
  batchCache.blankNodeValues_.resize(blankNodesToEvaluate.size());
  for (auto& column : batchCache.blankNodeValues_) {
    column.resize(numRows);
  }

  // Evaluate blank nodes using precomputed prefix and suffix_.
  // Only the row number needs to be concatenated per row.
  // Format: prefix + (currentRowOffset + rowIdx) + suffix_
  for (size_t blankIdx = 0; blankIdx < blankNodesToEvaluate.size();
       ++blankIdx) {
    const BlankNodeFormatInfo& formatInfo = blankNodesToEvaluate[blankIdx];
    auto& columnValues = batchCache.blankNodeValues_[blankIdx];

    for (size_t rowInBatch = 0; rowInBatch < numRows; ++rowInBatch) {
      const uint64_t rowIdx = rowIndices[rowInBatch];
      // Use precomputed prefix and suffix_, only concatenate row number
      columnValues[rowInBatch] = absl::StrCat(
          formatInfo.prefix_, currentRowOffset + rowIdx, formatInfo.suffix_);
    }
  }
}

// _____________________________________________________________________________
// Converts precomputed term values into concrete `StringTriples` or formatted
// strings. Each term position (subject, predicate, object) is resolved based
// on its TermType:
//   CONSTANT    -> Use precomputed string (evaluated once at construction)
//   VARIABLE    -> Lookup in variableStrings (pointer into IdCache)
//   BLANK_NODE  -> Use batch cache (row-specific, includes row number)
//
// variable strings are looked up once per variable per result-table row
// (via `lookupVariableStrings`) and reused across all triple patterns that
// reference the same variable in that row.
// If any term is UNDEF (nullopt/nullptr), the entire triple is skipped.
// This implements SPARQL CONSTRUCT semantics where incomplete triples are
// not included in the output.
// _____________________________________________________________________________
StringTriple ConstructTripleGenerator::instantiateTripleFromBatch(
    size_t tripleIdx, const BatchEvaluationCache& batchCache, size_t rowInBatch,
    const std::vector<std::shared_ptr<const std::string>>& variableStrings)
    const {
  // Get shared_ptr to all components, returning early if any is UNDEF
  auto subject =
      getTermStringPtr(tripleIdx, 0, batchCache, rowInBatch, variableStrings);
  if (!subject) return StringTriple{};

  auto predicate =
      getTermStringPtr(tripleIdx, 1, batchCache, rowInBatch, variableStrings);
  if (!predicate) return StringTriple{};

  auto object =
      getTermStringPtr(tripleIdx, 2, batchCache, rowInBatch, variableStrings);
  if (!object) return StringTriple{};

  return StringTriple{*subject, *predicate, *object};
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriplesForResultTable(
    const TableWithRange& table) {
  const auto tableWithVocab = table.tableWithVocab_;
  const size_t currentRowOffset = rowOffset_;
  const size_t numRows = tableWithVocab.idTable().numRows();
  rowOffset_ += numRows;

  auto [idCache, statsLogger] = createIdCacheWithStats(numRows);

  auto rowIndicesVec = std::make_shared<std::vector<uint64_t>>(
      ql::ranges::begin(table.view_), ql::ranges::end(table.view_));

  const size_t totalRows = rowIndicesVec->size();
  const size_t batchSize = ConstructBatchProcessor::getBatchSize();
  const size_t numBatches = (totalRows + batchSize - 1) / batchSize;

  // Process batches lazily, calling processBatchForStringTriples for each.
  auto processBatch = [this, tableWithVocab, currentRowOffset, idCache,
                       statsLogger, rowIndicesVec, totalRows,
                       batchSize](size_t batchIdx) mutable {
    const size_t batchStart = batchIdx * batchSize;
    const size_t batchEnd = std::min(batchStart + batchSize, totalRows);
    ql::span<const uint64_t> batchRowIndices(rowIndicesVec->data() + batchStart,
                                             batchEnd - batchStart);
    return processBatchForStringTriples(tableWithVocab, currentRowOffset,
                                        *idCache, *statsLogger,
                                        batchRowIndices);
  };

  return InputRangeTypeErased(ql::views::iota(size_t{0}, numBatches) |
                              ql::views::transform(processBatch) |
                              ql::views::join);
}

// _____________________________________________________________________________
std::vector<StringTriple>
ConstructTripleGenerator::processBatchForStringTriples(
    const TableConstRefWithVocab& tableWithVocab, size_t currentRowOffset,
    IdCache& idCache, IdCacheStatsLogger& statsLogger,
    ql::span<const uint64_t> batchRowIndices) {
  cancellationHandle_->throwIfCancelled();

  BatchEvaluationCache batchCache = evaluateBatchColumnOriented(
      tableWithVocab.idTable(), tableWithVocab.localVocab(), batchRowIndices,
      currentRowOffset, idCache, statsLogger);

  std::vector<StringTriple> batchTriples;
  batchTriples.reserve(batchCache.numRows_ * templateTriples_.size());

  std::vector<std::shared_ptr<const std::string>> variableStrings(
      blueprint_->variablesToEvaluate_.size());

  for (size_t rowInBatch = 0; rowInBatch < batchCache.numRows_; ++rowInBatch) {
    lookupVariableStrings(batchCache, rowInBatch, variableStrings);

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
}

// _____________________________________________________________________________
std::shared_ptr<const std::string> ConstructTripleGenerator::getTermStringPtr(
    size_t tripleIdx, size_t pos, const BatchEvaluationCache& batchCache,
    size_t rowInBatch,
    const std::vector<std::shared_ptr<const std::string>>& variableStrings)
    const {
  const TriplePatternInfo& info = blueprint_->triplePatternInfos_[tripleIdx];
  const TriplePatternInfo::TermLookupInfo& lookup = info.lookups_[pos];

  switch (lookup.type) {
    case TriplePatternInfo::TermType::CONSTANT: {
      return std::make_shared<std::string>(
          blueprint_->precomputedConstants_[tripleIdx][pos]);
    }
    case TriplePatternInfo::TermType::VARIABLE: {
      // Variable shared_ptr are already stored directly in the batch
      // cache, eliminating hash lookups during instantiation
      return variableStrings[lookup.index];
    }
    case TriplePatternInfo::TermType::BLANK_NODE: {
      // Blank node values are always valid (computed for each row)
      return std::make_shared<const std::string>(
          batchCache.getBlankNodeValue(lookup.index, rowInBatch));
    }
  }
  // TODO<ms2144>: I do not think it is good to ever return a nullptr.
  // we should probably throw an exception here or sth.
  return nullptr;  // Unreachable
}

// _____________________________________________________________________________
std::pair<std::shared_ptr<ConstructTripleGenerator::IdCache>,
          std::shared_ptr<ConstructTripleGenerator::IdCacheStatsLogger>>
ConstructTripleGenerator::createIdCacheWithStats(size_t numRows) const {
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
void ConstructTripleGenerator::lookupVariableStrings(
    const BatchEvaluationCache& batchCache, size_t rowInBatch,
    std::vector<std::shared_ptr<const std::string>>& variableStrings) const {
  // Get shared_ptr from the batch cache. Ownership is shared with IdCache.
  for (size_t varIdx = 0; varIdx < blueprint_->variablesToEvaluate_.size();
       ++varIdx) {
    variableStrings[varIdx] = batchCache.getVariableString(varIdx, rowInBatch);
  }
}

// _____________________________________________________________________________
// Entry point for generating CONSTRUCT query output.
// Used when caller needs structured access to triple components.
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
        // table -> processing batch -> table rows -> triple patterns -> string
        // triples
        return generator.generateStringTriplesForResultTable(table);
      });
  return InputRangeTypeErased(ql::views::join(std::move(tableTriples)));
}

// _____________________________________________________________________________
// Entry point for generating CONSTRUCT query output.
// More efficient for streaming output than `generateStringTriples` (avoids
// `StringTriple` allocations).
ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    const TableWithRange& table, ad_utility::MediaType mediaType) {
  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  return ad_utility::InputRangeTypeErased<std::string>{
      std::make_unique<ConstructBatchProcessor>(blueprint_, table, mediaType,
                                                currentRowOffset)};
}
