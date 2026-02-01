// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <functional>

#include "backports/span.h"
#include "engine/ConstructQueryEvaluator.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "parser/data/BlankNode.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"
#include "util/HashMap.h"
#include "util/StableLruCache.h"
#include "util/stream_generator.h"

// Output format for CONSTRUCT query results.
enum class ConstructOutputFormat { TURTLE, CSV, TSV };

// ============================================================================
// ConstructTripleGenerator
// ============================================================================
//
// Generates triples from CONSTRUCT query results by instantiating triple
// patterns (from the CONSTRUCT clause) with values from the result table
// (produced by the WHERE clause).
//
// ARCHITECTURE OVERVIEW
// ---------------------
// The generator transforms: Result Table → Rows → Triple Patterns → Output
//
// For each row in the result table, we instantiate each triple pattern by
// substituting variables with their values from that row. The output is either
// StringTriple objects or pre-formatted strings (Turtle/CSV/TSV).
//
// PERFORMANCE OPTIMIZATIONS
// -------------------------
// 1. PRECOMPUTATION (analyzeTemplate):
//    - Constants (IRIs, Literals) are evaluated once at construction time
//    - Variable column indices are pre-computed to avoid hash lookups per row
//    - Blank node format strings are pre-built (only row number varies)
//
// 2. BATCH PROCESSING (getBatchSize, evaluateBatchColumnOriented):
//    - Rows are processed in batches (default 64) for better cache locality
//    - Column-oriented access: we read all values for one variable across
//      all batch rows before moving to the next variable. This creates
//      sequential memory access patterns that benefit from CPU prefetching.
//
// 3. ID CACHING (IdCache):
//    - ID-to-string conversions are cached across rows within a table
//    - High hit rates when the same entity appears in multiple result rows
//    - Avoids redundant vocabulary lookups and string allocations
//
// 4. DIRECT FORMATTING (generateFormattedTriples):
//    - For streaming output, we directly yield formatted strings
//    - Avoids intermediate StringTriple object allocations
//    - Uses stream_generator for 1MB buffer coalescence
//
// USAGE
// -----
// The generator maintains state (rowOffset_) and must process tables IN ORDER.
// For streaming: use generateFormattedTriples() with the desired format.
// For object access: use generateStringTriples() static helper.
//
// ============================================================================

// Forward declaration for friend access
class FormattedTripleIterator;

class ConstructTripleGenerator {
  // Allow FormattedTripleIterator to access private members for iteration
  friend class FormattedTripleIterator;

 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;
  using Triples = ad_utility::sparql_types::Triples;

  // Number of positions in a triple: subject, predicate, object.
  static constexpr size_t NUM_TRIPLE_POSITIONS = 3;

  // Identifies the source of a term's value during triple instantiation
  enum class TermSource { CONSTANT, VARIABLE, BLANK_NODE };

  // Resolution info for a single term position
  struct TermResolution {
    TermSource source;
    size_t index;  // Index into the appropriate cache (constants/vars/blanks)
  };

  // Pre-analyzed info for a triple pattern to enable fast instantiation
  struct TriplePatternInfo {
    std::array<TermResolution, NUM_TRIPLE_POSITIONS> resolutions;
  };

  // Variable with pre-computed column index for fast evaluation
  struct VariableWithColumnIndex {
    Variable variable;
    std::optional<size_t> columnIndex;  // nullopt if variable not in result
  };

  // BlankNode with precomputed prefix and suffix for fast evaluation.
  // The blank node format is: prefix + rowNumber + suffix
  // where prefix is "_:g" or "_:u" and suffix is "_" + label.
  // This avoids recomputing these constant parts for every row.
  struct BlankNodeFormatInfo {
    std::string prefix;  // "_:g" or "_:u"
    std::string suffix;  // "_" + label
  };

  // Cache for ID-to-string conversions to avoid redundant vocabulary lookups
  // when the same ID appears multiple times across rows.
  // Uses LRU eviction to bound memory usage for queries with many unique IDs.
  // Empty string represents UNDEF values (no valid RDF term is empty).
  // Uses StableLRUCache to guarantee pointer stability for
  // BatchEvaluationCache.
  using IdCache = ad_utility::StableLRUCache<Id, std::string>;

  // Minimum capacity for the LRU cache. This should be large enough to hold
  // the working set of a single batch (batch_size * num_variables) plus
  // headroom for cross-batch cache hits on repeated values (e.g., predicates).
  // 100k entries ≈ 10-20MB depending on average string length.
  static constexpr size_t MIN_CACHE_CAPACITY = 100'000;

  // Statistics for ID cache performance analysis
  struct IdCacheStats {
    size_t hits = 0;
    size_t misses = 0;
    size_t totalLookups() const { return hits + misses; }
    double hitRate() const {
      return totalLookups() > 0 ? static_cast<double>(hits) /
                                      static_cast<double>(totalLookups())
                                : 0.0;
    }
  };

  // RAII logger for IdCache statistics. Logs stats at INFO level when
  // destroyed (i.e., after query execution completes). Only logs if there
  // were a meaningful number of lookups (> 1000).
  class IdCacheStatsLogger {
   public:
    IdCacheStatsLogger(size_t numRows, size_t cacheCapacity)
        : numRows_(numRows), cacheCapacity_(cacheCapacity) {}

    ~IdCacheStatsLogger();

    // Accessors for the stats (used during cache operations)
    IdCacheStats& stats() { return stats_; }
    const IdCacheStats& stats() const { return stats_; }

   private:
    IdCacheStats stats_;
    size_t numRows_;
    size_t cacheCapacity_;
  };

  // ---------------------------------------------------------------------------
  // Batch Processing Configuration
  // ---------------------------------------------------------------------------
  // Batch size controls the trade-off between cache locality and overhead:
  //   - Smaller batches (e.g., 64): Better L1/L2 cache hit rates because
  //     the working set (IDs + strings for batch rows) fits in cache.
  //   - Larger batches (e.g., 1000+): Lower per-batch overhead but risk
  //     cache thrashing when working set exceeds cache size.
  //
  // The optimal value depends on: number of variables, average string size,
  // and hardware cache hierarchy. Default of 64 works well empirically.
  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  // Get the batch size, configurable via QLEVER_CONSTRUCT_BATCH_SIZE env var.
  // Example: QLEVER_CONSTRUCT_BATCH_SIZE=256 ./qlever-server -i index -p 7001
  // The value is read once at first call and cached for the process lifetime.
  static size_t getBatchSize();

  // Batch evaluation cache organized for column-oriented access.
  // variableStringPtrs[varIdx][rowInBatch] stores pointers directly into the
  // IdCache, eliminating the need for a second hash lookup during triple
  // instantiation. Pointers are stable within a batch because we don't modify
  // IdCache between evaluation and instantiation.
  // blankNodeValues[blankNodeIdx][rowInBatch] stores strings directly since
  // blank nodes can't be cached (the blank node values include the row number).
  struct BatchEvaluationCache {
    // Store pointers to strings in IdCache - nullptr for UNDEF or missing
    std::vector<std::vector<const std::string*>> variableStringPtrs;
    // Store string values for blank nodes (can't be cached by Id)
    std::vector<std::vector<std::string>> blankNodeValues;
    size_t numRows = 0;

    // Get string pointer for a specific variable at a row in the batch
    const std::string* getVariableString(size_t varIdx,
                                         size_t rowInBatch) const {
      return variableStringPtrs[varIdx][rowInBatch];
    }

    // Get value for a specific blank node at a row in the batch
    const std::string& getBlankNodeValue(size_t blankNodeIdx,
                                         size_t rowInBatch) const {
      return blankNodeValues[blankNodeIdx][rowInBatch];
    }
  };

  // _____________________________________________________________________________
  ConstructTripleGenerator(Triples constructTriples,
                           std::shared_ptr<const Result> result,
                           const VariableToColumnMap& variableColumns,
                           const Index& index,
                           CancellationHandle cancellationHandle);

  // _____________________________________________________________________________
  // This generator has to be called for each table contained in the result of
  // `ExportQueryExecutionTrees::getRowIndices` IN ORDER (because of
  // rowOffsset).
  //
  // For each row of the result table (the table that is created as result of
  // processing the WHERE-clause of a CONSTRUCT-query) it creates the resulting
  // triples by instantiating the triple-patterns with the values of the
  // result-table row (triple-patterns are the triples in the CONSTRUCT-clause
  // of a CONSTRUCT-query). The following pipeline takes place conceptually:
  // result-table -> result-table Rows -> Triple Patterns -> StringTriples
  auto generateStringTriplesForResultTable(const TableWithRange& table);

  // _____________________________________________________________________________
  // Generate triples as formatted strings for the given output format.
  // This is the main entry point for streaming CONSTRUCT results.
  // Returns a type-erased input range that works in both C++17 and C++20.
  // Yields formatted strings directly, avoiding StringTriple allocation.
  ad_utility::InputRangeTypeErased<std::string> generateFormattedTriples(
      const TableWithRange& table, ConstructOutputFormat format);

  // _____________________________________________________________________________
  // Helper function that generates the result of a CONSTRUCT query as a range
  // of `StringTriple`s.
  static ad_utility::InputRangeTypeErased<StringTriple> generateStringTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

 private:
  // Scans the template triples to identify all unique Variables and BlankNodes,
  // precomputes constants (IRIs/Literals), and builds the resolution map.
  // Delegates to analyzeTerm() for each term in each triple pattern.
  void analyzeTemplate();

  // Analyzes a single term and returns its resolution info.
  // Dispatches to the appropriate type-specific handler based on term type.
  TermResolution analyzeTerm(const GraphTerm& term, size_t tripleIdx,
                             size_t pos, PositionInTriple role);

  // Analyzes an IRI term: precomputes the string value.
  TermResolution analyzeIriTerm(const Iri& iri, size_t tripleIdx, size_t pos);

  // Analyzes a Literal term: precomputes the string value.
  TermResolution analyzeLiteralTerm(const Literal& literal, size_t tripleIdx,
                                    size_t pos, PositionInTriple role);

  // Analyzes a Variable term: registers it and precomputes column index.
  TermResolution analyzeVariableTerm(const Variable& var);

  // Analyzes a BlankNode term: registers it and precomputes format strings.
  TermResolution analyzeBlankNodeTerm(const BlankNode& blankNode);

  // Evaluates all Variables and BlankNodes for a batch of rows using
  // column-oriented access for better cache locality.
  // Delegates to evaluateVariablesForBatch() and evaluateBlankNodesForBatch().
  BatchEvaluationCache evaluateBatchColumnOriented(
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      IdCache& idCache, IdCacheStatsLogger& statsLogger) const;

  // Evaluates all variables for a batch of rows using column-oriented access.
  // For each variable, reads all IDs from its column across all batch rows,
  // converts them to strings (using the cache), and stores pointers.
  void evaluateVariablesForBatch(BatchEvaluationCache& batchCache,
                                 const IdTable& idTable,
                                 const LocalVocab& localVocab,
                                 ql::span<const uint64_t> rowIndices,
                                 size_t currentRowOffset, IdCache& idCache,
                                 IdCacheStatsLogger& statsLogger) const;

  // Evaluates all blank nodes for a batch of rows.
  // Uses precomputed prefix/suffix, only concatenating the row number per row.
  void evaluateBlankNodesForBatch(BatchEvaluationCache& batchCache,
                                  ql::span<const uint64_t> rowIndices,
                                  size_t currentRowOffset) const;

  // Instantiates a single triple using the precomputed constants and
  // the batch evaluation cache for a specific row. Returns an empty
  // StringTriple if any component is UNDEF. Variable string values are
  // provided via the pre-computed variableStrings cache (one lookup per
  // variable per row, reused across all triples in the row).
  StringTriple instantiateTripleFromBatch(
      size_t tripleIdx, const BatchEvaluationCache& batchCache,
      size_t rowInBatch,
      const std::vector<const std::string*>& variableStrings) const;

  // Helper to get string pointer for a term in a triple.
  // Returns nullptr if the term is UNDEF.
  const std::string* getTermStringPtr(
      size_t tripleIdx, size_t pos, const BatchEvaluationCache& batchCache,
      size_t rowInBatch,
      const std::vector<const std::string*>& variableStrings) const;

  // Creates an ID cache with a statistics logger that logs at INFO level
  // when destroyed (after query execution completes).
  std::pair<std::shared_ptr<IdCache>, std::shared_ptr<IdCacheStatsLogger>>
  createIdCacheWithStats(size_t numRows) const;

  // Populates variableStrings with pointers to cached string values for a row.
  // Sets nullptr for variables that are not in the result or have UNDEF values.
  void lookupVariableStrings(
      const BatchEvaluationCache& batchCache, size_t rowInBatch,
      const IdCache& idCache,
      std::vector<const std::string*>& variableStrings) const;

  // Formats a single triple according to the output format.
  // Returns empty string if any component is UNDEF.
  std::string formatTriple(const std::string* subject,
                           const std::string* predicate,
                           const std::string* object,
                           ConstructOutputFormat format) const;

  // triple templates contained in the graph template of the CONSTRUCT-query.
  Triples templateTriples_;
  // wrapper around the result-table obtained from processing the
  // WHERE-clause of the CONSTRUCT-query.
  std::shared_ptr<const Result> result_;
  // map from Variables to the column idx of the `IdTable` (needed for fetching
  // the value of a Variable for a specific row of the `IdTable`).
  std::reference_wrapper<const VariableToColumnMap> variableColumns_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;

  // Precomputed constant values for IRIs and Literals
  // [tripleIdx][position] -> evaluated constant (or nullopt if not a constant)
  std::vector<std::array<std::optional<std::string>, NUM_TRIPLE_POSITIONS>>
      precomputedConstants_;

  // Pre-analyzed info for each triple pattern (resolutions + skip flag)
  std::vector<TriplePatternInfo> triplePatternInfos_;

  // Mapping from variable to index in the per-row variable cache
  ad_utility::HashMap<Variable, size_t> variableToIndex_;

  // Mapping from blank node label to index in the per-row blank node cache
  ad_utility::HashMap<std::string, size_t> blankNodeLabelToIndex_;

  // Ordered list of `Variables` with pre-computed column indices for evaluation
  // (index corresponds to cache index)
  std::vector<VariableWithColumnIndex> variablesToEvaluate_;

  // Ordered list of `BlankNodes` with precomputed format info for evaluation
  // (index corresponds to cache index)
  std::vector<BlankNodeFormatInfo> blankNodesToEvaluate_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
