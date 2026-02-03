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
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"

// _____________________________________________________________________________
// Generates triples from CONSTRUCT query results by instantiating triple
// patterns (from the CONSTRUCT clause) with values from the result table
// (produced by the WHERE clause).
// The generator transforms: Result Table -> Rows -> Triple Patterns -> Output.
// For each row in the result table, we instantiate each triple pattern by
// substituting variables with their values from that row. The output is either
// `StringTriple` objects or pre-formatted strings (Turtle/CSV/TSV).
// Constants (IRIs, Literals) are evaluated once at construction of the
// `ConstructTripleGenerator`. `Variable` column indices in `IdTable` are
// pre-computed. `BlankNode` format strings are pre-built (only row number
// varies). Rows of the result-table are processed in batches (default 64) for
// better cache locality. For each variable, we read all `Id`s from its column
// in the `IdTable` across all rows in the batch before moving to the next
// variable. Since `IdTable` uses column-major storage, this creates sequential
// memory access patterns that benefit from CPU prefetching. ID-to-string
// conversions are cached across rows within a table. For streaming output, we
// directly yield formatted strings. This Avoids intermediate `StringTriple`
// object allocations
// _____________________________________________________________________________
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
    size_t index;  // index into the appropriate cache (constants/vars/blanks)
  };

  // Pre-analyzed info for a triple pattern to enable fast instantiation
  struct TriplePatternInfo {
    std::array<TermResolution, NUM_TRIPLE_POSITIONS> resolutions_;
  };

  // Variable with pre-computed column index for `IdTable`.
  struct VariableWithColumnIndex {
    Variable variable_;
    // idx of the column for the variable in the `IdTable`.
    std::optional<size_t> columnIndex_;
  };

  // `BlankNode` with precomputed prefix and suffix for fast evaluation.
  // The blank node format is: prefix + rowNumber + suffix
  // where prefix is "_:g" or "_:u" and suffix_ is "_" + label.
  // This avoids recomputing these constant parts for every result table row.
  struct BlankNodeFormatInfo {
    std::string prefix_;  // "_:g" or "_:u"
    std::string suffix_;  // "_" + label
  };

  // Cache for ID-to-string conversions to avoid redundant vocabulary lookups
  // when the same ID appears multiple times across rows.
  // Uses LRU eviction to bound memory usage for queries with many unique IDs.
  // Empty string represents UNDEF values (no valid RDF term is empty).
  // Note: We use StableLRUCache for its LRU semantics; pointer stability is
  // not required since strings are copied into BatchEvaluationCache.
  using IdCache = ad_utility::StableLRUCache<Id, std::string>;

  // Minimum capacity for the LRU cache. Sized to maximize cross-batch cache
  // hits on repeated values (e.g., predicates that appear in many rows).
  static constexpr size_t MIN_CACHE_CAPACITY = 100'000;

  // Statistics for ID cache performance analysis
  struct IdCacheStats {
    size_t hits_ = 0;
    size_t misses_ = 0;
    size_t totalLookups() const { return hits_ + misses_; }
    double hitRate() const {
      return totalLookups() > 0 ? static_cast<double>(hits_) /
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

    // Non-copyable: copying would cause duplicate logging on destruction.
    IdCacheStatsLogger(const IdCacheStatsLogger&) = delete;
    IdCacheStatsLogger& operator=(const IdCacheStatsLogger&) = delete;

    // Non-movable: this logger is used in-place and should not be moved.
    IdCacheStatsLogger(IdCacheStatsLogger&&) = delete;
    IdCacheStatsLogger& operator=(IdCacheStatsLogger&&) = delete;

    // Accessors for the stats (used during cache operations)
    IdCacheStats& stats() { return stats_; }
    const IdCacheStats& stats() const { return stats_; }

   private:
    IdCacheStats stats_;
    size_t numRows_;
    size_t cacheCapacity_;
  };

  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  // Get the batch size, configurable via QLEVER_CONSTRUCT_BATCH_SIZE env var.
  // Example: QLEVER_CONSTRUCT_BATCH_SIZE=256 ./qlever-server -i index -p 7001
  // The value is read once at first call and cached for the process lifetime.
  static size_t getBatchSize();

  // Batch evaluation cache organized for column-oriented access.
  // variableStrings_[varIdx][rowInBatch] stores the string values directly,
  // providing clear ownership semantics. The IdCache is still used to
  // deduplicate vocabulary lookups, but strings are copied into this cache
  // for safe access during triple instantiation.
  // blankNodeValues_[blankNodeIdx][rowInBatch] stores strings directly since
  // blank nodes can't be cached across result table rows (the blank node
  // values include the row number).
  struct BatchEvaluationCache {
    // Store string values directly. Empty optional for UNDEF or missing.
    // maps: variable idx -> idx of row in batch -> string value (or nullopt)
    // which the variable corresponding to the variable idx for the
    // specific row of the batch evaluates to.
    std::vector<std::vector<std::optional<std::string>>> variableStrings_;
    // Store string values for blank nodes (can't be cached by `Id`)
    // maps: blank node idx -> idx of row in batch -> string object representing
    // the corresponding `BlankNode` object.
    std::vector<std::vector<std::string>> blankNodeValues_;
    size_t numRows_ = 0;

    // Get string pointer for a specific variable at a row in the batch.
    // Returns nullptr if the value is UNDEF or missing.
    const std::string* getVariableString(size_t varIdx,
                                         size_t rowInBatch) const {
      const auto& opt = variableStrings_[varIdx][rowInBatch];
      return opt.has_value() ? &opt.value() : nullptr;
    }

    // Get value for a specific blank node at a row in the batch
    const std::string& getBlankNodeValue(size_t blankNodeIdx,
                                         size_t rowInBatch) const {
      return blankNodeValues_[blankNodeIdx][rowInBatch];
    }
  };

  // ___________________________________________________________________________
  ConstructTripleGenerator(Triples constructTriples,
                           std::shared_ptr<const Result> result,
                           const VariableToColumnMap& variableColumns,
                           const Index& index,
                           CancellationHandle cancellationHandle);

  // ___________________________________________________________________________
  // This generator has to be called for each table contained in the result of
  // `ExportQueryExecutionTrees::getRowIndices` IN ORDER (because of
  // rowOffsset). For each row of the result table (the table that is created as
  // result of processing the WHERE-clause of a CONSTRUCT-query) it creates the
  // resulting triples by instantiating the triple-patterns with the values of
  // the result-table row (triple-patterns are the triples in the
  // CONSTRUCT-clause of a CONSTRUCT-query). The following pipeline takes place
  // conceptually: result-table -> processing batches -> result-table rows ->
  // triple patterns
  // -> `StringTriples`
  ad_utility::InputRangeTypeErased<StringTriple>
  generateStringTriplesForResultTable(const TableWithRange& table);

  // ___________________________________________________________________________
  // Generate triples as formatted strings for the given output format.
  // This is the main entry point for streaming CONSTRUCT results.
  // Yields formatted strings directly, avoiding `StringTriple` allocation.
  ad_utility::InputRangeTypeErased<std::string> generateFormattedTriples(
      const TableWithRange& table, ad_utility::MediaType mediaType);

  // ___________________________________________________________________________
  // Helper function that generates the result of a CONSTRUCT query as a range
  // of `StringTriple`s.
  static ad_utility::InputRangeTypeErased<StringTriple> generateStringTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

 private:
  // Scans the template triples to identify all unique `Variables` and
  // `BlankNodes`, precomputes constants (IRIs/Literals),
  // and builds the resolution map (which maps each position of the graph
  // template to how the term at this position is to be resolved).
  void analyzeTemplate();

  // Analyzes a single term and returns its resolution info.
  // Dispatches to the appropriate type-specific handler based on term type.
  TermResolution analyzeTerm(const GraphTerm& term, size_t tripleIdx,
                             size_t pos, PositionInTriple role);

  // Analyzes a `Iri` term: precomputes the string value.
  TermResolution analyzeIriTerm(const Iri& iri, size_t tripleIdx, size_t pos);

  // Analyzes a `Literal` term: precomputes the string value.
  TermResolution analyzeLiteralTerm(const Literal& literal, size_t tripleIdx,
                                    size_t pos, PositionInTriple role);

  // Analyzes a `Variable` term: registers it and precomputes column index.
  TermResolution analyzeVariableTerm(const Variable& var);

  // Analyzes a `BlankNode` term: registers it and precomputes format strings.
  TermResolution analyzeBlankNodeTerm(const BlankNode& blankNode);

  // Evaluates all `Variables` and `BlankNodes` for a batch of result-table
  // rows.
  BatchEvaluationCache evaluateBatchColumnOriented(
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      IdCache& idCache, IdCacheStatsLogger& statsLogger) const;

  // For each `Variable`, reads all IDs from its column across all batch rows,
  // converts them to strings (using the cache), and stores pointers to those
  // strings in `BatchEvaluationCache`.
  void evaluateVariablesForBatch(BatchEvaluationCache& batchCache,
                                 const IdTable& idTable,
                                 const LocalVocab& localVocab,
                                 ql::span<const uint64_t> rowIndices,
                                 size_t currentRowOffset, IdCache& idCache,
                                 IdCacheStatsLogger& statsLogger) const;

  // Evaluates all `BlankNodes` for a batch of rows. Uses precomputed
  // prefix/suffix, only concatenating the row number per row.
  void evaluateBlankNodesForBatch(BatchEvaluationCache& batchCache,
                                  ql::span<const uint64_t> rowIndices,
                                  size_t currentRowOffset) const;

  // Instantiates a single triple using the precomputed constants and
  // the batch evaluation cache for a specific row. Returns an empty
  // `StringTriple` if any component is UNDEF. `Variable` string values are
  // provided via the precomputed variableStrings cache (one lookup per
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
                           ad_utility::MediaType format) const;

  // Processes a single batch and returns the resulting StringTriples.
  // Used by generateStringTriplesForResultTable to lazily process batches.
  std::vector<StringTriple> processBatchForStringTriples(
      const TableConstRefWithVocab& tableWithVocab, size_t currentRowOffset,
      IdCache& idCache, IdCacheStatsLogger& statsLogger,
      ql::span<const uint64_t> batchRowIndices);

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

  // Precomputed constant values for iri's and Literals
  // [tripleIdx][position] -> evaluated constant (or nullopt if not a constant)
  std::vector<std::array<std::optional<std::string>, NUM_TRIPLE_POSITIONS>>
      precomputedConstants_;

  // Pre-analyzed info for each triple pattern (resolutions_ + skip flag)
  std::vector<TriplePatternInfo> triplePatternInfos_;

  // Mapping from variable to index in the per-row variable cache
  // `variablesToEvaluate_`.
  ad_utility::HashMap<Variable, size_t> variableToIndex_;

  // Mapping from blank node label to index in the per-row blank node cache
  ad_utility::HashMap<std::string, size_t> blankNodeLabelToIndex_;

  // Ordered list of `Variables` with pre-computed column indices for evaluation
  std::vector<VariableWithColumnIndex> variablesToEvaluate_;

  // Ordered list of `BlankNodes` with precomputed format info for evaluation
  // (index corresponds to cache index)
  std::vector<BlankNodeFormatInfo> blankNodesToEvaluate_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
