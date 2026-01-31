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
class ConstructTripleGenerator {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;
  using Triples = ad_utility::sparql_types::Triples;

  // Identifies the source of a term's value during triple instantiation
  enum class TermSource { CONSTANT, VARIABLE, BLANK_NODE };

  // Resolution info for a single term position
  struct TermResolution {
    TermSource source;
    size_t index;  // Index into the appropriate cache (constants/vars/blanks)
  };

  // Pre-analyzed info for a triple pattern to enable fast instantiation
  struct TriplePatternInfo {
    std::array<TermResolution, 3> resolutions;
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

  // Cache for ID-to-string conversions to avoid redundant conversions
  // when the same ID appears multiple times across rows
  using IdCache = ad_utility::HashMap<Id, std::optional<std::string>>;

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
  // variableIds[varIdx][rowInBatch] stores Id values - strings are looked up
  // from the shared IdCache on demand during instantiation, avoiding double
  // storage of string values.
  // blankNodeValues[blankNodeIdx][rowInBatch] stores strings directly since
  // blank nodes can't be cached (the blank node values include the row number).
  struct BatchEvaluationCache {
    // Store Id values for variables - nullopt if variable not in result
    std::vector<std::vector<std::optional<Id>>> variableIds;
    // Store string values for blank nodes (can't be cached by Id)
    std::vector<std::vector<std::optional<std::string>>> blankNodeValues;
    size_t numRows = 0;

    // Get Id for a specific variable at a row in the batch
    const std::optional<Id>& getVariableId(size_t varIdx,
                                           size_t rowInBatch) const {
      return variableIds[varIdx][rowInBatch];
    }

    // Get value for a specific blank node at a row in the batch
    const std::optional<std::string>& getBlankNodeValue(
        size_t blankNodeIdx, size_t rowInBatch) const {
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
  ad_utility::streams::stream_generator generateFormattedTriples(
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
  void analyzeTemplate();

  // Evaluates all Variables and BlankNodes for a batch of rows using
  // column-oriented access for better cache locality.
  // Processes variables column-by-column, then blank nodes row-by-row.
  // Uses ql::span for zero-copy access to row indices.
  BatchEvaluationCache evaluateBatchColumnOriented(
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      IdCache& idCache, IdCacheStats& cacheStats) const;

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

  // Creates an ID cache with optional statistics logging on destruction.
  // Stats are logged only when totalLookups > 10000.
  std::pair<std::shared_ptr<IdCache>, std::shared_ptr<IdCacheStats>>
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
  std::vector<std::array<std::optional<std::string>, 3>> precomputedConstants_;

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
