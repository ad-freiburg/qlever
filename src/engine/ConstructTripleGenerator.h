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

// ConstructTripleGenerator: generates StringTriples from
// query results. It manages the global row offset and transforms result tables
// and rows into a single continuous range of triples.
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

  // Per-row evaluation cache
  struct RowEvaluationCache {
    std::vector<std::optional<std::string>> variableValues;
    std::vector<std::optional<std::string>> blankNodeValues;
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

  // Default batch size for column-oriented processing.
  // Batch size affects CPU cache utilization:
  //   - Smaller batches: Better L1/L2 cache locality, more batch overhead
  //   - Larger batches: Amortized overhead, potential cache thrashing
  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  // Get the batch size, configurable via QLEVER_CONSTRUCT_BATCH_SIZE env var.
  // Example: QLEVER_CONSTRUCT_BATCH_SIZE=256 ./ServerMain -i index -p 7001
  // The value is read once at first call and cached for the process lifetime.
  static size_t getBatchSize();

  // Batch evaluation cache organized for column-oriented access.
  // variableIds[varIdx][rowInBatch] stores Id values - strings are looked up
  // from the shared IdCache on demand during instantiation, avoiding double
  // storage of string values.
  // blankNodeValues[blankNodeIdx][rowInBatch] stores strings directly since
  // blank nodes can't be cached (they include the row number).
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
  // Generate triples and yield them as formatted turtle strings.
  ad_utility::streams::stream_generator generateTurtleTriples(
      const TableWithRange& table);

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

  // Evaluates all Variables and BlankNodes for a single row, returning
  // a cache that can be used to instantiate all triples for that row.
  // Uses idCache to avoid redundant ID-to-string conversions.
  // Updates cacheStats with hit/miss information.
  RowEvaluationCache evaluateRowTerms(
      const ConstructQueryExportContext& context, IdCache& idCache,
      IdCacheStats& cacheStats) const;

  // Evaluates all Variables and BlankNodes for a batch of rows using
  // column-oriented access for better cache locality.
  // Processes variables column-by-column, then blank nodes row-by-row.
  // Uses ql::span for zero-copy access to row indices.
  BatchEvaluationCache evaluateBatchColumnOriented(
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      IdCache& idCache, IdCacheStats& cacheStats) const;

  // Instantiates a single triple using the precomputed constants and
  // the per-row evaluation cache. Returns an empty StringTriple if any
  // component is UNDEF.
  StringTriple instantiateTriple(size_t tripleIdx,
                                 const RowEvaluationCache& cache) const;

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

  // triple templates contained in the graph template
  // (the CONSTRUCT-clause of the CONSTRUCt-query) of the CONSTRUCT-query.
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

  // Mapping from Variable to index in the per-row variable cache
  ad_utility::HashMap<Variable, size_t> variableToIndex_;

  // Mapping from BlankNode label to index in the per-row blank node cache
  ad_utility::HashMap<std::string, size_t> blankNodeLabelToIndex_;

  // Ordered list of Variables with pre-computed column indices for evaluation
  // (index corresponds to cache index)
  std::vector<VariableWithColumnIndex> variablesToEvaluate_;

  // Ordered list of BlankNodes with precomputed format info for evaluation
  // (index corresponds to cache index)
  std::vector<BlankNodeFormatInfo> blankNodesToEvaluate_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
