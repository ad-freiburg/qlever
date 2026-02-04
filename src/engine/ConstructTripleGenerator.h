// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <functional>
#include <memory>

#include "backports/span.h"
#include "engine/ConstructIdCache.h"
#include "engine/ConstructQueryEvaluator.h"
#include "engine/InstantiationBlueprint.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "parser/data/BlankNode.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"
#include "util/HashMap.h"
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"

// _____________________________________________________________________________
// Generates triples from CONSTRUCT query results by instantiating triple
// patterns (from the CONSTRUCT clause) with values from the result table
// (produced by the WHERE clause).
// The generator transforms: Result Table -> Rows -> Triple Patterns -> Output.
// For each row in the result table, we instantiate each triple pattern by
// substituting variables with their values from that row.
// Constants (IRIs, Literals) are evaluated once at construction of the
// `ConstructTripleGenerator`. `Variable` column indices in `IdTable` are
// pre-computed. `BlankNode` format strings are pre-built (only row number
// varies). Rows of the result-table are processed in batches. For each
// variable , we read all `Id`s from its column in the `IdTable` across all
// rows in the batch before moving to the next variable. ID-to-string
// conversions are cached across rows within a table. For streaming output, we
// directly yield formatted strings. This Avoids intermediate `StringTriple`
// object allocations
// _____________________________________________________________________________
class ConstructTripleGenerator {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;
  using Triples = ad_utility::sparql_types::Triples;

  // Type aliases for ID cache types (defined in ConstructIdCache.h)
  using IdCache = ConstructIdCache;
  using IdCacheStatsLogger = ConstructIdCacheStatsLogger;

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
  // triple patterns -> `StringTriples`
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
  // Preprocesses the template triples to create the InstantiationBlueprint.
  // For each term position, determines how to obtain its value:
  // - Constants (IRIs/Literals): evaluates and stores the string
  // - Variables: precomputes column indices for IdTable lookup
  // - Blank nodes: precomputes format prefix/suffix
  void preprocessTemplate();

  // Analyzes a single term and returns its resolution info.
  // Dispatches to the appropriate type-specific handler based on term type.
  TriplePatternInfo::TermLookupInfo analyzeTerm(const GraphTerm& term,
                                                size_t tripleIdx, size_t pos,
                                                PositionInTriple role);

  // Analyzes a `Iri` term: precomputes the string value.
  TriplePatternInfo::TermLookupInfo analyzeIriTerm(const Iri& iri,
                                                   size_t tripleIdx,
                                                   size_t pos);

  // Analyzes a `Literal` term: precomputes the string value.
  TriplePatternInfo::TermLookupInfo analyzeLiteralTerm(const Literal& literal,
                                                       size_t tripleIdx,
                                                       size_t pos,
                                                       PositionInTriple role);

  // Analyzes a `Variable` term: registers it and precomputes `IdTable` column
  // index.
  TriplePatternInfo::TermLookupInfo analyzeVariableTerm(const Variable& var);

  // Analyzes a `BlankNode` term: registers it and precomputes format strings.
  TriplePatternInfo::TermLookupInfo analyzeBlankNodeTerm(
      const BlankNode& blankNode);

  // Instantiates a single triple using the precomputed constants and
  // the batch evaluation cache for a specific row. Returns an empty
  // `StringTriple` if any component is UNDEF.
  StringTriple instantiateTripleFromBatch(
      size_t tripleIdx, const BatchEvaluationCache& batchCache,
      size_t rowInBatch) const;

  // Helper to get shared_ptr for a term in a triple.
  // Returns nullptr if the term is UNDEF.
  std::shared_ptr<const std::string> getTermStringPtr(
      size_t tripleIdx, size_t pos, const BatchEvaluationCache& batchCache,
      size_t rowInBatch) const;

  // Creates an `Id` cache with a statistics logger that logs at INFO level
  // when destroyed (after query execution completes).
  std::pair<std::shared_ptr<IdCache>, std::shared_ptr<IdCacheStatsLogger>>
  createIdCacheWithStats(size_t numRows) const;

  // Evaluates all `Variables` and `BlankNodes` for a batch of result-table
  // rows.
  BatchEvaluationCache evaluateBatchColumnOriented(
      const IdTable& idTable, const LocalVocab& localVocab,
      ql::span<const uint64_t> rowIndices, size_t currentRowOffset,
      IdCache& idCache, IdCacheStatsLogger& statsLogger) const;

  // For each `Variable`, reads all `Id`s from its column across all batch rows,
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

  // Processes a single batch and returns the resulting `StringTriple` objects.
  // Used by `generateStringTriplesForResultTable` to lazily process batches.
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

  // Blueprint containing preprocessed template data. Created once by
  // preprocessTemplate() and shared (immutably) with ConstructBatchProcessor.
  std::shared_ptr<InstantiationBlueprint> blueprint_;

  // Mapping from variable to index in the per-row variable cache
  // `variablesToEvaluate_`.
  ad_utility::HashMap<Variable, size_t> variableToIndex_;

  // Mapping from blank node label to index in the per-row blank node cache
  ad_utility::HashMap<std::string, size_t> blankNodeLabelToIndex_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
