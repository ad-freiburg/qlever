// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <functional>
#include <memory>

#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"
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
  // triple templates contained in the graph template of the CONSTRUCT-query.
  Triples templateTriples_;
  // wrapper around the result-table obtained from processing the
  // WHERE-clause of the CONSTRUCT-query.
  std::shared_ptr<const Result> result_;
  // map from Variables to the column idx of the `IdTable` (needed for fetching
  // the value of a `Variable` for a specific row of the `IdTable`).
  std::reference_wrapper<const VariableToColumnMap> variableColumns_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;

  // Contains data obtained by preprocessing the template triples.
  // Created once by `ConstructTemplatePreprocessor::preprocess()`.
  std::shared_ptr<PreprocessedConstructTemplate> preprocessedConstructTemplate_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
