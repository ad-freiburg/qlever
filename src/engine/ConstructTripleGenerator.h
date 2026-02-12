// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <functional>
#include <memory>
#include <vector>

#include "engine/ConstructTypes.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"

// Generates triples from CONSTRUCT query results by instantiating triple
// patterns (from the CONSTRUCT clause) with values from the result table
// (produced by the WHERE clause).
class ConstructTripleGenerator {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;
  using Triples = ad_utility::sparql_types::Triples;

  ConstructTripleGenerator(Triples constructTriples,
                           std::shared_ptr<const Result> result,
                           const VariableToColumnMap& variableColumns,
                           const Index& index,
                           CancellationHandle cancellationHandle);

  // Generate `StringTriple`s for a single result table.
  ad_utility::InputRangeTypeErased<StringTriple>
  generateStringTriplesForResultTable(const TableWithRange& table);

  // Generate formatted strings for a single result table.
  template <ad_utility::MediaType format>
  ad_utility::InputRangeTypeErased<std::string> generateFormattedTriples(
      const TableWithRange& table);

  // Generate formatted strings for all tables in a range.
  template <ad_utility::MediaType format>
  ad_utility::InputRangeTypeErased<std::string> generateAllFormattedTriples(
      ad_utility::InputRangeTypeErased<TableWithRange> rowIndices);

  // Helper that generates `StringTriple`s for a full CONSTRUCT query.
  static ad_utility::InputRangeTypeErased<StringTriple> generateStringTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

 private:
  Triples templateTriples_;
  std::shared_ptr<const Result> result_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;

  // Preprocessed template with triples and unique variable columns.
  PreprocessedConstructTemplate preprocessedTemplate_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
