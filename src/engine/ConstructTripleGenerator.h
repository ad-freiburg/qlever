// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <functional>
#include <memory>
#include <vector>

#include "engine/ConstructTypes.h"
#include "engine/EvaluatedTripleIterator.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"
#include "util/http/MediaTypes.h"
#include "util/stream_generator.h"

namespace qlever::constructExport {

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

  // Generate formatted strings for all tables in a range. Consumes the
  // generator; must be called as
  // `std::move(generator).generateFormattedTriplesForResultTable(...)`.
  ad_utility::InputRangeTypeErased<std::string> generateFormattedTriples(
      ad_utility::InputRangeTypeErased<TableWithRange> rowIndices,
      ad_utility::MediaType format) &&;

  // Generate `StringTriple`s for all tables in a range. Consumes the
  // generator; must be called as
  // `std::move(generator).generateStringTriples(...)`.
  ad_utility::InputRangeTypeErased<StringTriple> generateStringTriples(
      ad_utility::InputRangeTypeErased<TableWithRange> rowIndices) &&;

 private:
  Triples templateTriples_;
  std::shared_ptr<const Result> result_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;

  // Preprocessed template with triples and unique variable columns.
  PreprocessedConstructTemplate preprocessedTemplate_;

  // Generate `StringTriple`s for a single result table.
  ad_utility::InputRangeTypeErased<StringTriple>
  generateStringTriplesForResultTable(const TableWithRange& table);

  // Generate formatted strings for a single result table.
  ad_utility::InputRangeTypeErased<std::string>
  generateFormattedTriplesForResultTable(const TableWithRange& table,
                                         ad_utility::MediaType format);

  // Helper that handles `rowOffset_` and creates a `EvaluatedTripleIterator`.
  std::unique_ptr<EvaluatedTripleIterator> prepareRowProcessor(
      const TableWithRange& table);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
