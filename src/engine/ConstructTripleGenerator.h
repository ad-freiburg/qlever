// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <memory>

#include "engine/ConstructTypes.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "engine/Result.h"
#include "engine/VariableToColumnMap.h"
#include "index/Index.h"
#include "util/CancellationHandle.h"
#include "util/Iterators.h"
#include "util/http/MediaTypes.h"

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
  // `std::move(generator).generateFormattedTriples(...)`.
  ad_utility::InputRangeTypeErased<std::string> generateFormattedTriples(
      ad_utility::InputRangeTypeErased<TableWithRange> tables,
      ad_utility::MediaType format) &&;

  // Generate `StringTriple`s for all tables in a range. Consumes the
  // generator; must be called as
  // `std::move(generator).generateStringTriples(...)`.
  ad_utility::InputRangeTypeErased<StringTriple> generateStringTriples(
      ad_utility::InputRangeTypeErased<TableWithRange> tables) &&;

 private:
  // Kept alive to prevent the IdTable/LocalVocab references in the
  // TableWithRange objects from dangling.
  std::shared_ptr<const Result> result_;
  PreprocessedConstructTemplate preprocessedTemplate_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
