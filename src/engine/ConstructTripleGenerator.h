// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <memory>

#include "engine/ConstructBatchEvaluator.h"
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

using ad_utility::InputRangeTypeErased;
using CancellationHandle = ad_utility::SharedCancellationHandle;
using Triples = ad_utility::sparql_types::Triples;
using IdCache =
    ad_utility::util::LRUCacheWithStatistics<Id, std::optional<EvaluatedTerm>>;
using StringTriple = QueryExecutionTree::StringTriple;

// Generates triples from the CONSTRUCT query results by instantiating the
// template triple patterns with the values from the result table produced by
// the WHERE clause of the CONSTRUCT query.
class ConstructTripleGenerator {
 public:
  //____________________________________________________________________________
  static InputRangeTypeErased<std::string> generateFormattedTriples(
      const Triples& templateTriples, const VariableToColumnMap& variableColums,
      const Index& index, CancellationHandle cancellationhandle,
      InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset,
      ad_utility::MediaType mediaType);

  //____________________________________________________________________________
  static InputRangeTypeErased<StringTriple> generateStringTriples(
      const Triples& templateTriples, const VariableToColumnMap& variableColums,
      const Index& index, CancellationHandle cancellationhandle,
      InputRangeTypeErased<TableWithRange> rowIndices, size_t rowOffset);

 private:
  static constexpr size_t DEFAULT_BATCH_SIZE = 1024;
  static constexpr size_t CACHE_ENTRIES_PER_VARIABLE = 2048;

  //____________________________________________________________________________
  static IdCache makeIdCache(const PreprocessedConstructTemplate& tmpl);

  //____________________________________________________________________________
  static InputRangeTypeErased<EvaluatedTriple> evaluateTableWithRange(
      const PreprocessedConstructTemplate& tmpl, const Index& index,
      CancellationHandle cancellationHandle, const TableWithRange& table,
      size_t rowOffset, IdCache& cache);

  //____________________________________________________________________________
  static InputRangeTypeErased<EvaluatedTriple> evaluateTables(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns, const Index& index,
      CancellationHandle cancellationhandle,
      ad_utility::InputRangeTypeErased<TableWithRange> rowIndices,
      size_t rowOffset);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
