// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <functional>

#include "engine/ConstructQueryEvaluator.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "global/Constants.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"

// ConstructTripleGenerator: generates StringTriples from
// query results. It manages the global row offset and transforms result tables
// and rows into a single continuous range of triples.
class ConstructTripleGenerator {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;
  using Triples = ad_utility::sparql_types::Triples;

  // _____________________________________________________________________________
  ConstructTripleGenerator(Triples constructTriples,
                           std::shared_ptr<const Result> result,
                           const VariableToColumnMap& variableColumns,
                           const Index& index,
                           CancellationHandle cancellationHandle)
      : templateTriples_(std::move(constructTriples)),
        result_(std::move(result)),
        variableColumns_(variableColumns),
        index_(index),
        cancellationHandle_(std::move(cancellationHandle)) {}

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
  // Helper function that generates the result of a CONSTRUCT query as a range
  // of `StringTriple`s.
  static ad_utility::InputRangeTypeErased<StringTriple> generateStringTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      const LimitOffsetClause& limitAndOffset,
      std::shared_ptr<const Result> result, uint64_t& resultSize,
      CancellationHandle cancellationHandle);

 private:
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
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
