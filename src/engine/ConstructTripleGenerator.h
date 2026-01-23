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

  template <class T>
  using InputRangeTypeErased = ad_utility::InputRangeTypeErased<T>;

  // _____________________________________________________________________________
  ConstructTripleGenerator(Triples constructTriples,
                           std::shared_ptr<const Result> result,
                           const VariableToColumnMap& variableColumns,
                           const Index& index,
                           CancellationHandle cancellationHandle)
      : constructTriples_(std::move(constructTriples)),
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
  auto generateStringTriplesForResultTable(TableWithRange table) {
    const auto tableWithVocab = table.tableWithVocab_;
    size_t currentRowOffset = rowOffset_;
    rowOffset_ += tableWithVocab.idTable().numRows();

    // For a single row from the WHERE clause (specified by `idTable` and
    // `rowIdx` stored in the `context`), evaluate all triples in the CONSTRUCT
    // template.
    auto outerTransformer = [this, tableWithVocab,
                             currentRowOffset](uint64_t rowIdx) {
      ConstructQueryExportContext context{rowIdx,
                                          tableWithVocab.idTable(),
                                          tableWithVocab.localVocab(),
                                          variableColumns_.get(),
                                          index_.get(),
                                          currentRowOffset};

      // Transform a single template triple from the CONSTRUCT-template into
      // a `StringTriple` for a single row of the WHERE clause (specified by
      // `idTable` and `rowIdx` stored in `context`).
      auto evaluateConstructTripleForRowFromWhereClause =
          [this, context = std::move(context)](const auto& templateTriple) {
            cancellationHandle_->throwIfCancelled();
            return ConstructQueryEvaluator::evaluateTriple(templateTriple,
                                                           context);
          };

      // Apply the transformer from above and filter out invalid evaluations
      // (which are returned as empty `StringTriples` from
      // `evaluateConstructTripleForRowFromWhereClause`).
      return constructTriples_ |
             ql::views::transform(
                 evaluateConstructTripleForRowFromWhereClause) |
             ql::views::filter(std::not_fn(&StringTriple::isEmpty));
    };
    return table.view_ | ql::views::transform(outerTransformer) |
           ql::views::join;
  }

  // _____________________________________________________________________________
  // Helper function that generates the result of a CONSTRUCT query as a range
  // of `StringTriple`s.
  static InputRangeTypeErased<StringTriple> generateStringTriples(
      const QueryExecutionTree& qet,
      const ad_utility::sparql_types::Triples& constructTriples,
      LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
      uint64_t& resultSize, CancellationHandle cancellationHandle);

 private:
  Triples constructTriples_;
  std::shared_ptr<const Result> result_;
  std::reference_wrapper<const VariableToColumnMap> variableColumns_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
