// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include <functional>

#include "engine/QueryExecutionTree.h"
#include "engine/QueryExportTypes.h"
#include "global/Constants.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"

/**
 * ConstructTripleGenerator: generates StringTriples from
 * query results. It manages the global row offset and transforms result tables
 * and rows into a single continuous range of triples.
 */
class ConstructTripleGenerator {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;
  using Triples = ad_utility::sparql_types::Triples;

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
  auto generateForTable(TableWithRange table) {
    const auto tableWithVocab = table.tableWithVocab_;
    size_t currentRowOffset = rowOffset_;
    rowOffset_ += tableWithVocab.idTable().numRows();

    // this is a pipeline which transforms the rows of the result table
    // (`table.view_`) using the triple patterns of the CONSTRUCT-clause
    return ql::views::join(ql::views::transform(
        std::move(table.view_),
        [this, tableWithVocab, currentRowOffset](uint64_t rowIdx) {
          ConstructQueryExportContext context{rowIdx,
                                              tableWithVocab.idTable(),
                                              tableWithVocab.localVocab(),
                                              variableColumns_.get(),
                                              index_.get(),
                                              currentRowOffset};

          // Transform patterns into triples and filter out UNDEF results.
          return ql::views::filter(
              ql::views::transform(
                  constructTriples_,
                  [this, context = std::move(context)](const auto& triple) {
                    cancellationHandle_->throwIfCancelled();
                    return this->evaluateTriple(triple, context);
                  }),
              [](const StringTriple& t) { return !t.isEmpty(); });
        }));
  }

 private:
  // Evaluates a single CONSTRUCT triple pattern using the provided context.
  StringTriple evaluateTriple(const std::array<GraphTerm, 3>& triple,
                              const ConstructQueryExportContext& context) const;

  Triples constructTriples_;
  std::shared_ptr<const Result> result_;
  std::reference_wrapper<const VariableToColumnMap> variableColumns_;
  std::reference_wrapper<const Index> index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
