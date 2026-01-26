// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include "engine/ExportQueryExecutionTrees.h"

using ad_utility::InputRangeTypeErased;

// _____________________________________________________________________________
auto ConstructTripleGenerator::generateStringTriplesForResultTable(
    const TableWithRange& table) {
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
    return templateTriples_ |
           ql::views::transform(evaluateConstructTripleForRowFromWhereClause) |
           ql::views::filter(std::not_fn(&StringTriple::isEmpty));
  };
  return table.view_ | ql::views::transform(outerTransformer) | ql::views::join;
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<QueryExecutionTree::StringTriple>
ConstructTripleGenerator::generateStringTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  // The `resultSizeMultiplicator`(last argument of `getRowIndices`) is
  // explained by the following: For each result from the WHERE clause, we
  // produce up to `constructTriples.size()` triples. We do not account for
  // triples that are filtered out because one of the components is UNDEF (it
  // would require materializing the whole result)
  auto rowIndices = ExportQueryExecutionTrees::getRowIndices(
      limitAndOffset, *result, resultSize, constructTriples.size());

  ConstructTripleGenerator generator(
      constructTriples, std::move(result), qet.getVariableColumns(),
      qet.getQec()->getIndex(), std::move(cancellationHandle));

  // Transform the range of tables into a flattened range of triples.
  // We move the generator into the transformation lambda to extend its
  // lifetime. Because the transformation is stateful (it tracks rowOffset_),
  // the lambda must be marked 'mutable'.
  auto tableTriples = ql::views::transform(
      ad_utility::OwningView{std::move(rowIndices)},
      [generator = std::move(generator)](const TableWithRange& table) mutable {
        // The generator now handles the:
        // Table -> Rows -> Triple Patterns -> StringTriples
        return generator.generateStringTriplesForResultTable(table);
      });
  return InputRangeTypeErased(ql::views::join(std::move(tableTriples)));
}
