// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "parser/data/ConstructQueryExportContext.h"

using ad_utility::InputRangeTypeErased;
using enum PositionInTriple;
using StringTriple = ConstructTripleGenerator::StringTriple;

// _____________________________________________________________________________
void ConstructTripleGenerator::precomputeConstants() {
  precomputedConstants_.resize(templateTriples_.size());
  for (size_t tripleIdx = 0; tripleIdx < templateTriples_.size(); ++tripleIdx) {
    const auto& triple = templateTriples_[tripleIdx];
    for (size_t pos = 0; pos < 3; ++pos) {
      const GraphTerm& term = triple[pos];
      PositionInTriple role = static_cast<PositionInTriple>(pos);
      if (std::holds_alternative<Iri>(term)) {
        auto val = ConstructQueryEvaluator::evaluate(std::get<Iri>(term));
        precomputedConstants_[tripleIdx][pos] =
            val.has_value() ? std::make_optional((std::move(val.value())))
                            : std::nullopt;
      } else if (std::holds_alternative<Literal>(term)) {
        auto val =
            ConstructQueryEvaluator::evaluate(std::get<Literal>(term), role);
        precomputedConstants_[tripleIdx][pos] =
            val.has_value() ? std::make_optional(std::move(val.value()))
                            : std::nullopt;
      }
    }
  }
}

// _____________________________________________________________________________
ConstructTripleGenerator::ConstructTripleGenerator(
    Triples constructTriples, std::shared_ptr<const Result> result,
    const VariableToColumnMap& variableColumns, const Index& index,
    CancellationHandle cancellationHandle)
    : templateTriples_(std::move(constructTriples)),
      result_(std::move(result)),
      variableColumns_(variableColumns),
      index_(index),
      cancellationHandle_(std::move(cancellationHandle)) {
  // initialize a cache containing the constants of the graph triple template.
  precomputeConstants();
}

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
        [this, context = std::move(context)](const auto& pair) {
          auto [tripleIdx, templateTriple] = pair;
          cancellationHandle_->throwIfCancelled();
          return ConstructTripleGenerator::evaluateTriple(
              tripleIdx, templateTriple, context);
        };

    // Apply the transformer from above and filter out invalid evaluations
    // (which are returned as empty `StringTriples` from
    // `evaluateConstructTripleForRowFromWhereClause`).
    return ql::views::iota(size_t{0}, templateTriples_.size()) |
           ql::views::transform([this](size_t tripleIdx) {
             return std::make_pair(tripleIdx, templateTriples_[tripleIdx]);
           }) |
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

// _____________________________________________________________________________
StringTriple ConstructTripleGenerator::evaluateTriple(
    size_t tripleIdx, const std::array<GraphTerm, 3>& triple,
    const ConstructQueryExportContext& context) {
  using enum PositionInTriple;

  // array to hold evaluated components [SUBJECT=0, PREDICATE=1, OBJECT=2]
  std::array<std::optional<std::string>, 3> components;

  for (size_t pos = 0; pos < 3; ++pos) {
    const GraphTerm& term = triple[pos];

    // 1. check precomputed constants (IRIs/Literals) first - O(1) lookup
    if (precomputedConstants_[tripleIdx][pos].has_value()) {
      components[pos] = precomputedConstants_[tripleIdx][pos];
    }
    // 2. fall back to per-row evaluation for Variables/BlankNodes.
    PositionInTriple role = static_cast<PositionInTriple>(pos);
    components[pos] =
        ConstructQueryEvaluator::evaluateTerm(term, context, role);

    // early exit if this component is UNDEF.
    if (!components[pos].has_value()) {
      return StringTriple{};
    }
  }
  return StringTriple{*components[0], *components[1], *components[2]};
}
