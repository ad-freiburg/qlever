// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructBatchProcessor.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/InstantiationBlueprint.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "rdfTypes/RdfEscaping.h"

using ad_utility::InputRangeTypeErased;
using enum PositionInTriple;
using StringTriple = ConstructTripleGenerator::StringTriple;
using CancellationHandle = ad_utility::SharedCancellationHandle;

// Called once at construction to preprocess the CONSTRUCT triple patterns.
// For each term, we precompute the following, based on the `TermType`:
// - CONSTANT: `Iri`s and `Literal`s are evaluated once and stored
// - VARIABLE: Column index of variable in `IdTable` is precomputed.
// - BLANK_NODE: Format prefix/suffix are pre-built (row number varies)
void ConstructTripleGenerator::preprocessTemplate() {
  // Create the context that will be shared with ConstructBatchProcessor
  // instances.
  instantiationBlueprint_ = std::make_shared<InstantiationBlueprint>(
      index_.get(), variableColumns_.get(), cancellationHandle_);

  // Resize context arrays for template analysis.
  instantiationBlueprint_->precomputedConstants_.resize(
      templateTriples_.size());
  instantiationBlueprint_->triplePatternInfos_.resize(templateTriples_.size());

  for (size_t tripleIdx = 0; tripleIdx < templateTriples_.size(); ++tripleIdx) {
    const auto& triple = templateTriples_[tripleIdx];
    TriplePatternInfo& info =
        instantiationBlueprint_->triplePatternInfos_[tripleIdx];

    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      auto role = static_cast<PositionInTriple>(pos);
      info.lookups_[pos] = analyzeTerm(triple[pos], tripleIdx, pos, role);
    }
  }
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeTerm(
    const GraphTerm& term, size_t tripleIdx, size_t pos,
    PositionInTriple role) {
  if (std::holds_alternative<Iri>(term)) {
    return analyzeIriTerm(std::get<Iri>(term), tripleIdx, pos);
  } else if (std::holds_alternative<Literal>(term)) {
    return analyzeLiteralTerm(std::get<Literal>(term), tripleIdx, pos, role);
  } else if (std::holds_alternative<Variable>(term)) {
    return analyzeVariableTerm(std::get<Variable>(term));
  } else if (std::holds_alternative<BlankNode>(term)) {
    return analyzeBlankNodeTerm(std::get<BlankNode>(term));
  }
  // Unreachable for valid GraphTerm
  // TODO<ms2144> add compile time error throw here.
  return {TriplePatternInfo::TermType::CONSTANT, 0};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeIriTerm(
    const Iri& iri, size_t tripleIdx, size_t pos) {
  // evaluate(Iri) always returns a valid string
  instantiationBlueprint_->precomputedConstants_[tripleIdx][pos] =
      ConstructQueryEvaluator::evaluate(iri);
  return {TriplePatternInfo::TermType::CONSTANT, tripleIdx};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeLiteralTerm(
    const Literal& literal, size_t tripleIdx, size_t pos,
    PositionInTriple role) {
  // evaluate(Literal) returns optional - only store if valid
  auto value = ConstructQueryEvaluator::evaluate(literal, role);
  if (value.has_value()) {
    instantiationBlueprint_->precomputedConstants_[tripleIdx][pos] =
        std::move(*value);
  }
  return {TriplePatternInfo::TermType::CONSTANT, tripleIdx};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo ConstructTripleGenerator::analyzeVariableTerm(
    const Variable& var) {
  if (!variableToIndex_.contains(var)) {
    size_t idx = instantiationBlueprint_->variablesToEvaluate_.size();
    variableToIndex_[var] = idx;
    // Pre-compute the column index corresponding to the variable in the
    // `IdTable`.
    std::optional<size_t> columnIndex;
    if (variableColumns_.get().contains(var)) {
      columnIndex = variableColumns_.get().at(var).columnIndex_;
    }
    instantiationBlueprint_->variablesToEvaluate_.emplace_back(
        VariableWithColumnIndex{var, columnIndex});
  }
  return {TriplePatternInfo::TermType::VARIABLE, variableToIndex_[var]};
}

// _____________________________________________________________________________
TriplePatternInfo::TermLookupInfo
ConstructTripleGenerator::analyzeBlankNodeTerm(const BlankNode& blankNode) {
  const std::string& label = blankNode.label();
  if (!blankNodeLabelToIndex_.contains(label)) {
    size_t idx = instantiationBlueprint_->blankNodesToEvaluate_.size();
    blankNodeLabelToIndex_[label] = idx;
    // Precompute prefix ("_:g" or "_:u") and suffix ("_" + label)
    // so we only need to concatenate the row number per row
    BlankNodeFormatInfo formatInfo;
    formatInfo.prefix_ = blankNode.isGenerated() ? "_:g" : "_:u";
    formatInfo.suffix_ = absl::StrCat("_", label);
    instantiationBlueprint_->blankNodesToEvaluate_.push_back(
        std::move(formatInfo));
  }
  return {TriplePatternInfo::TermType::BLANK_NODE,
          blankNodeLabelToIndex_[label]};
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
  // Analyze template: precompute constants and identify variables/blank nodes.
  preprocessTemplate();
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriplesForResultTable(
    const TableWithRange& table) {
  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  // Use ConstructBatchProcessor for batch evaluation, wrapped in
  // StringTripleBatchProcessor adapter for the StringTriple interface.
  auto processor = std::make_unique<ConstructBatchProcessor>(
      instantiationBlueprint_, table, ad_utility::MediaType::turtle,
      currentRowOffset);

  return ad_utility::InputRangeTypeErased<StringTriple>{
      std::make_unique<StringTripleBatchProcessor>(std::move(processor))};
}

// _____________________________________________________________________________
// Entry point for generating CONSTRUCT query output.
// Used when caller needs structured access to triple components.
ad_utility::InputRangeTypeErased<QueryExecutionTree::StringTriple>
ConstructTripleGenerator::generateStringTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    CancellationHandle cancellationHandle) {
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
        // conceptually, the generator now handles the following pipeline:
        // table -> processing batch -> table rows -> triple patterns -> string
        // triples
        return generator.generateStringTriplesForResultTable(table);
      });
  return InputRangeTypeErased(ql::views::join(std::move(tableTriples)));
}

// _____________________________________________________________________________
// Entry point for generating CONSTRUCT query output.
// More efficient for streaming output than `generateStringTriples` (avoids
// `StringTriple` allocations).
ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    const TableWithRange& table, ad_utility::MediaType mediaType) {
  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  return ad_utility::InputRangeTypeErased<std::string>{
      std::make_unique<ConstructBatchProcessor>(instantiationBlueprint_, table,
                                                mediaType, currentRowOffset)};
}
