// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTemplatePreprocessor.h"

#include <absl/strings/str_cat.h>

#include "engine/ConstructQueryEvaluator.h"

using enum PositionInTriple;

// _____________________________________________________________________________
std::shared_ptr<PreprocessedConstructTemplate>
ConstructTemplatePreprocessor::preprocess(
    const Triples& templateTriples, const Index& index,
    const VariableToColumnMap& variableColumns,
    CancellationHandle cancellationHandle) {
  auto result = std::make_shared<PreprocessedConstructTemplate>(
      index, variableColumns, std::move(cancellationHandle));

  // Resize context arrays for template analysis.
  result->precomputedConstants_.resize(templateTriples.size());
  result->triplePatternInfos_.resize(templateTriples.size());

  // Temporary maps for deduplication during preprocessing.
  ad_utility::HashMap<Variable, size_t> variableToIndex;
  ad_utility::HashMap<std::string, size_t> blankNodeLabelToIndex;

  for (size_t tripleIdx = 0; tripleIdx < templateTriples.size(); ++tripleIdx) {
    const auto& triple = templateTriples[tripleIdx];
    TripleInstantitationRecipe& info = result->triplePatternInfos_[tripleIdx];

    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      auto role = static_cast<PositionInTriple>(pos);
      info.lookups_[pos] =
          preprocessTerm(triple[pos], tripleIdx, role, *result, variableToIndex,
                         blankNodeLabelToIndex, variableColumns);
    }
  }

  return result;
}

// _____________________________________________________________________________
TripleInstantitationRecipe::TermInstantitationRecipe
ConstructTemplatePreprocessor::preprocessTerm(
    const GraphTerm& term, size_t tripleIdx, PositionInTriple role,
    PreprocessedConstructTemplate& result,
    ad_utility::HashMap<Variable, size_t>& variableToIndex,
    ad_utility::HashMap<std::string, size_t>& blankNodeLabelToIndex,
    const VariableToColumnMap& variableColumns) {
  const size_t pos = static_cast<size_t>(role);
  if (std::holds_alternative<Iri>(term)) {
    return preprocessIriTerm(std::get<Iri>(term), tripleIdx, pos, result);
  } else if (std::holds_alternative<Literal>(term)) {
    return preprocessLiteralTerm(std::get<Literal>(term), tripleIdx, role,
                                 result);
  } else if (std::holds_alternative<Variable>(term)) {
    return preprocessVariableTerm(std::get<Variable>(term), result,
                                  variableToIndex, variableColumns);
  } else if (std::holds_alternative<BlankNode>(term)) {
    return preprocessBlankNodeTerm(std::get<BlankNode>(term), result,
                                   blankNodeLabelToIndex);
  }
  // Unreachable for valid GraphTerm
  // TODO<ms2144> add error throw here.
  return {TripleInstantitationRecipe::TermType::CONSTANT, 0};
}

// _____________________________________________________________________________
TripleInstantitationRecipe::TermInstantitationRecipe
ConstructTemplatePreprocessor::preprocessIriTerm(
    const Iri& iri, size_t tripleIdx, size_t pos,
    PreprocessedConstructTemplate& result) {
  // evaluate(Iri) always returns a valid string
  result.precomputedConstants_[tripleIdx][pos] =
      ConstructQueryEvaluator::evaluate(iri);
  return {TripleInstantitationRecipe::TermType::CONSTANT, tripleIdx};
}

// _____________________________________________________________________________
TripleInstantitationRecipe::TermInstantitationRecipe
ConstructTemplatePreprocessor::preprocessLiteralTerm(
    const Literal& literal, size_t tripleIdx, PositionInTriple role,
    PreprocessedConstructTemplate& result) {
  const size_t pos = static_cast<size_t>(role);
  // evaluate(Literal) returns optional - only store if valid
  auto value = ConstructQueryEvaluator::evaluate(literal, role);
  if (value.has_value()) {
    result.precomputedConstants_[tripleIdx][pos] = std::move(*value);
  }
  return {TripleInstantitationRecipe::TermType::CONSTANT, tripleIdx};
}

// _____________________________________________________________________________
TripleInstantitationRecipe::TermInstantitationRecipe
ConstructTemplatePreprocessor::preprocessVariableTerm(
    const Variable& var, PreprocessedConstructTemplate& result,
    ad_utility::HashMap<Variable, size_t>& variableToIndex,
    const VariableToColumnMap& variableColumns) {
  if (!variableToIndex.contains(var)) {
    size_t idx = result.variablesToInstantiate_.size();
    variableToIndex[var] = idx;
    // Pre-compute the column index corresponding to the variable in the
    // `IdTable`.
    std::optional<size_t> columnIndex;
    if (variableColumns.contains(var)) {
      columnIndex = variableColumns.at(var).columnIndex_;
    }
    result.variablesToInstantiate_.emplace_back(
        VariableWithColumnIndex{var, columnIndex});
  }
  return {TripleInstantitationRecipe::TermType::VARIABLE, variableToIndex[var]};
}

// _____________________________________________________________________________
TripleInstantitationRecipe::TermInstantitationRecipe
ConstructTemplatePreprocessor::preprocessBlankNodeTerm(
    const BlankNode& blankNode, PreprocessedConstructTemplate& result,
    ad_utility::HashMap<std::string, size_t>& blankNodeLabelToIndex) {
  const std::string& label = blankNode.label();
  if (!blankNodeLabelToIndex.contains(label)) {
    size_t idx = result.blankNodesToInstantiate_.size();
    blankNodeLabelToIndex[label] = idx;
    // Precompute prefix ("_:g" or "_:u") and suffix ("_" + label)
    // so we only need to concatenate the row number per row
    BlankNodeFormatInfo formatInfo;
    formatInfo.prefix_ = blankNode.isGenerated() ? "_:g" : "_:u";
    formatInfo.suffix_ = absl::StrCat("_", label);
    result.blankNodesToInstantiate_.push_back(std::move(formatInfo));
  }
  return {TripleInstantitationRecipe::TermType::BLANK_NODE,
          blankNodeLabelToIndex[label]};
}
