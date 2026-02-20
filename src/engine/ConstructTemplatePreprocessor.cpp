// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTemplatePreprocessor.h"

#include <absl/strings/str_cat.h>

#include "engine/ConstructQueryEvaluator.h"
#include "util/Algorithm.h"
#include "util/TypeTraits.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessIri(
    const Iri& iri) {
  return PrecomputedConstant{ConstructQueryEvaluator::evaluate(iri)};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessLiteral(const Literal& literal,
                                                 PositionInTriple role) {
  auto opt = ConstructQueryEvaluator::evaluate(literal, role);
  if (opt) {
    return PrecomputedConstant{std::move(*opt)};
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessVariable(
    const Variable& variable, const VariableToColumnMap& variableColumns) {
  if (auto opt = ad_utility::findOptional(variableColumns, variable)) {
    return PrecomputedVariable{opt->columnIndex_};
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessBlankNode(const BlankNode& blankNode) {
  return PrecomputedBlankNode{blankNode.isGenerated() ? "_:g" : "_:u",
                              absl::StrCat("_", blankNode.label())};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessTerm(
    const GraphTerm& term, PositionInTriple role,
    const VariableToColumnMap& variableColumns) {
  return std::visit(
      [&role,
       &variableColumns](const auto& t) -> std::optional<PreprocessedTerm> {
        using T = std::decay_t<decltype(t)>;
        if constexpr (std::is_same_v<T, Iri>) {
          return preprocessIri(t);
        } else if constexpr (std::is_same_v<T, Literal>) {
          return preprocessLiteral(t, role);
        } else if constexpr (std::is_same_v<T, Variable>) {
          return preprocessVariable(t, variableColumns);
        } else if constexpr (std::is_same_v<T, BlankNode>) {
          return preprocessBlankNode(t);
        } else {
          static_assert(ad_utility::alwaysFalse<T>);
        }
      },
      term);
}

// _____________________________________________________________________________
std::optional<PreprocessedTriple>
ConstructTemplatePreprocessor::preprocessTriple(
    const std::array<GraphTerm, NUM_TRIPLE_POSITIONS>& triple,
    const VariableToColumnMap& variableColumns) {
  PreprocessedTriple preprocessedTriple;
  for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
    auto role = static_cast<PositionInTriple>(pos);
    auto preprocessed = preprocessTerm(triple[pos], role, variableColumns);
    if (!preprocessed) return std::nullopt;
    preprocessedTriple[pos] = std::move(*preprocessed);
  }
  return preprocessedTriple;
}

// _____________________________________________________________________________
PreprocessedConstructTemplate ConstructTemplatePreprocessor::preprocess(
    const Triples& templateTriples,
    const VariableToColumnMap& variableColumns) {
  PreprocessedConstructTemplate result;
  ad_utility::HashSet<size_t> uniqueColumnsSet;

  for (const auto& triple : templateTriples) {
    auto preprocessedTriple = preprocessTriple(triple, variableColumns);
    if (!preprocessedTriple) continue;

    // Only collect variable column indices once the triple is known to be
    // valid.
    for (const auto& term : *preprocessedTriple) {
      if (auto* var = std::get_if<PrecomputedVariable>(&term)) {
        uniqueColumnsSet.insert(var->columnIndex_);
      }
    }
    result.preprocessedTriples_.push_back(std::move(*preprocessedTriple));
  }

  result.uniqueVariableColumns_.assign(uniqueColumnsSet.begin(),
                                       uniqueColumnsSet.end());
  return result;
}

}  // namespace qlever::constructExport
