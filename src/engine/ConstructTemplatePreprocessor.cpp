// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTemplatePreprocessor.h"

#include <absl/strings/str_cat.h>

#include "util/Algorithm.h"
#include "util/HashMap.h"
#include "util/TypeTraits.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessIri(
    const Iri& iri) {
  return PrecomputedConstant{
      std::make_shared<const EvaluatedTermData>(iri.iri(), nullptr)};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessLiteral(const Literal& literal,
                                                 PositionInTriple role) {
  if (role == PositionInTriple::OBJECT) {
    return PrecomputedConstant{
        std::make_shared<const EvaluatedTermData>(literal.literal(), nullptr)};
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
  // Maps each IdTable column index to its position in
  // `result.uniqueVariableColumns_` (and in `BatchEvaluationResult`).
  ad_utility::HashMap<size_t, size_t> columnToPosition;

  for (const auto& triple : templateTriples) {
    auto preprocessedTriple = preprocessTriple(triple, variableColumns);
    if (!preprocessedTriple) continue;

    // Remap each variable's IdTable column index to its position index, which
    // enables direct vector indexing in `BatchEvaluationResult::getVariable`.
    for (auto& term : *preprocessedTriple) {
      if (auto* var = std::get_if<PrecomputedVariable>(&term)) {
        auto [it, wasNew] = columnToPosition.try_emplace(
            var->columnIndex_, columnToPosition.size());
        if (wasNew) {
          result.uniqueVariableColumns_.push_back(var->columnIndex_);
        }
        var->columnIndex_ = it->second;
      }
    }
    result.preprocessedTriples_.push_back(std::move(*preprocessedTriple));
  }

  return result;
}

}  // namespace qlever::constructExport
