// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTemplatePreprocessor.h"

#include <absl/strings/str_cat.h>

#include "parser/RdfParser.h"
#include "parser/TokenizerCtre.h"
#include "parser/TripleComponent.h"
#include "util/Algorithm.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/TypeTraits.h"
#include "util/VariantRangeFilter.h"

namespace qlever::constructExport {

// Resolve `tripleComponent` (a constant IRI or literal from the CONSTRUCT
// template) to its stable `ValueId`: encoded values and vocabulary terms become
// `ValueId`s directly; a literal/IRI not present in the vocabulary is assigned
// a fresh `LocalVocabIndex` in `constantLocalVocab`. This `ValueId` is the
// constant's component of the full-triple deduplication key.
static ValueId resolveConstantDedupId(TripleComponent tripleComponent,
                                      const Index& index,
                                      LocalVocab& constantLocalVocab) {
  return std::move(tripleComponent).toValueId(index, constantLocalVocab);
}

static bool tripleContainsBlankNode(const PreprocessedTriple& triple) {
  return ql::ranges::any_of(triple, [](const PreprocessedTerm& term) {
    return std::holds_alternative<PrecomputedBlankNode>(term);
  });
}

// A template triple is "ground" if all three positions are constants (no
// variables and no blank nodes). Such a triple instantiates to the same output
// triple for every result row.
static bool isGroundTriple(const PreprocessedTriple& triple) {
  return ql::ranges::all_of(triple, [](const PreprocessedTerm& term) {
    return std::holds_alternative<PrecomputedConstant>(term);
  });
}

// Builds the single `EvaluatedTriple` for a ground triple. Each position is a
// `PrecomputedConstant` whose `EvaluatedTerm` is already resolved, so no result
// row or `Index` is needed.
static EvaluatedTriple makeGroundTriple(const PreprocessedTriple& triple) {
  return EvaluatedTriple{
      std::get<PrecomputedConstant>(triple[0]).evaluatedTerm_,
      std::get<PrecomputedConstant>(triple[1]).evaluatedTerm_,
      std::get<PrecomputedConstant>(triple[2]).evaluatedTerm_};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessIri(
    const Iri& iri, const Index& index, LocalVocab& constantLocalVocab) {
  ValueId dedupId = resolveConstantDedupId(
      TripleComponent{
          ad_utility::triple_component::Iri::fromStringRepresentation(
              iri.toSparql())},
      index, constantLocalVocab);
  return PrecomputedConstant{
      std::make_shared<const EvaluatedTermData>(iri.iri(), nullptr), dedupId};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessLiteral(
    const Literal& literal, PositionInTriple role, const Index& index,
    LocalVocab& constantLocalVocab) {
  if (role == PositionInTriple::OBJECT) {
    ValueId dedupId = resolveConstantDedupId(
        RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
            literal.toSparql()),
        index, constantLocalVocab);
    return PrecomputedConstant{
        std::make_shared<const EvaluatedTermData>(literal.literal(), nullptr),
        dedupId};
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
    const VariableToColumnMap& variableColumns, const Index& index,
    LocalVocab& constantLocalVocab) {
  return term.visit([&role, &variableColumns, &index, &constantLocalVocab](
                        const auto& t) -> std::optional<PreprocessedTerm> {
    using T = std::decay_t<decltype(t)>;
    if constexpr (std::is_same_v<T, Iri>) {
      return preprocessIri(t, index, constantLocalVocab);
    } else if constexpr (std::is_same_v<T, Literal>) {
      return preprocessLiteral(t, role, index, constantLocalVocab);
    } else if constexpr (std::is_same_v<T, Variable>) {
      return preprocessVariable(t, variableColumns);
    } else if constexpr (std::is_same_v<T, BlankNode>) {
      return preprocessBlankNode(t);
    } else {
      static_assert(ad_utility::alwaysFalse<T>);
    }
  });
}

// _____________________________________________________________________________
std::optional<PreprocessedTriple>
ConstructTemplatePreprocessor::preprocessTriple(
    const std::array<GraphTerm, NUM_TRIPLE_POSITIONS>& triple,
    const VariableToColumnMap& variableColumns, const Index& index,
    LocalVocab& constantLocalVocab) {
  PreprocessedTriple preprocessedTriple;
  for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
    auto role = static_cast<PositionInTriple>(pos);
    auto preprocessed = preprocessTerm(triple[pos], role, variableColumns,
                                       index, constantLocalVocab);
    if (!preprocessed) return std::nullopt;
    preprocessedTriple[pos] = std::move(*preprocessed);
  }
  return preprocessedTriple;
}

// _____________________________________________________________________________
PreprocessedConstructTemplate ConstructTemplatePreprocessor::preprocess(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    const Index& index) {
  PreprocessedConstructTemplate result;
  // Tracks which `IdTable` column indices have already been added to
  // `result.uniqueVariableColumns_` to avoid duplicates.
  ad_utility::HashSet<size_t> seenColumns;

  for (const auto& triple : templateTriples) {
    auto preprocessedTriple = preprocessTriple(triple, variableColumns, index,
                                               result.constantLocalVocab_);
    if (!preprocessedTriple) continue;

    // Ground triples are handled separately (emitted once, never deduplicated)
    // and excluded from the per-row instantiation and dedup structures.
    if (isGroundTriple(*preprocessedTriple)) {
      result.groundTriples_.push_back(makeGroundTriple(*preprocessedTriple));
      continue;
    }

    // Collect each unique `IdTable` column index.
    // `PrecomputedVariable::columnIndex_` is kept as the original `IdTable`
    // column index so that it matches the keys in
    // `BatchEvaluationResult::variablesByColumn_`.
    std::vector<size_t> tripleColumns;
    for (const PrecomputedVariable& var :
         ad_utility::filterRangeOfVariantsByType<PrecomputedVariable>(
             *preprocessedTriple)) {
      if (seenColumns.insert(var.columnIndex_).second) {
        result.uniqueVariableColumns_.push_back(var.columnIndex_);
      }
      tripleColumns.push_back(var.columnIndex_);
    }
    result.variableColumnsPerTriple_.push_back(std::move(tripleColumns));
    result.tripleContainsBlankNode_.push_back(
        tripleContainsBlankNode(*preprocessedTriple));
    result.preprocessedTriples_.push_back(std::move(*preprocessedTriple));
  }

  return result;
}

}  // namespace qlever::constructExport
