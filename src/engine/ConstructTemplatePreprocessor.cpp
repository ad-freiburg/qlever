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
#include "util/TypeTraits.h"
#include "util/VariantRangeFilter.h"

namespace qlever::constructExport {

// Resolve `tripleComponent` (a constant IRI or literal from the CONSTRUCT
// template) to its `ValueId`: encoded values and vocabulary terms become
// `ValueId`s directly; a literal/IRI not present in the vocabulary is assigned
// a fresh `LocalVocabIndex` in `localvocabForConstructTemplateConstants`. This
// `ValueId` is the constant's component of the full-triple deduplication key.
ValueId ConstructTemplatePreprocessor::resolveConstantDedupId(
    TripleComponent tripleComponent) {
  return std::move(tripleComponent).toValueId(index_, localVocab_);
}

// True if any of the triple's three positions is a blank node. Such triples are
// excluded from deduplication: their per-row blank node ids make every
// instantiation distinct.
static bool tripleContainsBlankNode(const PreprocessedTriple& triple) {
  return ql::ranges::any_of(triple,
                            ad_utility::holdsAlternative<PrecomputedBlankNode>);
};

// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessIri(
    const Iri& iri) {
  ValueId dedupId = resolveConstantDedupId(TripleComponent{iri});
  return PrecomputedConstant{
      std::make_shared<const EvaluatedTermData>(iri.toSparql(), nullptr),
      dedupId};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessLiteral(const Literal& literal,
                                                 PositionInTriple role) {
  // A literal is only legal in OBJECT position; per SPARQL 1.1 §16.2 a template
  // instantiation yielding a literal in subject/predicate position produces no
  // RDF triple, so we return `nullopt` to drop the triple. For the object we
  // use the full Turtle object parser (not a string normalize like
  // `preprocessIri`): the literal's datatype decides its `ValueId` encoding
  // (e.g. `"1"^^xsd:integer` → encoded integer, not a vocab string), which
  // `parseTripleObject` resolves the same way the index does at build time,
  // keeping the dedup key consistent.
  if (role != PositionInTriple::OBJECT) {
    return std::nullopt;
  }
  TripleComponent parsedObject =
      // TODO: Use only a single `Literal` class in all of QLever.
      RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
          literal.toSparql());
  ValueId dedupId = resolveConstantDedupId(std::move(parsedObject));
  return PrecomputedConstant{
      std::make_shared<const EvaluatedTermData>(literal.literal(), nullptr),
      dedupId};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessVariable(const Variable& variable) {
  if (auto opt = ad_utility::findOptional(variableColumns_, variable)) {
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
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessTermImpl(const GraphTerm& term,
                                                  PositionInTriple role) {
  return term.visit(
      [this, &role](const auto& t) -> std::optional<PreprocessedTerm> {
        using T = std::decay_t<decltype(t)>;
        if constexpr (std::is_same_v<T, Iri>) {
          return preprocessIri(t);
        } else if constexpr (std::is_same_v<T, Literal>) {
          return preprocessLiteral(t, role);
        } else if constexpr (std::is_same_v<T, Variable>) {
          return preprocessVariable(t);
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
    const std::array<GraphTerm, NUM_TRIPLE_POSITIONS>& triple) {
  PreprocessedTriple preprocessedTriple;
  for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
    auto role = static_cast<PositionInTriple>(pos);
    auto preprocessed = preprocessTermImpl(triple[pos], role);
    if (!preprocessed) return std::nullopt;
    preprocessedTriple[pos] = std::move(*preprocessed);
  }
  return preprocessedTriple;
}

// _____________________________________________________________________________
PreprocessedConstructTemplate ConstructTemplatePreprocessor::preprocess(
    const Triples& templateTriples, const VariableToColumnMap& variableColumns,
    const Index& index) {
  return ConstructTemplatePreprocessor{variableColumns, index}.run(
      templateTriples);
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessTerm(
    const GraphTerm& term, PositionInTriple role,
    const VariableToColumnMap& variableColumns, const Index& index,
    LocalVocab& localVocabForConstants) {
  return ConstructTemplatePreprocessor{variableColumns, index,
                                       localVocabForConstants}
      .preprocessTermImpl(term, role);
}

// _____________________________________________________________________________
PreprocessedConstructTemplate ConstructTemplatePreprocessor::run(
    const Triples& templateTriples) && {
  for (const auto& triple : templateTriples) {
    auto preprocessedTriple = preprocessTriple(triple);
    if (!preprocessedTriple) continue;

    // Collect each unique `IdTable` column index.
    // `PrecomputedVariable::columnIndex_` is kept as the original `IdTable`
    // column index so that it matches the keys in
    // `BatchEvaluationResult::variablesByColumn_`.
    for (const PrecomputedVariable& var :
         ad_utility::filterRangeOfVariantsByType<PrecomputedVariable>(
             *preprocessedTriple)) {
      if (seenColumns_.insert(var.columnIndex_).second) {
        result_.uniqueVariableColumns_.push_back(var.columnIndex_);
      }
    }
    result_.tripleContainsBlankNode_.push_back(
        tripleContainsBlankNode(*preprocessedTriple));
    result_.preprocessedTriples_.push_back(std::move(*preprocessedTriple));
  }

  return std::move(result_);
}

}  // namespace qlever::constructExport
