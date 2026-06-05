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
// template) to its `ValueId`: encoded values and vocabulary terms become
// `ValueId`s directly; a literal/IRI not present in the vocabulary is assigned
// a fresh `LocalVocabIndex` in `localvocabForConstructTemplateConstants`. This
// `ValueId` is the constant's component of the full-triple deduplication key.
static ValueId resolveConstantDedupId(
    TripleComponent tripleComponent, const Index& index,
    LocalVocab& localvocabForConstructTemplateConstants) {
  return std::move(tripleComponent)
      .toValueId(index, localvocabForConstructTemplateConstants);
}

// True if any of the triple's three positions is a blank node. Such triples are
// excluded from deduplication: their per-row blank node ids make every
// instantiation distinct.
static bool tripleContainsBlankNode(const PreprocessedTriple& triple) {
  return ql::ranges::any_of(triple, [](const PreprocessedTerm& term) {
    return std::holds_alternative<PrecomputedBlankNode>(term);
  });
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm> ConstructTemplatePreprocessor::preprocessIri(
    const Iri& iri, const Index& index,
    LocalVocab& localVocabForConstantsInTemplate) {
  // `iri.toSparql()` yields the IRI in SPARQL `IRIREF` syntax, i.e. enclosed in
  // angle brackets (`<...>`); see the SPARQL 1.1 grammar production [139]
  // (https://www.w3.org/TR/sparql11-query/#rIRIREF). We use `fromIriref`
  // (rather than `fromStringRepresentation`) because it actually parses that
  // syntax: it normalizes/escapes the IRI content via
  // `RdfEscaping::normalizeIriWithBrackets`, producing the same internal
  // storage representation the vocabulary uses.
  ValueId dedupId = resolveConstantDedupId(
      // TODO<ms2144>: verify that `iri.toSparql()` yields the Iri string in
      // exactly the format which `fromIriref` expects in in?
      // maybe we should make the "contract" of the expected format expliciy
      // here, but how?
      //  At SPARQL parse time (SparqlQLeverVisitor.cpp), the
      // construct template IRI is built. There `Iri::toStringRepresentation()`
      // is called to fill  the parser/data/Iri.h _string member variable with
      // the format that Iri::toStringRepresentation() yields.
      // Thus we need to ensure here that we 1) know which format the IRI is in
      // and 2) thus choose the correct method to parse the Iri.
      // Best case would be to make everything explicit.
      // Even better would be to not have a string representation of the Iri
      // in the construct template but have a struct which documents the format?
      TripleComponent{
          ad_utility::triple_component::Iri::fromIriref(iri.toSparql())},
      index, localVocabForConstantsInTemplate);
  return PrecomputedConstant{std::make_shared<const EvaluatedTermData>(
                                 EvaluatedTermData{iri.iri(), nullptr}),
                             dedupId};
}

// _____________________________________________________________________________
std::optional<PreprocessedTerm>
ConstructTemplatePreprocessor::preprocessLiteral(
    const Literal& literal, PositionInTriple role, const Index& index,
    LocalVocab& localVocabForConstantsInTemplate) {
  // A literal is only legal in OBJECT position; per SPARQL 1.1 §16.2 a template
  // instantiation yielding a literal in subject/predicate position produces no
  // RDF triple, so we return `nullopt` to drop the triple. For the object we
  // use the full Turtle object parser (not a string normalize like
  // `preprocessIri`): the literal's datatype decides its `ValueId` encoding
  // (e.g. `"1"^^xsd:integer` → encoded integer, not a vocab string), which
  // `parseTripleObject` resolves the same way the index does at build time,
  // keeping the dedup key consistent.
  if (role == PositionInTriple::OBJECT) {
    TripleComponent parsedObject =
        RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
            literal.toSparql());
    ValueId dedupId = resolveConstantDedupId(std::move(parsedObject), index,
                                             localVocabForConstantsInTemplate);
    return PrecomputedConstant{
        std::make_shared<const EvaluatedTermData>(
            EvaluatedTermData{literal.literal(), nullptr}),
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

    // Collect each unique `IdTable` column index.
    // `PrecomputedVariable::columnIndex_` is kept as the original `IdTable`
    // column index so that it matches the keys in
    // `BatchEvaluationResult::variablesByColumn_`.
    for (const PrecomputedVariable& var :
         ad_utility::filterRangeOfVariantsByType<PrecomputedVariable>(
             *preprocessedTriple)) {
      if (seenColumns.insert(var.columnIndex_).second) {
        result.uniqueVariableColumns_.push_back(var.columnIndex_);
      }
    }
    result.tripleContainsBlankNode_.push_back(
        tripleContainsBlankNode(*preprocessedTriple));
    result.preprocessedTriples_.push_back(std::move(*preprocessedTriple));
  }

  return result;
}

}  // namespace qlever::constructExport
