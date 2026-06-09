// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H

#include <vector>

#include "engine/ConstructTypes.h"
#include "engine/VariableToColumnMap.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/Types.h"

namespace qlever::constructExport {

// Preprocesses CONSTRUCT template triples. For each term, precomputes
// the following:
// - constants (IRIs/literals): evaluates and stores the string value.
// - variables: precomputes the column index into the `IdTable`.
// - blank nodes: precomputes the format prefix/suffix.
class ConstructTemplatePreprocessor {
 public:
  using Triples = ad_utility::sparql_types::Triples;

  // Preprocess the template triples. Returns the preprocessed triples together
  // with the unique variable column indices needed when evaluating the template
  // triples for specific result-table rows. Constant terms (IRIs/literals) are
  // resolved to a stable `ValueId` (their full-triple deduplication key
  // component) using `index`; literals not present in the vocabulary are
  // assigned a fresh id in the returned template's `constantLocalVocab_`.
  static PreprocessedConstructTemplate preprocess(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns, const Index& index);

  // Preprocess a single `GraphTerm` into a `PreprocessedTerm`. Returns
  // `std::nullopt` if the term is undefined (e.g. an unbound variable).
  static std::optional<PreprocessedTerm> preprocessTerm(
      const GraphTerm& term, PositionInTriple role,
      const VariableToColumnMap& variableColumns, const Index& index,
      LocalVocab& localVocabForConstants);

 private:
  static std::optional<PreprocessedTerm> preprocessIri(
      const Iri& iri, const Index& index, LocalVocab& localVocabForConstants);
  static std::optional<PreprocessedTerm> preprocessLiteral(
      const Literal& literal, PositionInTriple role, const Index& index,
      LocalVocab& localVocabForConstants);
  static std::optional<PreprocessedTerm> preprocessVariable(
      const Variable& variable, const VariableToColumnMap& variableColumns);
  static std::optional<PreprocessedTerm> preprocessBlankNode(
      const BlankNode& blankNode);

  // Preprocess all three terms of a single template triple. Returns
  // `std::nullopt` if any term fails to preprocess (e.g. an unbound variable).
  static std::optional<PreprocessedTriple> preprocessTriple(
      const std::array<GraphTerm, NUM_TRIPLE_POSITIONS>& triple,
      const VariableToColumnMap& variableColumns, const Index& index,
      LocalVocab& constantLocalVocab);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H
