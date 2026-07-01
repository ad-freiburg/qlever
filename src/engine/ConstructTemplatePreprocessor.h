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
#include "util/HashSet.h"

namespace qlever::constructExport {

// Preprocesses CONSTRUCT template triples. For each term, precomputes
// the following:
// - constants (IRIs/literals): evaluates and stores the string value.
// - variables: precomputes the column index into the `IdTable`.
// - blank nodes: precomputes the format prefix/suffix.
class ConstructTemplatePreprocessor {
 public:
  using Triples = ad_utility::sparql_types::Triples;
  using Iri = ad_utility::triple_component::Iri;

  // Preprocess the template triples. Returns the preprocessed triples together
  // with the unique variable column indices needed when evaluating the template
  // triples for specific result-table rows. Constant terms (IRIs/literals) are
  // resolved to a stable `ValueId` (their full-triple deduplication key
  // component) using `index`; literals not present in the vocabulary are
  // assigned a fresh id in the returned template's
  // `localVocabForConstants_`.
  //
  // This is a thin factory over the internal single-use builder: it constructs
  // a `ConstructTemplatePreprocessor` and runs it once.
  static PreprocessedConstructTemplate preprocess(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns, const Index& index);

  // Preprocess a single `GraphTerm` into a `PreprocessedTerm`. Returns
  // `std::nullopt` if the term is undefined (e.g. an unbound variable).
  // `localVocabForConstants` backs the entries of any constant's `dedupId_` and
  // must outlive the returned term. Exposed (as a thin builder wrapper) for
  // white-box per-term unit tests.
  static std::optional<PreprocessedTerm> preprocessTerm(
      const GraphTerm& term, PositionInTriple role,
      const VariableToColumnMap& variableColumns, const Index& index,
      LocalVocab& localVocabForConstants);

 private:
  // The read-only collaborators, stored once instead of being threaded through
  // every helper.
  const VariableToColumnMap& variableColumns_;
  const Index& index_;
  // The template being built up.
  PreprocessedConstructTemplate result_;
  // The `LocalVocab` that backs constant `dedupId_` entries. In the full
  // `preprocess` flow this references
  // `result_.localVocabForConstants_` (so it ships with the result);
  // the per-term `preprocessTerm` entry points it at a caller-owned vocab
  // instead. Declared after `result_` so the self-reference in the constructor
  // is well-defined.
  LocalVocab& localVocab_;
  // Tracks which `IdTable` column indices have already been added to
  // `result_.uniqueVariableColumns_` to avoid duplicates.
  ad_utility::HashSet<size_t> seenColumns_;

  // Constructor for the full-template flow: constants go into `result_`'s own
  // vocab, which is returned alongside the template.
  ConstructTemplatePreprocessor(const VariableToColumnMap& variableColumns,
                                const Index& index)
      : variableColumns_{variableColumns},
        index_{index},
        localVocab_{result_.localVocabForConstants_} {}

  // Constructor for the per-term flow: constants go into a caller-owned vocab.
  ConstructTemplatePreprocessor(const VariableToColumnMap& variableColumns,
                                const Index& index, LocalVocab& localVocab)
      : variableColumns_{variableColumns},
        index_{index},
        localVocab_{localVocab} {}

  // Run the preprocessing over all template triples and return the (moved-out)
  // result. Single-shot: call exactly once per instance.
  PreprocessedConstructTemplate run(const Triples& templateTriples) &&;

  std::optional<PreprocessedTerm> preprocessTermImpl(const GraphTerm& term,
                                                     PositionInTriple role);

  std::optional<PreprocessedTerm> preprocessIri(const Iri& iri);
  std::optional<PreprocessedTerm> preprocessLiteral(const Literal& literal,
                                                    PositionInTriple role);
  std::optional<PreprocessedTerm> preprocessVariable(const Variable& variable);
  std::optional<PreprocessedTerm> preprocessBlankNode(
      const BlankNode& blankNode);

  // Preprocess all three terms of a single template triple. Returns
  // `std::nullopt` if any term fails to preprocess (e.g. an unbound variable).
  std::optional<PreprocessedTriple> preprocessTriple(
      const std::array<GraphTerm, NUM_TRIPLE_POSITIONS>& triple);

  ValueId resolveConstantDedupId(TripleComponent tripleComponent);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H
