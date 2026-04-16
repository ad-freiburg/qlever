// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H

#include <array>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace qlever::constructExport {

// Canonical representation of a resolved RDF term stored in the LRU cache.
//
// Two fundamentally different representations are used, distinguished by
// whether `type` is null:
// 1) type != nullptr: `str` represents an encoded literal (directly encoded
// into `ValueId`). `str` is the raw unquoted value (e.g. "42" for an xsd:int,
// "3.14" for an xsd:decimal). `type` points to the compile-time XSD type string
// constant (e.g. XSD_INT_TYPE).  Whether to emit the short form ("42") or the
// fully-qualified form ("\"42\"^^<xsd:integer>") is decided at formatting time
// by formatTerm.
// 2)  type == nullptr: an IRI, a blank node, or a vocabulary-indexed literal.
// `str` already holds the complete, ready-to-emit serialized
// form (e.g. "<http://example.org/>", "\"hello\"@en"). No further formatting is
// needed; the value is returned as-is for every format.
// This is the legacy format returned by `ExportIds::idToStringAndType`.
struct EvaluatedTermData {
  std::string rdfTermString;
  const char* rdfTermDataType;  // non-null iff encoded literal (case 1 above)
};

// Shared ownership of `EvaluatedTermData`. The shared_ptr allows cheap copying
// when the same `Id` appears in multiple rows or is reused from the `IdCache`.
using EvaluatedTerm = std::shared_ptr<const EvaluatedTermData>;

// A constant (`Iri` or `Literal`) whose string value is fully known at
// preprocessing time. The `EvaluatedTerm` is built once at preprocessing and
// shared across all rows, avoiding per-row heap allocation.
struct PrecomputedConstant {
  EvaluatedTerm evaluatedTerm_;
};

// After preprocessing (via `ConstructTemplatePreprocessor::preprocess`),
// `columnIndex_` is the position of this variable in the
// `BatchEvaluationResult::variablesByColumn_` vector. The mapping from position
// to column is recorded in
// `PreprocessedConstructTemplate::uniqueVariableColumns_`.
struct PrecomputedVariable {
  size_t columnIndex_;
};

// A blank node with precomputed prefix and suffix for fast evaluation. The
// blank node format is: prefix + rowNumber + suffix, where prefix is "_:g" or
// "_:u" (generated or user-defined) and suffix is "_" + label. This avoids
// recomputing these constant parts for every result table row.
struct PrecomputedBlankNode {
  std::string prefix_;  // "_:g" or "_:u"
  std::string suffix_;  // "_" + label
};

// A single preprocessed term position in a CONSTRUCT template triple. The
// variant type encodes what kind of term it is and holds all precomputed data
// needed for later evaluation.
using PreprocessedTerm = std::variant<PrecomputedConstant, PrecomputedVariable,
                                      PrecomputedBlankNode>;

// Number of positions in a triple: subject, predicate, object.
inline constexpr size_t NUM_TRIPLE_POSITIONS = 3;

// A single preprocessed CONSTRUCT template triple.
using PreprocessedTriple = std::array<PreprocessedTerm, NUM_TRIPLE_POSITIONS>;

// Result of preprocessing all CONSTRUCT template triples. Contains the
// preprocessed triples and the unique variable column indices (indices into the
// `IdTable` that the variables in the construct template correspond to).
struct PreprocessedConstructTemplate {
  std::vector<PreprocessedTriple> preprocessedTriples_;
  // The dedupicated set of `IdTable` column indices that appear in the template
  // triples, in order of first encounter.
  std::vector<size_t> uniqueVariableColumns_;
};

// Result of instantiating a single template triple for a specific result table
// row.
struct EvaluatedTriple {
  EvaluatedTerm subject_;
  EvaluatedTerm predicate_;
  EvaluatedTerm object_;
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
