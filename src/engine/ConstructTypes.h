// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H

#include <array>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace qlever::constructExport {

// A constant (`Iri` or `Literal`) whose string value is fully known at
// preprocessing time.
struct PrecomputedConstant {
  std::string value_;
};

// We precompute which `IdTable` column to look up at construct query triple
// instantitation time.
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
// preprocessed triples and the unique variable column indices that need to be
// evaluated for each row of the result-table.
struct PreprocessedConstructTemplate {
  std::vector<PreprocessedTriple> preprocessedTriples_;
  std::vector<size_t> uniqueVariableColumns_;
};

// Result of evaluating a term (`Iri`, `Literal`, `Variable`, `BlankNode`) to
// its string representation.
using EvaluatedTerm = std::shared_ptr<const std::string>;

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
