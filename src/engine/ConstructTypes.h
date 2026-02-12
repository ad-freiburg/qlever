// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H

#include <array>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "util/HashMap.h"

// --- Preprocessing types ---

// A constant (`Iri` or `Literal`) whose string value is fully known at
// preprocessing time.
struct PrecomputedConstant {
  std::string value_;
};

// A variable: we precompute which `IdTable` column to look up at query time.
// `columnIndex_` is `std::nullopt` if the variable does not appear in the
// result table (`IdTable`). (i.e., the variable is used in the CONSTRUCT
// template but not bound by the WHERE clause).
struct PrecomputedVariable {
  std::optional<size_t> columnIndex_;
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
// evaluated at query time.
struct PreprocessedConstructTemplate {
  std::vector<PreprocessedTriple> preprocessedTriples_;
  std::vector<size_t> uniqueVariableColumns_;
};

// --- Evaluation types ---

// Tag type representing an unbound variable (UNDEF in SPARQL).
struct Undef {};

// Result of evaluating a term.
using EvaluatedTerm = std::variant<Undef, std::shared_ptr<const std::string>>;

// Result of instantiating a single template triple for a specific row. Contains
// the resolved string values for subject, predicate, and object. Each component
// is either Undef (variable unbound) or a valid string.
struct InstantiatedTriple {
  EvaluatedTerm subject_;
  EvaluatedTerm predicate_;
  EvaluatedTerm object_;

  // Returns true if all three components have values (not Undef).
  bool isComplete() const {
    return !std::holds_alternative<Undef>(subject_) &&
           !std::holds_alternative<Undef>(predicate_) &&
           !std::holds_alternative<Undef>(object_);
  }

  // Get string value for a component. Precondition: component is not Undef.
  static const std::string& getValue(const EvaluatedTerm& var) {
    return *std::get<std::shared_ptr<const std::string>>(var);
  }
};

// Result of batch-evaluating variables for a batch of rows. Stores evaluated
// values indexed by `IdTable` column index, enabling efficient lookup during
// triple instantiation.
struct BatchEvaluationResult {
  // Map from `IdTable` column index to evaluated values for each row in batch.
  ad_utility::HashMap<size_t, std::vector<EvaluatedTerm>> variablesByColumn_;
  size_t numRows_ = 0;

  const EvaluatedTerm& getVariable(size_t columnIndex,
                                   size_t rowInBatch) const {
    return variablesByColumn_.at(columnIndex).at(rowInBatch);
  }
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
