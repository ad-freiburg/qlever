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
// --- Evaluation types ---

// Result of evaluating a term.
using EvaluatedTerm = std::shared_ptr<const std::string>;

// Result of batch-evaluating variables for a batch of rows. Stores evaluated
// values indexed by `IdTable` column index, enabling efficient lookup during
// triple instantiation.
struct BatchEvaluationResult {
  // Map from `IdTable` column index to evaluated values for each row in batch.
  // Each entry is `std::nullopt` if the variable evaluation failed for that
  // row.
  ::ad_utility::HashMap<size_t, std::vector<std::optional<EvaluatedTerm>>>
      variablesByColumn_;
  size_t numRows_ = 0;

  const std::optional<EvaluatedTerm>& getVariable(size_t columnIndex,
                                                  size_t rowInBatch) const {
    return variablesByColumn_.at(columnIndex).at(rowInBatch);
  }
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
