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

#include "rdfTypes/Variable.h"

// Tag type representing an unbound variable (UNDEF in SPARQL).
struct Undef {};

// Result of instantiation a term.
using InstantiatedTerm =
    std::variant<Undef, std::shared_ptr<const std::string>>;

// Number of positions in a triple: subject, predicate, object.
inline constexpr size_t NUM_TRIPLE_POSITIONS = 3;

// This struct specifies how to instantiate a template triple of the construct
// graph template. In more detail: This struct contains a
// `TermInstantiationSpec`, which specifies where the corresponding value which
// the term is to be instantiated with.
struct TemplateTripleLookupSpec {
  enum class TermType { CONSTANT, VARIABLE, BLANK_NODE };

  // Specifies how to look up the value for a term position
  // during triple instantiation.
  // `type`: Indicates whether the term is a CONSTANT, VARIABLE, or BLANK_NODE.
  // `index`: The index into the corresponding storage:
  // For CONSTANT: index into `precomputedConstants_[tripleIdx]`
  // For VARIABLE: index into `variablesToInstantiate_` /
  // `variableInstantiations_` For BLANK_NODE: index into
  // `blankNodesToInstantiate_` / `instantiatedBlankNodes`
  struct TermInstantiationSpec {
    TermType type;
    size_t index;
  };

  std::array<TermInstantiationSpec, NUM_TRIPLE_POSITIONS> lookups_;
};

// Variable with column index into the `IdTable`.
struct VariableWithColumnIndex {
  Variable variable_;
  // idx of the column for the variable in the `IdTable`.
  std::optional<size_t> columnIndex_;
};

// `BlankNode` with precomputed prefix and suffix for fast evaluation.
// The blank node format is: prefix + rowNumber + suffix
// where prefix is "_:g" or "_:u" and suffix_ is "_" + label.
// This avoids recomputing these constant parts for every result table row.
struct BlankNodeFormatInfo {
  std::string prefix_;  // "_:g" or "_:u"
  std::string suffix_;  // "_" + label
};

// Result of instantiating a single template triple for a specific row.
// Contains the resolved string values for subject, predicate, and object.
// Each component is either Undef (variable unbound) or a valid string.
struct InstantiatedTriple {
  InstantiatedTerm subject_;
  InstantiatedTerm predicate_;
  InstantiatedTerm object_;

  // Returns true if all three components have values (not Undef).
  bool isComplete() const {
    return !std::holds_alternative<Undef>(subject_) &&
           !std::holds_alternative<Undef>(predicate_) &&
           !std::holds_alternative<Undef>(object_);
  }

  // Get string value for a component. Precondition: component is not Undef.
  static const std::string& getValue(const InstantiatedTerm& var) {
    return *std::get<std::shared_ptr<const std::string>>(var);
  }
};

// Result of batch-evaluating `Variable` objects and `BlankNode` objects.
// This stores the results of evaluating all variables and blank nodes
// for a batch of rows, enabling efficient lookup during triple instantiation.
struct BatchEvaluationResult {
  // maps: variable idx -> idx of row in batch -> InstantiatedTerm
  std::vector<std::vector<InstantiatedTerm>> variableInstantiations_;
  // maps: blank node idx -> idx of row in batch -> string value
  std::vector<std::vector<std::string>> instantiatedBlankNodes;
  size_t numRows_ = 0;

  // Get the evaluated variable for a specific variable at a row in the batch.
  const InstantiatedTerm& getEvaluatedVariable(size_t varIdx,
                                               size_t rowInBatch) const {
    return variableInstantiations_[varIdx][rowInBatch];
  }

  // Get string for a specific blank node at a row in the batch.
  const std::string& getBlankNodeValue(size_t blankNodeIdx,
                                       size_t rowInBatch) const {
    return instantiatedBlankNodes[blankNodeIdx][rowInBatch];
  }
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
