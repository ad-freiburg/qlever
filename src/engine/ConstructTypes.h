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

// Result of evaluating a term.
using InstantiatedTerm =
    std::variant<Undef, std::shared_ptr<const std::string>>;

// Number of positions in a triple: subject, predicate, object.
inline constexpr size_t NUM_TRIPLE_POSITIONS = 3;

// Specifies how to look up the value for each term position in a triple during
// triple instantiation.
// `type`: Indicates whether the term is a CONSTANT, VARIABLE, or BLANK_NODE.
// `index`: The idx into the corresponding storage depending on `type`
// CONSTANT: `precomputedConstants_[tripleIdx]`
// VARIABLE: `variablesToInstantiate_` / `variableInstantiations_`
// BLANK_NODE: `blankNodesToInstantiate_` / `blankNodeInstantiations_`.
struct TripleInstantitationRecipe {
  enum class TermType { CONSTANT, VARIABLE, BLANK_NODE };

  struct TermInstantitationRecipe {
    TermType type_;
    size_t index_;
  };

  std::array<TermInstantitationRecipe, NUM_TRIPLE_POSITIONS> lookups_;
};

// Variable with column index into the `IdTable`.
struct VariableWithColumnIndex {
  Variable variable_;
  // idx of the column for the variable in the `IdTable`.
  std::optional<size_t> columnIndex_;
};

// `BlankNode` with precomputed prefix and suffix for fast evaluation. The blank
// node format is: prefix + rowNumber + suffix where prefix is "_:g" or "_:u"
// and suffix_ is "_" + label. This avoids recomputing these constant parts for
// every result table row.
struct BlankNodeFormatInfo {
  std::string prefix_;  // "_:g" or "_:u"
  std::string suffix_;  // "_" + label
};

// Result of instantiating a single template triple for a specific row. Contains
// the resolved string values for subject, predicate, and object. Each component
// is either Undef (variable unbound) or a valid string.
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

// Column-major storage for batch instantiation results. Each column holds the
// instantiated values for one entity (a particular variable or blank node)
// across all rows in the batch.
template <typename T>
class BatchInstantiations {
 public:
  // Resize to `numColumns` columns, each with `numRows` default-constructed
  // elements.
  void resize(size_t numColumns, size_t numRows) {
    columns_.resize(numColumns);
    for (auto& column : columns_) {
      column.resize(numRows);
    }
  }

  // Resize to `numColumns` columns, each with `numRows` elements initialized
  // to `defaultValue`.
  void resize(size_t numColumns, size_t numRows, const T& defaultValue) {
    columns_.resize(numColumns);
    for (auto& column : columns_) {
      column.resize(numRows, defaultValue);
    }
  }

  // Access a specific column for writing.
  std::vector<T>& operator[](size_t columnIdx) { return columns_[columnIdx]; }

  // Read a specific element.
  const T& get(size_t columnIdx, size_t rowInBatch) const {
    return columns_[columnIdx][rowInBatch];
  }

 private:
  std::vector<std::vector<T>> columns_;
};

// Result of batch-evaluating `Variable` objects and `BlankNode` objects. This
// stores the results of evaluating all variables and blank nodes for a batch of
// rows, enabling efficient lookup during triple instantiation.
struct BatchEvaluationResult {
  BatchInstantiations<InstantiatedTerm> variableInstantiations_;
  BatchInstantiations<std::string> blankNodeInstantiations_;
  size_t numRows_ = 0;

  // Get the evaluated variable for a specific variable at a row in the batch.
  const InstantiatedTerm& getEvaluatedVariable(size_t varIdx,
                                               size_t rowInBatch) const {
    return variableInstantiations_.get(varIdx, rowInBatch);
  }

  // Get string for a specific blank node at a row in the batch.
  const std::string& getBlankNodeValue(size_t blankNodeIdx,
                                       size_t rowInBatch) const {
    return blankNodeInstantiations_.get(blankNodeIdx, rowInBatch);
  }
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTYPES_H
