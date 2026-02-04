// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_TRIPLEINSTANTIATIONCONTEXT_H
#define QLEVER_SRC_ENGINE_TRIPLEINSTANTIATIONCONTEXT_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "engine/VariableToColumnMap.h"
#include "global/Constants.h"
#include "rdfTypes/Variable.h"
#include "util/CancellationHandle.h"

// Forward declarations
class Index;

// Number of positions in a triple: subject, predicate, object.
inline constexpr size_t NUM_TRIPLE_POSITIONS = 3;

// Identifies the source of a term's value during triple instantiation.
enum class TermType { CONSTANT, VARIABLE, BLANK_NODE };

// Describes how to look up the value for a single term position (subject,
// predicate, or object) during triple instantiation.
//
// - `type`: Indicates whether the term is a CONSTANT (precomputed IRI/Literal),
//   VARIABLE (looked up from IdTable), or BLANK_NODE (generated per row).
// - `index`: The index into the corresponding cache:
//   - For CONSTANT: index into `precomputedConstants_[tripleIdx]`
//   - For VARIABLE: index into `variablesToEvaluate_` / `variableStrings_`
//   - For BLANK_NODE: index into `blankNodesToEvaluate_` / `blankNodeValues_`
struct TermLookupInfo {
  TermType type;
  size_t index;
};

// Pre-analyzed info for a triple pattern to enable fast instantiation.
struct TriplePatternInfo {
  std::array<TermLookupInfo, NUM_TRIPLE_POSITIONS> lookups_;
};

// Variable with pre-computed column index into the `IdTable`.
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

// Cache for batch-evaluated `Variable` objects and `BlankNode` objects.
// This stores the results of evaluating all variables and blank nodes
// for a batch of rows, enabling efficient lookup during triple instantiation.
struct BatchEvaluationCache {
  // maps: variable idx -> idx of row in batch -> shared_ptr<string>
  std::vector<std::vector<std::shared_ptr<const std::string>>> variableStrings_;
  // maps: blank node idx -> idx of row in batch -> string value
  std::vector<std::vector<std::string>> blankNodeValues_;
  size_t numRows_ = 0;

  // Get shared_ptr for a specific variable at a row in the batch.
  const std::shared_ptr<const std::string>& getVariableString(
      size_t varIdx, size_t rowInBatch) const {
    return variableStrings_[varIdx][rowInBatch];
  }

  // Get string for a specific blank node at a row in the batch.
  const std::string& getBlankNodeValue(size_t blankNodeIdx,
                                       size_t rowInBatch) const {
    return blankNodeValues_[blankNodeIdx][rowInBatch];
  }
};

// Bundles all pre-analyzed template data needed for batch processing.
// This context is created once by ConstructTripleGenerator during template
// analysis and shared (immutably) with all ConstructBatchProcessor instances.
struct TripleInstantiationContext {
  // Constructor taking required references.
  TripleInstantiationContext(
      const Index& index, const VariableToColumnMap& variableColumns,
      ad_utility::SharedCancellationHandle cancellationHandle)
      : index_(index),
        variableColumns_(variableColumns),
        cancellationHandle_(std::move(cancellationHandle)) {}

  // Pre-analyzed info for each triple pattern (resolutions + skip flag).
  std::vector<TriplePatternInfo> triplePatternInfos_;

  // Precomputed constant values for `Iri` objects and `Literal` objects.
  // [tripleIdx][position] -> constant string (empty if not a constant)
  std::vector<std::array<std::string, NUM_TRIPLE_POSITIONS>>
      precomputedConstants_;

  // Ordered list of `Variable` objects with pre-computed column indices for
  // evaluation.
  std::vector<VariableWithColumnIndex> variablesToEvaluate_;

  // Ordered list of `BlankNode` objects with precomputed format info for
  // evaluation (index corresponds to cache index).
  std::vector<BlankNodeFormatInfo> blankNodesToEvaluate_;

  // Reference to the `Index` for vocabulary lookups.
  std::reference_wrapper<const Index> index_;

  // Map from `Variable` objects to the column idx of the `IdTable`.
  std::reference_wrapper<const VariableToColumnMap> variableColumns_;

  // Handle for checking query cancellation.
  ad_utility::SharedCancellationHandle cancellationHandle_;

  // Default batch size for processing rows.
  static constexpr size_t DEFAULT_BATCH_SIZE = 64;

  static size_t getBatchSize() { return DEFAULT_BATCH_SIZE; }

  // Number of template triples.
  size_t numTemplateTriples() const { return triplePatternInfos_.size(); }
};

#endif  // QLEVER_SRC_ENGINE_TRIPLEINSTANTIATIONCONTEXT_H
