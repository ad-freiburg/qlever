// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "engine/ConstructTypes.h"
#include "engine/VariableToColumnMap.h"
#include "global/Constants.h"
#include "parser/data/BlankNode.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/Iri.h"
#include "parser/data/Literal.h"
#include "parser/data/Types.h"
#include "util/CancellationHandle.h"
#include "util/HashMap.h"

// Forward declarations
class Index;

// Result of preprocessing the construct template triples.
struct PreprocessedConstructTemplate {
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  PreprocessedConstructTemplate(const Index& index,
                                const VariableToColumnMap& variableColumns,
                                CancellationHandle cancellationHandle)
      : index_(index),
        variableColumns_(variableColumns),
        cancellationHandle_(std::move(cancellationHandle)) {}

  // Lookup info for each triple pattern.
  std::vector<TripleInstantitationRecipe> triplePatternInfos_;

  // Precomputed constant values for `Iri` objects and `Literal` objects.
  // [tripleIdx][positionInTriple] -> constant string (empty if not a constant)
  std::vector<std::array<std::string, NUM_TRIPLE_POSITIONS>>
      precomputedConstants_;

  // Ordered list of `Variable` objects with pre-computed column indices which
  // are later needed for instantiation of the `Variable` objects with actual
  // values from the result table rows.
  std::vector<VariableWithColumnIndex> variablesToInstantiate_;

  // Ordered list of `BlankNode` objects with precomputed format info.
  std::vector<BlankNodeFormatInfo> blankNodesToInstantiate_;

  // Reference to the `Index` for vocabulary lookups.
  std::reference_wrapper<const Index> index_;

  // Map from `Variable` objects to the column idx of the `IdTable` where the
  // values for that `Variable` are contained in.
  std::reference_wrapper<const VariableToColumnMap> variableColumns_;

  // Handle for checking query cancellation.
  CancellationHandle cancellationHandle_;

  // Number of template triples.
  size_t numTemplateTriples() const { return triplePatternInfos_.size(); }
};

// _____________________________________________________________________________
// Preprocesses CONSTRUCT template triples to create a
// `PreprocessedConstructTemplate`. For each term position, determines how to
// obtain its value:
// - Constants (`Iri`s/`Literal`s): evaluates and stores the string
// - `Variables`: precomputes column indices for `IdTable` lookup.
// - `BlankNode`s: precomputes format prefix/suffix
class ConstructTemplatePreprocessor {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using Triples = ad_utility::sparql_types::Triples;
  using enum PositionInTriple;

  // Main entry point: preprocess template triples and return the result.
  static std::shared_ptr<PreprocessedConstructTemplate> preprocess(
      const Triples& templateTriples, const Index& index,
      const VariableToColumnMap& variableColumns,
      CancellationHandle cancellationHandle);

 private:
  // Analyzes a single term and returns its resolution info.
  // Dispatches to the appropriate type-specific handler based on term type.
  static TripleInstantitationRecipe::TermInstantitationRecipe preprocessTerm(
      const GraphTerm& term, size_t tripleIdx, PositionInTriple role,
      PreprocessedConstructTemplate& result,
      ad_utility::HashMap<Variable, size_t>& variableToIndex,
      ad_utility::HashMap<std::string, size_t>& blankNodeLabelToIndex,
      const VariableToColumnMap& variableColumns);

  // Analyzes a `Iri` term: precomputes the string value.
  static TripleInstantitationRecipe::TermInstantitationRecipe preprocessIriTerm(
      const Iri& iri, size_t tripleIdx, size_t pos,
      PreprocessedConstructTemplate& result);

  // Analyzes a `Literal` term: precomputes the string value.
  static TripleInstantitationRecipe::TermInstantitationRecipe
  preprocessLiteralTerm(const Literal& literal, size_t tripleIdx,
                        PositionInTriple role,
                        PreprocessedConstructTemplate& result);

  // Analyzes a `Variable` term: registers it and precomputes `IdTable` column
  // index.
  static TripleInstantitationRecipe::TermInstantitationRecipe
  preprocessVariableTerm(const Variable& var,
                         PreprocessedConstructTemplate& result,
                         ad_utility::HashMap<Variable, size_t>& variableToIndex,
                         const VariableToColumnMap& variableColumns);

  // Analyzes a `BlankNode` term: registers it and precomputes format strings.
  static TripleInstantitationRecipe::TermInstantitationRecipe
  preprocessBlankNodeTerm(
      const BlankNode& blankNode, PreprocessedConstructTemplate& result,
      ad_utility::HashMap<std::string, size_t>& blankNodeLabelToIndex);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H
