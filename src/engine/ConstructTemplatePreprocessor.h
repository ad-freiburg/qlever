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
#include "parser/data/Types.h"

// Preprocesses CONSTRUCT template triples. For each term, precomputes
// the following:
// - constants (IRIs/literals): evaluates and stores the string value.
// - variables: precomputes the column index into the `IdTable`.
// - blank nodes: precomputes the format prefix/suffix.
class ConstructTemplatePreprocessor {
 public:
  using Triples = ad_utility::sparql_types::Triples;
  using PreprocessedConstructTemplate =
      qlever::constructExport::PreprocessedConstructTemplate;

  // Preprocess the template triples. Returns the preprocessed triples together
  // with the unique variable column indices needed when evaluating the template
  // triples for specific result-table rows.
  static PreprocessedConstructTemplate preprocess(
      const Triples& templateTriples,
      const VariableToColumnMap& variableColumns);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTEMPLATEPREPROCESSOR_H
