// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H

#include <memory>
#include <string>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "util/http/MediaTypes.h"

namespace qlever::constructExport {

// Provides methods for instantiating terms and formatting triples.
class ConstructTripleInstantiator {
 public:
  // Instantiates a single preprocessed term for a specific row.
  // For constants: returns the precomputed string.
  // For variables: looks up the batch-evaluated value.
  // For blank nodes: computes the value on the fly using precomputed
  //   prefix/suffix and the blank node row id (rowOffset + actualRowIdx).
  static std::optional<EvaluatedTerm> instantiateTerm(
      const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
      size_t rowInBatch, size_t blankNodeRowId);

  // Formats a triple (subject, predicate, object) according to the output
  // format. Returns empty string if any component is `Undef`.
  static std::string formatTriple(const EvaluatedTerm& subject,
                                  const EvaluatedTerm& predicate,
                                  const EvaluatedTerm& object,
                                  const ad_utility::MediaType& format);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
