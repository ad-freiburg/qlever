// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H

#include <memory>
#include <string>

#include "engine/ConstructTypes.h"
#include "util/http/MediaTypes.h"

// Provides methods for instantiating terms and formatting triples.
class ConstructTripleInstantiator {
 public:
  using EvaluatedTerm = qlever::constructExport::EvaluatedTerm;
  using PreprocessedTerm = qlever::constructExport::PreprocessedTerm;
  using BatchEvaluationResult = qlever::constructExport::BatchEvaluationResult;
  // Instantiates a single preprocessed term for a specific row.
  // For constants: returns the precomputed string.
  // For variables: looks up the batch-evaluated value.
  // For blank nodes: computes the value on the fly using precomputed
  //   prefix/suffix and the blank node row id (rowOffset + actualRowIdx).
  static EvaluatedTerm instantiateTerm(const PreprocessedTerm& term,
                                       const BatchEvaluationResult& batchResult,
                                       size_t rowInBatch,
                                       size_t blankNodeRowId);

  // Formats a triple (subject, predicate, object) according to the output
  // format. Returns empty string if any component is `Undef`.
  template <ad_utility::MediaType format>
  static std::string formatTriple(const EvaluatedTerm& subject,
                                  const EvaluatedTerm& predicate,
                                  const EvaluatedTerm& object);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
