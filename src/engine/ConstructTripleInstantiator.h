// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H

#include <memory>
#include <string>

#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTypes.h"
#include "util/http/MediaTypes.h"

// _____________________________________________________________________________
// Provides methods for instantiating terms and formatting triples.
class ConstructTripleInstantiator {
 public:
  // Instantiates a single term from the template triple.
  // Returns Undef if the term is a variable that is unbound.
  // Uses the preprocessed template data and batch evaluation result.
  // `tripleIdx`: index of the triple in the template
  // `pos`: position within the triple (0=subject, 1=predicate, 2=object)
  // `rowIdxInBatch`: which row in the current batch
  static InstantiatedVariable instantiateTerm(
      size_t tripleIdx, size_t pos,
      const PreprocessedConstructTemplate& preprocessedTemplate,
      const BatchEvaluationResult& batchResult, size_t rowIdxInBatch);

  // Formats a triple (subject, predicate, object) according to the output
  // format. Returns empty string if any component is Undef.
  static std::string formatTriple(const InstantiatedVariable& subject,
                                  const InstantiatedVariable& predicate,
                                  const InstantiatedVariable& object,
                                  ad_utility::MediaType format);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
