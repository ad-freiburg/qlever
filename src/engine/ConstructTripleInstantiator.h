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
  // Instantiates a single term from the template triple, returning its string
  // value. Uses the preprocessed template data and batch evaluation cache.
  // `tripleIdx`: index of the triple in the template
  // `pos`: position within the triple (0=subject, 1=predicate, 2=object)
  // `rowIdxInBatch`: which row in the current batch
  static std::shared_ptr<const std::string> instantiateTerm(
      size_t tripleIdx, size_t pos,
      const PreprocessedConstructTemplate& preprocessedTemplate,
      const BatchEvaluationCache& batchCache, size_t rowIdxInBatch);

  // Formats a triple (subject, predicate, object) according to the output
  // format. Returns empty string if any component is null.
  static std::string formatTriple(
      const std::shared_ptr<const std::string>& subject,
      const std::shared_ptr<const std::string>& predicate,
      const std::shared_ptr<const std::string>& object,
      ad_utility::MediaType format);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
