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
#include "engine/QueryExecutionTree.h"
#include "util/http/MediaTypes.h"

// _____________________________________________________________________________
// Creates output triples from evaluated batch data.
// Provides methods for both formatted string output and StringTriple output.
class ConstructTripleInstantiator {
 public:
  using StringTriple = QueryExecutionTree::StringTriple;

  // Gets the shared_ptr to the string resulting from evaluating the term
  // specified by `tripleIdx` (idx of the triple in the template triples) and
  // `pos` (position of the term in said template triple) on the row of the
  // WHERE-clause result table specified by `rowIdxInBatch`.
  static std::shared_ptr<const std::string> instantiateTerm(
      size_t tripleIdx, size_t pos,
      const PreprocessedConstructTemplate& preprocessedTemplate,
      const BatchEvaluationCache& batchCache, size_t rowIdxInBatch);

  // Formats a single triple according to the output format. Returns empty
  // string if any component is UNDEF.
  static std::string formatTriple(
      const std::shared_ptr<const std::string>& subject,
      const std::shared_ptr<const std::string>& predicate,
      const std::shared_ptr<const std::string>& object,
      ad_utility::MediaType format);

  // Instantiates a single triple as StringTriple. Returns empty `StringTriple`
  // if any component is UNDEF.
  static StringTriple instantiateTriple(
      const std::shared_ptr<const std::string>& subject,
      const std::shared_ptr<const std::string>& predicate,
      const std::shared_ptr<const std::string>& object);
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEINSTANTIATOR_H
