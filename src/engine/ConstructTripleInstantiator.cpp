// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "rdfTypes/RdfEscaping.h"

// _____________________________________________________________________________
std::shared_ptr<const std::string> ConstructTripleInstantiator::instantiateTerm(
    size_t tripleIdx, size_t pos,
    const PreprocessedConstructTemplate& preprocessedTemplate,
    const BatchEvaluationCache& batchCache, size_t rowIdxInBatch) {
  const TemplateTripleLookupSpec& info =
      preprocessedTemplate.triplePatternInfos_[tripleIdx];
  const TemplateTripleLookupSpec::TermInstantiationSpec& lookup =
      info.lookups_[pos];

  switch (lookup.type) {
    case TemplateTripleLookupSpec::TermType::CONSTANT: {
      return std::make_shared<std::string>(
          preprocessedTemplate.precomputedConstants_[tripleIdx][pos]);
    }
    case TemplateTripleLookupSpec::TermType::VARIABLE: {
      // Variable shared_ptr are stored in the batch cache, eliminating
      // hash lookups during instantiation.
      return batchCache.getVariableString(lookup.index, rowIdxInBatch);
    }
    case TemplateTripleLookupSpec::TermType::BLANK_NODE: {
      // Blank node values are always valid (computed for each row).
      return std::make_shared<const std::string>(
          batchCache.getBlankNodeValue(lookup.index, rowIdxInBatch));
    }
  }
  // TODO<ms2144>: I do not think it is good to ever return a nullptr.
  // We should probably throw an exception here or sth.
  return nullptr;  // Unreachable
}

// _____________________________________________________________________________
std::string ConstructTripleInstantiator::formatTriple(
    const std::shared_ptr<const std::string>& subject,
    const std::shared_ptr<const std::string>& predicate,
    const std::shared_ptr<const std::string>& object,
    ad_utility::MediaType format) {
  if (!subject || !predicate || !object) {
    return {};
  }

  switch (format) {
    case ad_utility::MediaType::turtle: {
      // Only escape literals (strings starting with "). IRIs and blank nodes
      // are used as-is, avoiding an unnecessary string copy.
      if (ql::starts_with(*object, "\"")) {
        return absl::StrCat(*subject, " ", *predicate, " ",
                            RdfEscaping::validRDFLiteralFromNormalized(*object),
                            " .\n");
      }
      return absl::StrCat(*subject, " ", *predicate, " ", *object, " .\n");
    }
    case ad_utility::MediaType::csv: {
      return absl::StrCat(RdfEscaping::escapeForCsv(*subject), ",",
                          RdfEscaping::escapeForCsv(*predicate), ",",
                          RdfEscaping::escapeForCsv(*object), "\n");
    }
    case ad_utility::MediaType::tsv: {
      return absl::StrCat(RdfEscaping::escapeForTsv(*subject), "\t",
                          RdfEscaping::escapeForTsv(*predicate), "\t",
                          RdfEscaping::escapeForTsv(*object), "\n");
    }
    default:
      return {};  // TODO<ms2144>: add proper error throwing here?
  }
}
