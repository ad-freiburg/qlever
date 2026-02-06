// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"

// _____________________________________________________________________________
InstantiatedTerm ConstructTripleInstantiator::instantiateTerm(
    size_t tripleIdx, size_t pos,
    const PreprocessedConstructTemplate& preprocessedTemplate,
    const BatchEvaluationResult& batchResult, size_t rowIdxInBatch) {
  using enum TripleInstantitationRecipe::TermType;

  const TripleInstantitationRecipe& info =
      preprocessedTemplate.triplePatternInfos_[tripleIdx];
  const TripleInstantitationRecipe::TermInstantitationRecipe& lookup =
      info.lookups_[pos];

  switch (lookup.type_) {
    case CONSTANT: {
      return std::make_shared<std::string>(
          preprocessedTemplate.precomputedConstants_[tripleIdx][pos]);
    }
    case VARIABLE: {
      // Variable values are stored in the batch result.
      // May be Undef if the variable is unbound.
      return batchResult.getEvaluatedVariable(lookup.index_, rowIdxInBatch);
    }
    case BLANK_NODE: {
      // Blank node values are always valid (computed for each row).
      return std::make_shared<const std::string>(
          batchResult.getBlankNodeValue(lookup.index_, rowIdxInBatch));
    }
  }
  AD_FAIL();  // Unreachable: all enum cases handled above.
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
std::string ConstructTripleInstantiator::formatTriple(
    const InstantiatedTerm& subject, const InstantiatedTerm& predicate,
    const InstantiatedTerm& object) {
  static_assert(format == ad_utility::MediaType::turtle ||
                format == ad_utility::MediaType::csv ||
                format == ad_utility::MediaType::tsv);

  // Return empty string if any component is Undef.
  if (std::holds_alternative<Undef>(subject) ||
      std::holds_alternative<Undef>(predicate) ||
      std::holds_alternative<Undef>(object)) {
    return {};
  }

  const auto& subjectStr =
      *std::get<std::shared_ptr<const std::string>>(subject);
  const auto& predicateStr =
      *std::get<std::shared_ptr<const std::string>>(predicate);
  const auto& objectStr = *std::get<std::shared_ptr<const std::string>>(object);

  if constexpr (format == ad_utility::MediaType::turtle) {
    // Only escape literals (strings starting with "). IRIs and blank nodes
    // are used as-is, avoiding an unnecessary string copy.
    if (ql::starts_with(objectStr, "\"")) {
      return absl::StrCat(subjectStr, " ", predicateStr, " ",
                          RdfEscaping::validRDFLiteralFromNormalized(objectStr),
                          " .\n");
    }
    return absl::StrCat(subjectStr, " ", predicateStr, " ", objectStr, " .\n");

  } else if constexpr (format == ad_utility::MediaType::csv) {
    return absl::StrCat(RdfEscaping::escapeForCsv(subjectStr), ",",
                        RdfEscaping::escapeForCsv(predicateStr), ",",
                        RdfEscaping::escapeForCsv(objectStr), "\n");
  } else if constexpr (format == ad_utility::MediaType::tsv) {
    return absl::StrCat(RdfEscaping::escapeForTsv(subjectStr), "\t",
                        RdfEscaping::escapeForTsv(predicateStr), "\t",
                        RdfEscaping::escapeForTsv(objectStr), "\n");
  } else {
    AD_FAIL();  // unreachable
  }
}

// Explicit instantiations.
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::turtle>(
    const InstantiatedTerm&, const InstantiatedTerm&, const InstantiatedTerm&);
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::csv>(
    const InstantiatedTerm&, const InstantiatedTerm&, const InstantiatedTerm&);
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::tsv>(
    const InstantiatedTerm&, const InstantiatedTerm&, const InstantiatedTerm&);
