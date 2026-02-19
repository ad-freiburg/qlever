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

using namespace qlever::constructExport;

// TODO<ms2144>: think about whether we can cache the escaped/formatted
// form directly (alywyas, the same export format is used for the same result).
// I don't really undserstand the comment above, think about what it means.

// _____________________________________________________________________________
std::optional<EvaluatedTerm> ConstructTripleInstantiator::instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowInBatch, size_t blankNodeRowId) {
  return std::visit(
      [&](const auto& t) -> std::optional<EvaluatedTerm> {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return std::make_shared<const std::string>(t.value_);
        } else if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          return batchResult.getVariable(t.columnIndex_, rowInBatch);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return std::make_shared<const std::string>(
              absl::StrCat(t.prefix_, blankNodeRowId, t.suffix_));
        } else {
          static_assert(false, "Unhandled variant type");
        }
      },
      term);
}

// TODO<ms2144>: take the format as runtime parameter (code review comment).
// _____________________________________________________________________________
template <ad_utility::MediaType format>
std::string ConstructTripleInstantiator::formatTriple(
    const EvaluatedTerm& subject, const EvaluatedTerm& predicate,
    const EvaluatedTerm& object) {
  static_assert(format == ad_utility::MediaType::turtle ||
                format == ad_utility::MediaType::csv ||
                format == ad_utility::MediaType::tsv);

  if constexpr (format == ad_utility::MediaType::turtle) {
    // Only escape literals (strings starting with "). IRIs and blank nodes
    // are used as-is, avoiding an unnecessary string copy.
    if (ql::starts_with(*object, "\"")) {
      return absl::StrCat(*subject, " ", *predicate, " ",
                          RdfEscaping::validRDFLiteralFromNormalized(*object),
                          " .\n");
    }
    return absl::StrCat(*subject, " ", *predicate, " ", *object, " .\n");

  } else if constexpr (format == ad_utility::MediaType::csv) {
    return absl::StrCat(RdfEscaping::escapeForCsv(*subject), ",",
                        RdfEscaping::escapeForCsv(*predicate), ",",
                        RdfEscaping::escapeForCsv(*object), "\n");
  } else if constexpr (format == ad_utility::MediaType::tsv) {
    return absl::StrCat(RdfEscaping::escapeForTsv(*subject), "\t",
                        RdfEscaping::escapeForTsv(*predicate), "\t",
                        RdfEscaping::escapeForTsv(*object), "\n");
  } else {
    AD_FAIL();  // unreachable
  }
}

// Explicit instantiations.
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::turtle>(
    const EvaluatedTerm&, const EvaluatedTerm&, const EvaluatedTerm&);
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::csv>(
    const EvaluatedTerm&, const EvaluatedTerm&, const EvaluatedTerm&);
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::tsv>(
    const EvaluatedTerm&, const EvaluatedTerm&, const EvaluatedTerm&);
