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

// _____________________________________________________________________________
EvaluatedTerm ConstructTripleInstantiator::instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowInBatch, size_t blankNodeRowId) {
  return std::visit(
      [&](const auto& t) -> EvaluatedTerm {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return std::make_shared<const std::string>(t.value_);
        } else if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          if (!t.columnIndex_.has_value()) {
            return Undef{};
          }
          return batchResult.getVariable(*t.columnIndex_, rowInBatch);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return std::make_shared<const std::string>(
              absl::StrCat(t.prefix_, blankNodeRowId, t.suffix_));
        } else {
          static_assert(false, "Unhandled variant type");
        }
      },
      term);
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
std::string ConstructTripleInstantiator::formatTriple(
    const EvaluatedTerm& subject, const EvaluatedTerm& predicate,
    const EvaluatedTerm& object) {
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
    const EvaluatedTerm&, const EvaluatedTerm&, const EvaluatedTerm&);
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::csv>(
    const EvaluatedTerm&, const EvaluatedTerm&, const EvaluatedTerm&);
template std::string
ConstructTripleInstantiator::formatTriple<ad_utility::MediaType::tsv>(
    const EvaluatedTerm&, const EvaluatedTerm&, const EvaluatedTerm&);
