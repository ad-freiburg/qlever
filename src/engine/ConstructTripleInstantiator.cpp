// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/Views.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
std::optional<EvaluatedTerm> instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowInBatch, size_t blankNodeRowId) {
  return std::visit(
      [&](const auto& t) -> std::optional<EvaluatedTerm> {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return t.evaluatedTerm_;
        } else if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          return batchResult.getVariable(t.columnIndex_, rowInBatch);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return std::make_shared<const EvaluatedTermData>(
              absl::StrCat(t.prefix_, blankNodeRowId, t.suffix_), nullptr);
        } else {
          static_assert(false, "Unhandled variant type");
        }
      },
      term);
}

// _____________________________________________________________________________
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t blankNodeBaseId) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(batchResult.numRows_ * tmpl.preprocessedTriples_.size());

  for (size_t rowInBatch : ql::views::iota(size_t{0}, batchResult.numRows_)) {
    const size_t blankNodeRowId = blankNodeBaseId + rowInBatch;
    for (const auto& triple : tmpl.preprocessedTriples_) {
      auto subject =
          instantiateTerm(triple[0], batchResult, rowInBatch, blankNodeRowId);
      auto predicate =
          instantiateTerm(triple[1], batchResult, rowInBatch, blankNodeRowId);
      auto object =
          instantiateTerm(triple[2], batchResult, rowInBatch, blankNodeRowId);
      if (subject && predicate && object) {
        triples.push_back(EvaluatedTriple{*subject, *predicate, *object});
      }
    }
  }
  return triples;
}

// _____________________________________________________________________________
std::string formatTerm(const EvaluatedTermData& term, bool shortForm) {
  if (term.type == nullptr) {
    // IRI, blank node, or vocab-indexed literal: already in final form.
    return term.str;
  }
  const char* i = XSD_INT_TYPE;
  const char* d = XSD_DECIMAL_TYPE;
  const char* b = XSD_BOOLEAN_TYPE;
  // Note: XSD_DOUBLE_TYPE values ("NaN", "INF", "-INF") never use short form.
  if (shortForm && (term.type == i || term.type == d ||
                    (term.type == b && term.str.length() > 1))) {
    return term.str;
  }
  return absl::StrCat("\"", term.str, "\"^^<", term.type, ">");
}

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTerm& subject,
                         const EvaluatedTerm& predicate,
                         const EvaluatedTerm& object,
                         const ad_utility::MediaType& format) {
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv, ntriples};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  const bool shortForm = (format != ntriples);
  std::string s = formatTerm(*subject, shortForm);
  std::string p = formatTerm(*predicate, shortForm);
  std::string o = formatTerm(*object, shortForm);

  if (format == turtle || format == ntriples) {
    // Only escape literals (strings starting with "). IRIs and blank nodes
    // are used as-is, avoiding an unnecessary string copy.
    if (ql::starts_with(o, "\"")) {
      return absl::StrCat(s, " ", p, " ",
                          RdfEscaping::validRDFLiteralFromNormalized(o),
                          " .\n");
    }
    return absl::StrCat(s, " ", p, " ", o, " .\n");

  } else if (format == csv) {
    return absl::StrCat(RdfEscaping::escapeForCsv(std::move(s)), ",",
                        RdfEscaping::escapeForCsv(std::move(p)), ",",
                        RdfEscaping::escapeForCsv(std::move(o)), "\n");
  } else if (format == tsv) {
    return absl::StrCat(RdfEscaping::escapeForTsv(std::move(s)), "\t",
                        RdfEscaping::escapeForTsv(std::move(p)), "\t",
                        RdfEscaping::escapeForTsv(std::move(o)), "\n");
  } else {
    AD_FAIL();  // unreachable
  }
}
}  // namespace qlever::constructExport
