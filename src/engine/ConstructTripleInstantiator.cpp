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

namespace qlever::constructExport {

// _____________________________________________________________________________
std::optional<EvaluatedTerm> instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowIdxInBatch, size_t rowIdxTotal) {
  return std::visit(
      [&](const auto& t) -> std::optional<EvaluatedTerm> {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return t.evaluatedTerm_;
        } else if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          return batchResult.getVariable(t.columnIndex_, rowIdxInBatch);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return std::make_shared<const EvaluatedTermData>(
              absl::StrCat(t.prefix_, rowIdxTotal, t.suffix_), nullptr);
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "Unhandled variant type");
        }
      },
      term);
}

// _____________________________________________________________________________
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t batchOffset) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(batchResult.numRows_ * tmpl.preprocessedTriples_.size());

  for (size_t rowInBatch : ql::views::iota(size_t{0}, batchResult.numRows_)) {
    const size_t blankNodeRowId = batchOffset + rowInBatch;
    for (const auto& triple : tmpl.preprocessedTriples_) {
      auto instantiate = [&triple, &batchResult, rowInBatch,
                          blankNodeRowId](size_t pos) {
        return instantiateTerm(triple[pos], batchResult, rowInBatch,
                               blankNodeRowId);
      };
      auto subject = instantiate(0);
      auto predicate = instantiate(1);
      auto object = instantiate(2);
      if (subject && predicate && object) {
        triples.push_back(EvaluatedTriple{*subject, *predicate, *object});
      }
    }
  }
  return triples;
}

// _____________________________________________________________________________
std::string formatTerm(const EvaluatedTermData& term, bool includeDataType) {
  if (term.rdfTermDataType_ == nullptr) {
    // IRI, blank node, or vocab-indexed literal: already in final form.
    return term.rdfTermString_;
  }
  const char* i = XSD_INT_TYPE;
  const char* d = XSD_DECIMAL_TYPE;
  const char* b = XSD_BOOLEAN_TYPE;
  // Note: XSD_DOUBLE_TYPE values ("NaN", "INF", "-INF") always include the
  // datatype.
  if (!includeDataType &&
      (term.rdfTermDataType_ == i || term.rdfTermDataType_ == d ||
       (term.rdfTermDataType_ == b && term.rdfTermString_.length() > 1))) {
    return term.rdfTermString_;
  }
  return absl::StrCat("\"", term.rdfTermString_, "\"^^<", term.rdfTermDataType_,
                      ">");
}

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format,
                         bool includeDataType) {
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  const auto& [subject, predicate, object] = evaluatedTriple;

  std::string s = formatTerm(*subject, includeDataType);
  std::string p = formatTerm(*predicate, includeDataType);
  std::string o = formatTerm(*object, includeDataType);

  if (format == turtle) {
    // Only escape literals (strings starting with "). IRIs and blank nodes
    // are used as-is, avoiding an unnecessary string copy.
    if (ql::starts_with(o, '"')) {
      return absl::StrCat(
          s, " ", p, " ",
          RdfEscaping::validRDFLiteralFromNormalized(std::move(o)), " .\n");
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

// _____________________________________________________________________________
StringTriple createStringTriple(const EvaluatedTriple& evaluatedTriple,
                                bool includeDataType) {
  const auto& [subject, predicate, object] = evaluatedTriple;

  std::string s = formatTerm(*subject, includeDataType);
  std::string p = formatTerm(*predicate, includeDataType);
  std::string o = formatTerm(*object, includeDataType);

  return StringTriple{std::move(s), std::move(p), std::move(o)};
}
}  // namespace qlever::constructExport
