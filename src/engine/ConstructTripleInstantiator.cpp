// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstring>
#include <optional>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructDeduplicator.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/Views.h"

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
          return std::make_shared<const EvaluatedTermData>(EvaluatedTermData{
              absl::StrCat(t.prefix_, rowIdxTotal, t.suffix_), nullptr});
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "Unhandled variant type");
        }
      },
      term);
}

namespace {
// Instantiates one template triple for one result row, or returns
// `nullopt` if a term is undefined or the triple is a duplicate under
// `deduplication`.
std::optional<EvaluatedTriple> tryInstantiateTriple(
    const PreprocessedTriple& triple, const BatchEvaluationResult& batchResult,
    size_t rowInBatch, size_t blankNodeRowId, size_t tripleIdx,
    const PreprocessedConstructTemplate& tmpl,
    const std::optional<DeduplicationParams>& deduplication) {
  auto instantiate = [&triple, &batchResult, rowInBatch,
                      blankNodeRowId](size_t pos) {
    return instantiateTerm(triple.at(pos), batchResult, rowInBatch,
                           blankNodeRowId);
  };
  auto subject = instantiate(0);
  auto predicate = instantiate(1);
  auto object = instantiate(2);
  if (!subject || !predicate || !object) {
    return std::nullopt;
  }
  if (deduplication) {
    const size_t rowIdxInIdTable =
        deduplication.value().ctx_.get().firstRow_ + rowInBatch;
    if (!deduplication.value().deduplicator_.get().isNew(
            tripleIdx, rowIdxInIdTable, tmpl, deduplication.value().ctx_)) {
      return std::nullopt;
    }
  }
  return EvaluatedTriple{*subject, *predicate, *object};
}
}  // namespace

// _____________________________________________________________________________
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t batchOffset,
    std::optional<DeduplicationParams> deduplicationParams) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(batchResult.numRows_ * tmpl.preprocessedTriples_.size());

  for (const size_t rowInBatch :
       ad_utility::integerRange(batchResult.numRows_)) {
    const size_t blankNodeRowId = batchOffset + rowInBatch;
    for (auto&& [tripleIdx, triple] :
         ::ranges::views::enumerate(tmpl.preprocessedTriples_)) {
      if (auto instantiated = tryInstantiateTriple(
              triple, batchResult, rowInBatch, blankNodeRowId,
              static_cast<size_t>(tripleIdx), tmpl, deduplicationParams)) {
        triples.push_back(std::move(*instantiated));
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
  const auto* i = static_cast<const char*>(XSD_INT_TYPE);
  const auto* d = static_cast<const char*>(XSD_DECIMAL_TYPE);
  const auto* b = static_cast<const char*>(XSD_BOOLEAN_TYPE);

  // Note: XSD_DOUBLE_TYPE values (for example "NaN", "INF", "-INF") always
  // include the datatype.
  if (!includeDataType &&
      (term.rdfTermDataType_ == i || term.rdfTermDataType_ == d ||
       (term.rdfTermDataType_ == b && term.rdfTermString_.length() > 1))) {
    return term.rdfTermString_;
  }
  return absl::StrCat("\"", term.rdfTermString_, "\"^^<", term.rdfTermDataType_,
                      ">");
}

namespace {
// Upper bound on the number of bytes that `FastExportStreamFormatter` writes
// for `term` in Turtle: escaping at most doubles the characters of the term
// string, and a fully qualified literal adds its datatype and at most six
// delimiter characters (`"`, `"^^<`, `>`).
size_t turtleTermSizeUpperBound(const EvaluatedTermData& term) {
  size_t bound = 2 * term.rdfTermString_.size() + 6;
  if (term.rdfTermDataType_ != nullptr) {
    bound += std::strlen(term.rdfTermDataType_);
  }
  return bound;
}

// Upper bound on the number of bytes that `FastExportStreamFormatter` writes
// for `triple` in Turtle: the three terms, two separating spaces, and the
// trailing " .\n".
size_t turtleTripleSizeUpperBound(const EvaluatedTriple& triple) {
  const auto& [subject, predicate, object] = triple;
  AD_CONTRACT_CHECK(subject != nullptr && predicate != nullptr &&
                    object != nullptr);
  return turtleTermSizeUpperBound(*subject) +
         turtleTermSizeUpperBound(*predicate) +
         turtleTermSizeUpperBound(*object) + 5;
}

// Format the pending triple and as many following triples as fit into one
// batch. `pending` holds a triple on entry; on exit it holds the first triple
// that no longer fits, or nothing when `triples` is exhausted.
std::string formatPendingBatch(
    ad_utility::InputRangeTypeErased<EvaluatedTriple>& triples,
    size_t targetBatchBytes, std::optional<EvaluatedTriple>& pending) {
  // The first triple of a batch always fits, also if it is larger than
  // `targetBatchBytes`.
  std::string batch(
      std::max(targetBatchBytes, turtleTripleSizeUpperBound(pending.value())),
      '\0');
  ql::export_formatting::FastExportStreamFormatter formatter(
      ql::span<char>(batch.data(), batch.size()));
  do {
    formatter.writeTriple(ql::export_formatting::ExportFormat::Turtle,
                          pending.value());
    pending = triples.get();
  } while (pending.has_value() &&
           formatter.currentChunk().size() +
                   turtleTripleSizeUpperBound(pending.value()) <=
               batch.size());
  batch.resize(formatter.currentChunk().size());
  return batch;
}

}  // namespace

// _____________________________________________________________________________
std::string formatTripleAsTurtleWithFastFormatter(
    const EvaluatedTriple& evaluatedTriple) {
  using ql::export_formatting::ExportFormat;
  using ql::export_formatting::FastExportStreamFormatter;
  // Sized to the upper bound, so the fixed-span formatter never runs out of
  // space, and shrunk to the written size afterwards.
  std::string result(turtleTripleSizeUpperBound(evaluatedTriple), '\0');
  FastExportStreamFormatter formatter(
      ql::span<char>(result.data(), result.size()));
  formatter.writeTriple(ExportFormat::Turtle, evaluatedTriple);
  result.resize(formatter.currentChunk().size());
  return result;
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<std::string> formatTriplesAsTurtleInBatches(
    ad_utility::InputRangeTypeErased<EvaluatedTriple> triples,
    size_t targetBatchBytes) {
  AD_CONTRACT_CHECK(targetBatchBytes > 0);
  // `pending` is the next triple to be formatted. It is pulled from `triples`
  // before it is known whether it still fits into the current batch, so it has
  // to survive until the next call if it does not.
  auto nextBatch = [triples = std::move(triples), targetBatchBytes,
                    pending = std::optional<EvaluatedTriple>{}]() mutable
      -> std::optional<std::string> {
    if (!pending.has_value()) {
      pending = triples.get();
    }
    if (!pending.has_value()) {
      return std::nullopt;
    }
    return formatPendingBatch(triples, targetBatchBytes, pending);
  };
  return ad_utility::InputRangeTypeErased<std::string>{
      ad_utility::InputRangeFromGetCallable{std::move(nextBatch)}};
}

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format) {
  // TODO<ms2144>: take a look where we can eliminate string constructions here.
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv, ntriples};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  const auto& [subject, predicate, object] = evaluatedTriple;

  const bool includeDataType = (format == ntriples);

  std::string s = formatTerm(*subject, includeDataType);
  std::string p = formatTerm(*predicate, includeDataType);
  std::string o = formatTerm(*object, includeDataType);

  if (format == turtle || format == ntriples) {
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
