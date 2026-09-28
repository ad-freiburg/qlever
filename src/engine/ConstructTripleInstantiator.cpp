// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>

#include <cstring>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructDeduplicator.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
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

// Formats a single triple as Turtle using `FastExportStreamFormatter`
// (in-buffer escaping) instead of the per-term `std::string` construction in
// `formatTerm`. Produces output byte-identical to the legacy Turtle branch of
// `formatTriple` below; only used when `use-fast-export-stream-formatter` is
// enabled.
std::string formatTripleFastTurtle(const EvaluatedTriple& evaluatedTriple) {
  using ql::export_formatting::ExportFormat;
  using ql::export_formatting::FastExportStreamFormatter;
  const auto& [subject, predicate, object] = evaluatedTriple;
  AD_CONTRACT_CHECK(subject != nullptr && predicate != nullptr &&
                    object != nullptr);
  // Two separating spaces and the trailing " .\n".
  const size_t sizeBound = turtleTermSizeUpperBound(*subject) +
                           turtleTermSizeUpperBound(*predicate) +
                           turtleTermSizeUpperBound(*object) + 5;
  // Reused across calls to avoid a heap allocation per triple. It is sized to
  // the upper bound before formatting, so the fixed-span formatter can never
  // run out of space.
  static thread_local std::vector<char> buffer;
  if (buffer.size() < sizeBound) {
    buffer.resize(sizeBound);
  }
  FastExportStreamFormatter formatter(
      ql::span<char>(buffer.data(), buffer.size()));
  formatter.writeTriple(ExportFormat::Turtle, evaluatedTriple);
  return std::string{formatter.currentChunk()};
}
}  // namespace

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format) {
  // TODO<ms2144>: take a look where we can eliminate string constructions here.
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv, ntriples};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  if (format == turtle &&
      getRuntimeParameter<
          &RuntimeParameters::useFastExportStreamFormatter_>()) {
    return formatTripleFastTurtle(evaluatedTriple);
  }

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
std::string formatTripleRle(const EvaluatedTriple& evaluatedTriple,
                            const ad_utility::MediaType& format,
                            RleConstructTripleCache& cache) {
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv, ntriples};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  const auto& [subject, predicate, object] = evaluatedTriple;
  const bool includeDataType = (format == ntriples);

  // RLE prefix constant folding: reuse the previous row's formatted
  // subject/predicate string when the `EvaluatedTerm` is pointer-identical
  // to the last row's (guaranteed for repeated `Id`s within a batch by
  // `ConstructBatchEvaluator`'s `IdCache`), instead of reformatting it.
  // `shared_ptr` comparison is pointer comparison, and the owning handles
  // in the cache keep the previous row's terms alive across batches.
  std::string s;
  if (cache.lastSubject_ == subject) {
    s = cache.cachedSubject_;
  } else {
    s = formatTerm(*subject, includeDataType);
    cache.lastSubject_ = subject;
    cache.cachedSubject_ = s;
  }
  std::string p;
  if (cache.lastPredicate_ == predicate) {
    p = cache.cachedPredicate_;
  } else {
    p = formatTerm(*predicate, includeDataType);
    cache.lastPredicate_ = predicate;
    cache.cachedPredicate_ = p;
  }
  std::string o = formatTerm(*object, includeDataType);

  if (format == turtle || format == ntriples) {
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
